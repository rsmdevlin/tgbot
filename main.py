import telebot
from telebot import types
import sqlite3  # только для чтения .db/.sqlite файлов пользователя
import datetime
import threading
import asyncio
import os
from collections import OrderedDict
import re as _re

try:
    from telethon import TelegramClient, events
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, User, Channel, Chat
    from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError
    TELETHON_OK = True
except ImportError:
    TELETHON_OK = False

# ─────────────────────────────────────────────
#  КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────
BOT_TOKEN       = "6530521973:AAE4KHV9Sb-3Uv2e8NIIzKxuBzs6h7ZGjoo"
ADMIN_IDS       = [6091955295, 5184773786]
ACCOUNT_USER_ID = 6091955295

# ── Динамические роли (модераторы/доп.админы из БД) ─────────────
_staff_cache: dict = {}   # user_id -> role ('admin'|'moderator')
_staff_lock  = threading.Lock()

def _load_staff_cache():
    global _staff_cache
    try:
        with get_conn() as conn:
            rows = conn.execute("SELECT user_id, role FROM staff_roles").fetchall()
        with _staff_lock:
            _staff_cache = {r["user_id"]: r["role"] for r in rows}
    except Exception:
        pass

def get_role(user_id: int) -> str:
    """Возвращает роль: 'superadmin' | 'admin' | 'moderator' | 'user'"""
    if user_id in ADMIN_IDS:
        return "superadmin"
    with _staff_lock:
        return _staff_cache.get(user_id, "user")

def is_admin_or_higher(user_id: int) -> bool:
    return get_role(user_id) in ("superadmin", "admin")

def is_staff(user_id: int) -> bool:
    return get_role(user_id) in ("superadmin", "admin", "moderator")

TELE_API_ID   = 34264041
TELE_API_HASH = "11906a924b9115ae93dc6fa3ebca1137"
SESSIONS_DIR  = "sessions"

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")


def safe_edit(text, chat_id, message_id, reply_markup=None, **kwargs):
    import time as _time
    for _attempt in range(3):
        try:
            bot.edit_message_text(text, chat_id, message_id,
                                  reply_markup=reply_markup,
                                  parse_mode="HTML", **kwargs)
            return
        except Exception as e:
            err = str(e)
            if "message is not modified" in err or "query is too old" in err:
                return
            elif "Too Many Requests" in err or "429" in err:
                # Получаем retry_after из сообщения или ждём по умолчанию
                try:
                    secs = int(_re.search(r"retry after (\d+)", err).group(1))
                except Exception:
                    secs = 5
                _time.sleep(min(secs, 30))
                continue
            elif "can't parse entities" in err or "Bad Request" in err:
                try:
                    clean = _re.sub(r'<[^>]+>', '', text)
                    bot.edit_message_text(clean, chat_id, message_id, reply_markup=reply_markup)
                except Exception:
                    pass
                return
            elif "message to edit not found" in err or "MESSAGE_ID_INVALID" in err:
                return
            else:
                return  # Не падаем из-за edit-ошибок — просто логируем молча


def _row(r, key, default=None):
    """Safely get value from sqlite3.Row or dict."""
    try:
        v = r[key]
        return v if v is not None else default
    except (IndexError, KeyError):
        return default


def escape_html(text: str) -> str:
    """Экранирует HTML-спецсимволы и угловые скобки чтобы избежать parse errors."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# ─────────────────────────────────────────────
#  ФОРМАТИРОВАНИЕ
# ─────────────────────────────────────────────
_MONTHS_RU = {
    1:"янв",2:"фев",3:"мар",4:"апр",5:"май",6:"июн",
    7:"июл",8:"авг",9:"сен",10:"окт",11:"ноя",12:"дек"
}

def fmt_date(ts: str) -> str:
    if not ts:
        return "—"
    ts = ts.strip()
    try:
        ts_clean = ts[:16].replace("T", " ")
        dt = datetime.datetime.strptime(ts_clean, "%Y-%m-%d %H:%M")
        month = _MONTHS_RU.get(dt.month, f"{dt.month:02d}")
        return f"{dt.day} {month}, {dt.hour:02d}:{dt.minute:02d}"
    except Exception:
        return ts[:16]

def fmt_msg_link(url: str, is_public: bool, ts: str = "") -> str:
    date_str = fmt_date(ts)
    if is_public:
        return f'<a href="{url}">🔗 перейти</a>  · <i>{date_str}</i>'
    return f'🔒 <i>приватная</i>  · <i>{date_str}</i>'


# ─────────────────────────────────────────────
#  БАЗА ДАННЫХ  (PostgreSQL через psycopg2)
# ─────────────────────────────────────────────
# URL подключения берётся из переменной окружения DATABASE_URL
# Пример: postgres://user:pass@host:5432/dbname
import psycopg2
import psycopg2.extras
import psycopg2.pool

db_lock = threading.Lock()

DATABASE_URL = os.environ.get("DATABASE_URL", "")

_pg_pool = None
_pg_pool_lock = threading.Lock()

def _get_pg_pool():
    global _pg_pool
    with _pg_pool_lock:
        if _pg_pool is None:
            _pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1, maxconn=20, dsn=DATABASE_URL
            )
    return _pg_pool


class _PGRow(dict):
    """Строка результата — поддерживает доступ и по имени r["col"] и по индексу r[0]."""
    def __init__(self, data: dict):
        super().__init__(data)
        self._keys = list(data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(self._keys[key])
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError):
            return default

    def keys(self):
        return self._keys


class _PGCursor:
    """Эмулирует sqlite3 cursor с доступом к строкам по имени колонки."""
    def __init__(self, pg_cursor):
        self._cur = pg_cursor
        self.lastrowid = None

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return _PGRow(dict(row))

    def fetchall(self):
        rows = self._cur.fetchall()
        return [_PGRow(dict(r)) for r in rows]

    def __iter__(self):
        return iter(self.fetchall())


class _PGConn:
    """Эмулирует sqlite3 Connection интерфейс для psycopg2."""

    # Конвертирует ? плейсхолдеры в %s для psycopg2
    @staticmethod
    def _fix_sql(sql: str) -> str:
        # Заменяем ? на %s но не трогаем %s которые уже есть
        result = []
        i = 0
        while i < len(sql):
            ch = sql[i]
            if ch == '?' :
                result.append('%s')
            elif ch == '%' and i + 1 < len(sql) and sql[i+1] == 's':
                result.append('%s')
                i += 2
                continue
            else:
                result.append(ch)
            i += 1
        return ''.join(result)

    @staticmethod
    def _fix_sql_pg(sql: str) -> str:
        """Полная конвертация SQLite SQL → PostgreSQL SQL."""
        import re as _r
        s = sql.strip()
        # INTEGER PRIMARY KEY AUTOINCREMENT → BIGSERIAL PRIMARY KEY
        s = _r.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', 'BIGSERIAL PRIMARY KEY', s, flags=_r.IGNORECASE)
        # INTEGER PRIMARY KEY (без AUTOINCREMENT) → BIGINT PRIMARY KEY
        s = _r.sub(r'\bINTEGER\s+PRIMARY\s+KEY\b', 'BIGINT PRIMARY KEY', s, flags=_r.IGNORECASE)
        # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
        s = _r.sub(r'\bINSERT\s+OR\s+IGNORE\b', 'INSERT', s, flags=_r.IGNORECASE)
        if 'INSERT' in s.upper() and 'ON CONFLICT' not in s.upper() and 'OR IGNORE' not in sql.upper():
            pass  # обычный INSERT
        elif 'INSERT' in s.upper() and 'ON CONFLICT' not in s.upper():
            # была OR IGNORE — добавляем ON CONFLICT DO NOTHING
            s = s.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
        # INSERT OR REPLACE → INSERT ... ON CONFLICT DO UPDATE (упрощённо)
        s = _r.sub(r'\bINSERT\s+OR\s+REPLACE\b', 'INSERT', s, flags=_r.IGNORECASE)
        # lower(col) → LOWER(col) — OK в PG
        # DESC в индексах — OK в PG
        # PRAGMA — убираем
        if _r.match(r'\s*PRAGMA\b', s, _r.IGNORECASE):
            return None  # сигнал пропустить
        # sqlite_master → information_schema (обрабатывается отдельно)
        s = s.replace('sqlite_master', 'pg_catalog.pg_indexes')
        # ? → %s
        s = _PGConn._fix_sql(s)
        return s

    def __init__(self, pg_conn):
        self._conn = pg_conn
        self._conn.autocommit = False
        self._last_rowcount = 0
        self._last_insert_id = None

    def execute(self, sql, params=None):
        import re as _r2
        fixed = self._fix_sql_pg(sql)
        if fixed is None:
            # PRAGMA — возвращаем фейковый курсор
            class _Fake:
                lastrowid = None
                description = None
                rowcount = 0
                def fetchone(self): return _PGRow({'changes()': 0, 'count': 0})
                def fetchall(self): return []
                def __iter__(self): return iter([])
            return _Fake()

        # SELECT changes() — SQLite специфика, эмулируем
        if _r2.match(r'\s*SELECT\s+changes\(\)', fixed, _r2.IGNORECASE):
            class _ChangeFake:
                def __init__(self, n): self._n = n
                lastrowid = None
                description = None
                rowcount = 0
                def fetchone(self_): return _PGRow({'changes()': self_._n})
                def fetchall(self_): return [_PGRow({'changes()': self_._n})]
                def __iter__(self_): return iter(self_.fetchall())
            return _ChangeFake(self._last_rowcount)

        # SELECT last_insert_rowid() — SQLite специфика
        if _r2.match(r'\s*SELECT\s+last_insert_rowid\(\)', fixed, _r2.IGNORECASE):
            class _RowidFake:
                def __init__(self, n): self._n = n
                lastrowid = None
                description = None
                rowcount = 0
                def fetchone(self_): return _PGRow({'last_insert_rowid()': self_._n})
                def fetchall(self_): return [_PGRow({'last_insert_rowid()': self_._n})]
                def __iter__(self_): return iter(self_.fetchall())
            return _RowidFake(self._last_insert_id or 0)

        # BEGIN / COMMIT / ROLLBACK как отдельные execute() вызовы
        if _r2.match(r'\s*(BEGIN|COMMIT|ROLLBACK)\s*$', fixed, _r2.IGNORECASE):
            upper = fixed.upper().strip()
            if 'ROLLBACK' in upper:
                self._conn.rollback()
            elif 'COMMIT' in upper:
                self._conn.commit()
            # BEGIN — psycopg2 с autocommit=False начинает транзакцию автоматически, просто игнорируем
            class _TxFake:
                lastrowid = None; description = None; rowcount = 0
                def fetchone(self): return None
                def fetchall(self): return []
                def __iter__(self): return iter([])
            return _TxFake()

        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            # Добавляем RETURNING для INSERT чтобы получить lastrowid
            needs_returning = False
            if _r2.match(r'\s*INSERT\b', fixed, _r2.IGNORECASE) and 'RETURNING' not in fixed.upper():
                fixed_ret = fixed.rstrip().rstrip(';') + ' RETURNING id'
                needs_returning = True
            else:
                fixed_ret = fixed

            if params:
                cur.execute(fixed_ret, params)
            else:
                cur.execute(fixed_ret)

            self._last_rowcount = cur.rowcount
            wrapper = _PGCursor(cur)
            if needs_returning:
                try:
                    row = cur.fetchone()
                    if row and 'id' in row:
                        wrapper.lastrowid = row['id']
                        self._last_insert_id = row['id']
                except Exception:
                    pass
            return wrapper
        except Exception as e:
            # Если RETURNING не сработал (таблица без id) — пробуем без него
            if needs_returning:
                try:
                    self._conn.rollback()
                    cur2 = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    if params:
                        cur2.execute(fixed, params)
                    else:
                        cur2.execute(fixed)
                    self._last_rowcount = cur2.rowcount
                    return _PGCursor(cur2)
                except Exception as e2:
                    raise e2
            raise

    def executemany(self, sql, params_list):
        fixed = self._fix_sql_pg(sql)
        if fixed is None:
            return
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        psycopg2.extras.execute_batch(cur, fixed, params_list)
        return _PGCursor(cur)

    def executescript(self, sql):
        """Выполняет набор SQL команд (аналог SQLite executescript)."""
        import re as _r3
        # Конвертируем весь скрипт
        # Убираем PRAGMA optimize и другие PRAGMA
        sql = _r3.sub(r'\bPRAGMA\s+[^;]+;?', '', sql, flags=_r3.IGNORECASE)
        # INTEGER PRIMARY KEY AUTOINCREMENT → BIGSERIAL PRIMARY KEY
        sql = _r3.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', 'BIGSERIAL PRIMARY KEY', sql, flags=_r3.IGNORECASE)
        # INTEGER PRIMARY KEY (без AUTOINCREMENT) → BIGINT PRIMARY KEY
        sql = _r3.sub(r'\bINTEGER\s+PRIMARY\s+KEY\b', 'BIGINT PRIMARY KEY', sql, flags=_r3.IGNORECASE)
        # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
        def _fix_insert_ignore(m):
            stmt = m.group(0)
            stmt = _r3.sub(r'\bINSERT\s+OR\s+IGNORE\b', 'INSERT', stmt, flags=_r3.IGNORECASE)
            stmt = stmt.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING;'
            return stmt
        sql = _r3.sub(r'INSERT\s+OR\s+IGNORE\b[^;]+;', _fix_insert_ignore, sql, flags=_r3.IGNORECASE|_r3.DOTALL)
        # INSERT OR REPLACE → INSERT ... ON CONFLICT DO UPDATE SET ...
        sql = _r3.sub(r'\bINSERT\s+OR\s+REPLACE\b', 'INSERT', sql, flags=_r3.IGNORECASE)
        # sqlite_master → pg_catalog.pg_class
        sql = sql.replace('sqlite_master', 'pg_catalog.pg_class')
        # lower() → LOWER() — OK
        # Разбиваем на отдельные команды и выполняем
        cur = self._conn.cursor()
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        for stmt in statements:
            try:
                cur.execute(stmt)
            except Exception as e:
                err = str(e).lower()
                # Игнорируем "already exists" ошибки
                if 'already exists' in err or 'duplicate' in err:
                    self._conn.rollback()
                    continue
                else:
                    self._conn.rollback()
                    # не прерываем — продолжаем

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            _get_pg_pool().putconn(self._conn)
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            try:
                self._conn.commit()
            except Exception:
                self._conn.rollback()
        else:
            try:
                self._conn.rollback()
            except Exception:
                pass
        return False  # не подавляем исключения


_db_pool_cache: dict = {}  # per-thread connection pool
_db_pool_cache_lock = threading.Lock()

def get_conn() -> _PGConn:
    import threading as _t
    tid = _t.current_thread().ident
    with _db_pool_cache_lock:
        conn_wrapper = _db_pool_cache.get(tid)
    if conn_wrapper is not None:
        # Проверяем что соединение живо
        try:
            conn_wrapper._conn.cursor().execute("SELECT 1")
            return conn_wrapper
        except Exception:
            with _db_pool_cache_lock:
                _db_pool_cache.pop(tid, None)

    pg_conn = _get_pg_pool().getconn()
    pg_conn.autocommit = False
    wrapper = _PGConn(pg_conn)
    with _db_pool_cache_lock:
        _db_pool_cache[tid] = wrapper
    return wrapper

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id            BIGINT PRIMARY KEY,
            username      TEXT,
            first_name    TEXT,
            last_name     TEXT,
            phone         TEXT,
            city          TEXT,
            country       TEXT,
            language_code TEXT,
            is_premium    INTEGER DEFAULT 0,
            first_seen    TEXT,
            last_seen     TEXT,
            notes         TEXT,
            is_banned     INTEGER DEFAULT 0,
            ban_reason    TEXT
        );
        CREATE TABLE IF NOT EXISTS chats (
            id            BIGINT PRIMARY KEY,
            title         TEXT,
            chat_type     TEXT,
            username      TEXT,
            invite_link   TEXT,
            description   TEXT,
            member_count  INTEGER,
            joined_at     TEXT,
            last_activity TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id              BIGSERIAL PRIMARY KEY,
            user_id         BIGINT,
            chat_id         BIGINT,
            chat_title      TEXT,
            chat_type       TEXT,
            text            TEXT,
            content_type    TEXT DEFAULT 'text',
            message_id      INTEGER,
            reply_to_msg_id INTEGER,
            fwd_from_id     BIGINT,
            timestamp       TEXT
        );
        CREATE TABLE IF NOT EXISTS banned_users (
            user_id   BIGINT PRIMARY KEY,
            reason    TEXT,
            banned_at TEXT,
            banned_by BIGINT
        );
        CREATE TABLE IF NOT EXISTS events (
            id          BIGSERIAL PRIMARY KEY,
            event_type  TEXT,
            user_id     BIGINT,
            chat_id     BIGINT,
            description TEXT,
            timestamp   TEXT
        );
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        INSERT OR IGNORE INTO settings VALUES ('anonymous_mode',   '0');
        INSERT OR IGNORE INTO settings VALUES ('logging_enabled',  '1');
        INSERT OR IGNORE INTO settings VALUES ('forward_to_admin', '1');
        INSERT OR IGNORE INTO settings VALUES ('ban_block',        '1');
        INSERT OR IGNORE INTO settings VALUES ('event_log',        '1');

        CREATE TABLE IF NOT EXISTS staff_roles (
            user_id    BIGINT PRIMARY KEY,
            role       TEXT NOT NULL DEFAULT 'moderator',
            username   TEXT,
            added_by   BIGINT,
            added_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS basa_messages (
            id            BIGSERIAL PRIMARY KEY,
            session_id    TEXT,
            chat_id       BIGINT,
            chat_title    TEXT,
            chat_username TEXT,
            chat_type     TEXT,
            user_id       BIGINT,
            username      TEXT,
            first_name    TEXT,
            last_name     TEXT,
            phone         TEXT,
            message_id    INTEGER,
            text          TEXT,
            content_type  TEXT,
            timestamp     TEXT,
            saved_at      TEXT,
            UNIQUE(chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS basa_sessions (
            id             BIGSERIAL PRIMARY KEY,
            session_id     TEXT UNIQUE,
            chat_id        BIGINT,
            chat_title     TEXT,
            chat_username  TEXT,
            total_messages INTEGER DEFAULT 0,
            started_at     TEXT,
            created_at     TEXT,
            admin_id       BIGINT,
            is_live        INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS chat_members (
            id            BIGSERIAL PRIMARY KEY,
            chat_id       BIGINT NOT NULL,
            user_id       BIGINT NOT NULL,
            username      TEXT,
            first_name    TEXT,
            last_name     TEXT,
            phone         TEXT,
            is_bot        INTEGER DEFAULT 0,
            is_admin      INTEGER DEFAULT 0,
            is_banned     INTEGER DEFAULT 0,
            joined_date   TEXT,
            scraped_at    TEXT,
            UNIQUE(chat_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cm_chat ON chat_members(chat_id);
        CREATE INDEX IF NOT EXISTS idx_cm_user ON chat_members(user_id);
        CREATE TABLE IF NOT EXISTS scrape_tasks (
            id          BIGSERIAL PRIMARY KEY,
            chat_id     BIGINT UNIQUE,
            chat_title  TEXT,
            status      TEXT DEFAULT 'pending',
            total       INTEGER DEFAULT 0,
            done        INTEGER DEFAULT 0,
            started_at  TEXT,
            finished_at TEXT,
            error_msg   TEXT
        );
        CREATE TABLE IF NOT EXISTS partner_bots (
            id            BIGSERIAL PRIMARY KEY,
            bot_token     TEXT UNIQUE NOT NULL,
            bot_username  TEXT,
            bot_name      TEXT,
            owner_id      BIGINT,
            owner_username TEXT,
            owner_name    TEXT,
            stars         INTEGER DEFAULT 0,
            plan          TEXT DEFAULT 'free',
            is_active     INTEGER DEFAULT 1,
            notifications INTEGER DEFAULT 1,
            connected_at  TEXT,
            last_ping     TEXT,
            note          TEXT
        );
        CREATE TABLE IF NOT EXISTS partner_chats (
            id          BIGSERIAL PRIMARY KEY,
            bot_id      BIGINT NOT NULL,
            chat_id     BIGINT NOT NULL,
            chat_title  TEXT,
            chat_username TEXT,
            chat_type   TEXT,
            member_count INTEGER DEFAULT 0,
            joined_at   TEXT,
            UNIQUE(bot_id, chat_id)
        );
        CREATE TABLE IF NOT EXISTS stars_log (
            id          BIGSERIAL PRIMARY KEY,
            user_id     BIGINT,
            bot_id      BIGINT,
            amount      INTEGER,
            reason      TEXT,
            ts          TEXT
        );
        CREATE TABLE IF NOT EXISTS chat_notif_settings (
            chat_id     BIGINT PRIMARY KEY,
            muted       INTEGER DEFAULT 0,
            muted_until TEXT,
            note        TEXT
        );
        CREATE TABLE IF NOT EXISTS user_stars (
            user_id     BIGINT PRIMARY KEY,
            stars       INTEGER DEFAULT 0,
            last_daily  TEXT
        );
        CREATE TABLE IF NOT EXISTS user_probiv_log (
            id          BIGSERIAL PRIMARY KEY,
            user_id     BIGINT,
            ts          TEXT
        );
        CREATE TABLE IF NOT EXISTS user_referrals (
            id              BIGSERIAL PRIMARY KEY,
            referrer_id     BIGINT NOT NULL,
            referred_id     BIGINT NOT NULL UNIQUE,
            stars_awarded   INTEGER DEFAULT 0,
            created_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS support_tickets (
            id              BIGSERIAL PRIMARY KEY,
            user_id         BIGINT NOT NULL,
            message_id      INTEGER,
            admin_reply_msg INTEGER,
            text            TEXT,
            content_type    TEXT DEFAULT 'text',
            file_id         TEXT,
            status          TEXT DEFAULT 'open',
            created_at      TEXT,
            updated_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS support_auto_replies (
            id          BIGSERIAL PRIMARY KEY,
            keyword     TEXT NOT NULL,
            reply       TEXT NOT NULL,
            is_active   INTEGER DEFAULT 1,
            created_at  TEXT
        );
        INSERT OR IGNORE INTO settings VALUES ('support_enabled',   '1');
        INSERT OR IGNORE INTO settings VALUES ('referral_stars',    '10');
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id          BIGSERIAL PRIMARY KEY,
            admin_id    BIGINT,
            preview     TEXT,
            sent        INTEGER DEFAULT 0,
            failed      INTEGER DEFAULT 0,
            started_at  TEXT,
            finished_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bc_at ON broadcast_log(started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ur_ref  ON user_referrals(referrer_id);
        CREATE INDEX IF NOT EXISTS idx_ur_red  ON user_referrals(referred_id);
        CREATE INDEX IF NOT EXISTS idx_st_uid  ON support_tickets(user_id);
        CREATE INDEX IF NOT EXISTS idx_st_stat ON support_tickets(status);
        CREATE INDEX IF NOT EXISTS idx_u_fn    ON users(first_name);
        CREATE INDEX IF NOT EXISTS idx_u_ln    ON users(last_name);
        CREATE INDEX IF NOT EXISTS idx_cm_fn   ON chat_members(first_name);
        CREATE INDEX IF NOT EXISTS idx_bm_fn   ON basa_messages(first_name);
        CREATE INDEX IF NOT EXISTS idx_bm_un   ON basa_messages(lower(username));
        CREATE INDEX IF NOT EXISTS idx_u_un    ON users(lower(username));
        CREATE INDEX IF NOT EXISTS idx_u_ph    ON users(phone);
        CREATE INDEX IF NOT EXISTS idx_u_ban   ON users(is_banned);
        CREATE INDEX IF NOT EXISTS idx_m_uid   ON messages(user_id);
        CREATE INDEX IF NOT EXISTS idx_m_cid   ON messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_m_ts    ON messages(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_bm_uid  ON basa_messages(user_id);
        CREATE INDEX IF NOT EXISTS idx_bm_cid  ON basa_messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_bm_sid  ON basa_messages(session_id);
        CREATE INDEX IF NOT EXISTS idx_bm_ts   ON basa_messages(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_cm_uid  ON chat_members(user_id);
        CREATE INDEX IF NOT EXISTS idx_cm_cid  ON chat_members(chat_id);
        CREATE INDEX IF NOT EXISTS idx_cm_un   ON chat_members(lower(username));
        CREATE INDEX IF NOT EXISTS idx_cm_ph   ON chat_members(phone);
        CREATE INDEX IF NOT EXISTS idx_bs_cid  ON basa_sessions(chat_id);
        CREATE INDEX IF NOT EXISTS idx_sl_bid  ON stars_log(bot_id, ts);
        CREATE INDEX IF NOT EXISTS idx_ch_act  ON chats(last_activity DESC);
        CREATE INDEX IF NOT EXISTS idx_us_ls   ON users(last_seen DESC);
        CREATE INDEX IF NOT EXISTS idx_us_fs   ON users(first_seen DESC);
        CREATE INDEX IF NOT EXISTS idx_m_ts2   ON messages(timestamp DESC, chat_id);
        CREATE INDEX IF NOT EXISTS idx_bm_ts2  ON basa_messages(timestamp DESC, chat_id);
        CREATE INDEX IF NOT EXISTS idx_upl_uid ON user_probiv_log(user_id, ts);
        CREATE INDEX IF NOT EXISTS idx_sl_ts   ON stars_log(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_sl_uid  ON stars_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_pbots   ON partner_bots(is_active, owner_id);
        PRAGMA optimize;

        CREATE TABLE IF NOT EXISTS uploaded_databases (
            id          BIGSERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            orig_name   TEXT NOT NULL,
            file_format TEXT NOT NULL,
            uploaded_by BIGINT,
            uploaded_at TEXT,
            total_rows  INTEGER DEFAULT 0,
            note        TEXT
        );
        CREATE TABLE IF NOT EXISTS udb_entries (
            id          BIGSERIAL PRIMARY KEY,
            db_id       BIGINT NOT NULL,
            phone       TEXT,
            username    TEXT,
            email       TEXT,
            tg_id       TEXT,
            name        TEXT,
            message     TEXT,
            raw_data    TEXT,
            extra       TEXT,
            row_index   INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_udb_dbid    ON udb_entries(db_id);
        CREATE INDEX IF NOT EXISTS idx_udb_phone   ON udb_entries(phone);
        CREATE INDEX IF NOT EXISTS idx_udb_un      ON udb_entries(username);
        CREATE INDEX IF NOT EXISTS idx_udb_email   ON udb_entries(email);
        CREATE INDEX IF NOT EXISTS idx_udb_tgid    ON udb_entries(tg_id);
        CREATE INDEX IF NOT EXISTS idx_udb_name    ON udb_entries(name);
        CREATE INDEX IF NOT EXISTS idx_udb_msg     ON udb_entries(message);
        CREATE INDEX IF NOT EXISTS idx_udb_raw     ON udb_entries(raw_data);
        CREATE INDEX IF NOT EXISTS idx_users_un    ON users(username);
        CREATE INDEX IF NOT EXISTS idx_users_fn    ON users(first_name);
        CREATE INDEX IF NOT EXISTS idx_users_ln    ON users(last_name);
        CREATE INDEX IF NOT EXISTS idx_users_ph    ON users(phone);
        CREATE INDEX IF NOT EXISTS idx_msg_uid2    ON messages(user_id);
        CREATE INDEX IF NOT EXISTS idx_msg_ts2x    ON messages(timestamp);
        """)
    _safe_migrate()

def _safe_migrate():
    migrations = [
        ("users",    "language_code",   "TEXT"),
        ("users",    "is_premium",      "INTEGER DEFAULT 0"),
        ("users",    "notes",           "TEXT"),
        ("users",    "is_banned",       "INTEGER DEFAULT 0"),
        ("users",    "ban_reason",      "TEXT"),
        ("chats",    "description",     "TEXT"),
        ("chats",    "member_count",    "INTEGER"),
        ("chats",    "last_activity",   "TEXT"),
        ("messages",      "content_type",    "TEXT DEFAULT 'text'"),
        ("messages",      "reply_to_msg_id", "INTEGER"),
        ("messages",      "fwd_from_id",     "INTEGER"),
        ("basa_sessions", "is_live",         "INTEGER DEFAULT 0"),
        ("basa_sessions", "started_at",      "TEXT"),
        ("basa_sessions", "last_message_id", "INTEGER DEFAULT 0"),
        ("basa_sessions", "copy_status",     "TEXT DEFAULT 'idle'"),
        ("users",         "is_bot",          "INTEGER DEFAULT 0"),
        ("basa_messages", "phone",           "TEXT"),
    ]
    with get_conn() as conn:
        for table, column, col_type in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                conn.commit()
            except Exception:
                pass

    # Миграция: убираем дубликаты basa_messages и добавляем UNIQUE(chat_id, message_id)
    with get_conn() as conn:
        try:
            # Проверяем есть ли уже индекс (PostgreSQL способ)
            idx = conn.execute(
                "SELECT indexname FROM pg_indexes WHERE indexname='idx_basa_msg_unique'"
            ).fetchone()
            if not idx:
                # Удаляем дубли — оставляем запись с наименьшим id
                conn.execute("""
                    DELETE FROM basa_messages WHERE id NOT IN (
                        SELECT MIN(id) FROM basa_messages
                        WHERE chat_id IS NOT NULL AND message_id IS NOT NULL
                        GROUP BY chat_id, message_id
                    ) AND chat_id IS NOT NULL AND message_id IS NOT NULL
                """)
                # Создаём уникальный индекс
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_basa_msg_unique
                    ON basa_messages(chat_id, message_id)
                """)
                conn.commit()
                print("✅ Миграция basa_messages: дубли удалены, UNIQUE индекс создан")
        except Exception as e:
            print(f"⚠️  Миграция basa_messages: {e}")

    # Создаём индексы которые зависят от колонок добавленных через миграцию
    with get_conn() as conn:
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_bs_live ON basa_sessions(is_live)",
        ]:
            try:
                conn.execute(idx_sql)
            except Exception:
                pass


# ═════════════════════════════════════════════
#  СИСТЕМА ЗАГРУЖАЕМЫХ БАЗ ДАННЫХ
# ═════════════════════════════════════════════

import re as _re_udb
import csv as _csv
import json as _json
import io as _io
import os as _os
import tempfile as _tempfile

_PHONE_RE_UDB = _re_udb.compile(
    r'(?<!\d)(\+?[78][\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}'
    r'|\+?[1-9]\d{6,14})(?!\d)'
)
_EMAIL_RE_UDB = _re_udb.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}')
_UN_RE        = _re_udb.compile(r'(?:^|[\s,;|:])@?([A-Za-z][A-Za-z0-9_]{3,31})(?=[\s,;|:]|$)')

# Словари для автоопределения типа колонки по имени
_COLS_PHONE    = {"phone","телефон","номер","mobile","tel","msisdn","phone_number","phonenumber","моб","kontakt","telefon","cell","cellular"}
_COLS_EMAIL    = {"email","e-mail","mail","почта","email_address","электронная_почта","e_mail"}
_COLS_USERNAME = {"username","user_name","login","логин","nick","nickname","ник","tg_username","telegram_username","юзернейм","handle","tg","telegram","screen_name"}
_COLS_TGID     = {"user_id","tg_id","telegram_id","userid","tgid","chat_id","from_id","sender_id","uid","account_id"}
_COLS_NAME     = {"name","full_name","fullname","first_name","last_name","firstname","lastname","имя","фамилия","фио","имя_фамилия","contact_name","display_name","real_name","fio"}
_COLS_MSG      = {"message","text","msg","content","body","comment","note","сообщение","текст","комментарий","bio","about","description","status"}
_COLS_CITY     = {"city","город","town","region","регион","oblast","область","district","country","страна","location","адрес","address"}
_COLS_DATE     = {"date","дата","created_at","registered","timestamp","registration_date","birth_date","birthdate","дата_рождения","дата_регистрации"}


def _classify_col(col: str) -> str:
    c = col.lower().strip().replace(" ","_").replace("-","_")
    if c in _COLS_PHONE    or any(x in c for x in ("phone","телефон","mobile","msisdn")): return "phone"
    if c in _COLS_EMAIL    or "email" in c or ("mail" in c and "gmail" not in c):         return "email"
    if c in _COLS_USERNAME or "username" in c or "login" in c or "логин" in c:            return "username"
    if c in _COLS_TGID     or (c.endswith("_id") and "user" in c):                       return "tg_id"
    if c == "id":                                                                           return "tg_id"
    if c in _COLS_NAME     or "name" in c or "имя" in c or "фио" in c:                   return "name"
    if c in _COLS_MSG      or "message" in c or "text" in c or "текст" in c:             return "message"
    if c in _COLS_CITY:                                                                    return "city"
    if c in _COLS_DATE:                                                                    return "date"
    return "extra"


def _clean_phone(v: str) -> str | None:
    if not v: return None
    d = _re_udb.sub(r'[^\d+]', '', str(v).strip())
    if len(_re_udb.sub(r'\D','',d)) < 7: return None
    return d[:20]


def _clean_username(v: str) -> str | None:
    if not v: return None
    v = v.strip().lstrip("@")
    if _re_udb.match(r'^[A-Za-z][A-Za-z0-9_]{2,31}$', v):
        return v[:50]
    return None


def _extract_fields(text: str) -> dict:
    s = text or ""
    phones = [_clean_phone(p) for p in _PHONE_RE_UDB.findall(s)]
    phones = [p for p in phones if p]
    emails = _EMAIL_RE_UDB.findall(s)
    usernames = _UN_RE.findall(s)
    tg_ids = []
    for tok in _re_udb.findall(r'\b\d{5,15}\b', s):
        if 10000 <= int(tok) <= 9_999_999_999:
            tg_ids.append(tok); break
    return {
        "phone":    phones[0]    if phones    else None,
        "email":    emails[0]    if emails    else None,
        "username": usernames[0] if usernames else None,
        "tg_id":    tg_ids[0]   if tg_ids    else None,
    }


def _guess_encoding(fp: str) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            open(fp, "r", encoding=enc, errors="strict").read(8192); return enc
        except Exception: pass
    return "utf-8"


def _smart_row(rd: dict, col_map: dict, raw: str) -> dict:
    """Умно собирает запись из строки, используя карту колонок."""
    phone = email = username = tg_id = name = message = None
    extra_parts = []

    for col, val in rd.items():
        val = (val or "").strip()
        if not val or val.lower() in ("none","null","nan","-",""):
            continue
        ct = col_map.get(col, _classify_col(col))

        if ct == "phone" and not phone:
            phone = _clean_phone(val) or (_extract_fields(val)["phone"])
        elif ct == "email" and not email:
            email = (val[:200] if "@" in val else None) or _extract_fields(val)["email"]
        elif ct == "username" and not username:
            username = _clean_username(val)
        elif ct == "tg_id" and not tg_id:
            d = _re_udb.sub(r'\D','',val)
            if 5 <= len(d) <= 15:
                try:
                    if 10000 <= int(d) <= 9_999_999_999: tg_id = d
                except: pass
        elif ct == "name" and not name:
            if len(val) >= 2: name = val[:120]
        elif ct == "message" and not message:
            message = val[:500]
        elif ct == "city":
            extra_parts.append(f"🏙{val[:40]}")
        elif ct == "date":
            extra_parts.append(f"📅{val[:20]}")
        elif ct == "extra":
            if len(val) > 1: extra_parts.append(f"{col}={val[:60]}")

    # Фоллбэк — ищем регуляркой в raw если что-то не нашли
    if not (phone and email and tg_id and username):
        f2 = _extract_fields(raw)
        phone    = phone    or f2["phone"]
        email    = email    or f2["email"]
        tg_id    = tg_id    or f2["tg_id"]
        username = username or f2["username"]

    return {
        "phone":    phone,
        "username": username,
        "email":    email,
        "tg_id":    tg_id,
        "name":     name,
        "message":  message,
        "raw_data": raw[:1000],
        "extra":    " | ".join(extra_parts)[:500] or None,
    }


def _parse_uploaded_file(file_path: str, file_ext: str, progress_cb=None):
    """Умный универсальный парсер любого формата.
    Возвращает (rows: list[dict], detected_cols: dict[type -> list[col_name]])
    progress_cb(done, total) вызывается каждые 50к строк при парсинге CSV.
    """
    ext  = file_ext.lower().lstrip(".")
    rows = []
    enc  = _guess_encoding(file_path)
    MAX  = 500_000
    # Словарь: тип -> список названий колонок этого типа
    detected_cols: dict = {}

    def _reg_cols(col_map: dict):
        for col, ctype in col_map.items():
            if ctype not in ("extra",):
                detected_cols.setdefault(ctype, [])
                if col not in detected_cols[ctype]:
                    detected_cols[ctype].append(col)

    try:
        # ── TXT / LOG ────────────────────────────────────────────────
        if ext in ("txt", "log"):
            with open(file_path, "r", encoding=enc, errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= MAX: break
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    rd = {}
                    for sep in (";", ":", "|", "\t", ","):
                        if line.count(sep) >= 1:
                            parts = [p.strip() for p in line.split(sep)]
                            rd = {f"f{j}": p for j, p in enumerate(parts)}
                            break
                    raw = line[:1000]
                    r = _smart_row(rd, {}, raw)
                    r["row_index"] = i
                    rows.append(r)

        # ── CSV ──────────────────────────────────────────────────────
        elif ext == "csv":
            with open(file_path, "r", encoding=enc, errors="replace") as f:
                sample = f.read(32768); f.seek(0)
                try:    dialect = _csv.Sniffer().sniff(sample, delimiters=',;\t|')
                except: dialect = _csv.excel
                reader  = _csv.DictReader(f, dialect=dialect)
                headers = reader.fieldnames or []
                col_map = {h.lower().strip(): _classify_col(h) for h in headers if h}
                _reg_cols(col_map)

                # Кешируем часто используемые типы колонок для скорости
                phone_cols    = [c for c,t in col_map.items() if t == "phone"]
                email_cols    = [c for c,t in col_map.items() if t == "email"]
                user_cols     = [c for c,t in col_map.items() if t == "username"]
                id_cols       = [c for c,t in col_map.items() if t == "tg_id"]
                name_cols     = [c for c,t in col_map.items() if t == "name"]
                msg_cols      = [c for c,t in col_map.items() if t == "message"]
                extra_cols    = [c for c,t in col_map.items() if t in ("city","date","extra")]

                def _fast_row(rd: dict, i: int) -> dict:
                    # Берём значения напрямую по заранее известным колонкам — без classify в цикле
                    phone = next((rd[c] for c in phone_cols if rd.get(c)), None)
                    email = next((rd[c] for c in email_cols if rd.get(c) and "@" in rd[c]), None)
                    uname = next((rd[c] for c in user_cols  if rd.get(c)), None)
                    tgid  = next((rd[c] for c in id_cols    if rd.get(c)), None)
                    name  = next((rd[c] for c in name_cols  if rd.get(c) and len(rd[c]) >= 2), None)
                    msg   = next((rd[c] for c in msg_cols   if rd.get(c)), None)
                    raw   = None  # строим только если нужно

                    # Чистим телефон
                    if phone:
                        d = _re_udb.sub(r'[^\d+]','', phone)
                        phone = d[:20] if len(_re_udb.sub(r'\D','',d)) >= 7 else None
                    # Чистим username
                    if uname:
                        uname = uname.strip().lstrip("@")
                        if not _re_udb.match(r'^[A-Za-z][A-Za-z0-9_]{2,31}$', uname):
                            uname = None
                    # Чистим tg_id
                    if tgid:
                        d = _re_udb.sub(r'\D','', tgid)
                        try:    tgid = d[:15] if 5 <= len(d) <= 15 and 10000 <= int(d) <= 9_999_999_999 else None
                        except: tgid = None
                    # Если ничего не нашли через колонки — фоллбэк на regex
                    if not (phone and email and tgid and uname):
                        raw = " ".join(v for v in rd.values() if v)[:500]
                        f2  = _extract_fields(raw)
                        phone = phone or f2["phone"]
                        email = email or f2["email"]
                        tgid  = tgid  or f2["tg_id"]
                        uname = uname or f2["username"]

                    extra_str = " | ".join(
                        f"{c}={rd[c][:40]}" for c in extra_cols if rd.get(c)
                    )[:300] or None

                    return {
                        "phone": phone, "username": uname, "email": (email or "")[:200] or None,
                        "tg_id": tgid,  "name": (name or "")[:120] or None,
                        "message": (msg or "")[:500] or None,
                        "raw_data": (raw or " ".join(v for v in rd.values() if v)[:500]),
                        "extra": extra_str, "row_index": i,
                    }

                # Оцениваем общее кол-во строк по размеру файла и первым строкам
                file_size = _os.path.getsize(file_path)
                total_est = [0]
                PROG_EVERY = 50_000

                for i, row in enumerate(reader):
                    if i >= MAX: break
                    # Обновляем прогресс каждые 50к строк
                    if progress_cb and i > 0 and i % PROG_EVERY == 0:
                        if total_est[0] == 0 and i > 0:
                            # Оцениваем общее кол-во по позиции в файле
                            try:
                                pos = f.tell()
                                if pos > 0:
                                    total_est[0] = int(file_size / pos * i)
                            except Exception:
                                pass
                        try: progress_cb(i, total_est[0] or i * 2)
                        except Exception: pass
                    rd = {k.lower().strip(): (v or "").strip() for k, v in row.items() if k}
                    rows.append(_fast_row(rd, i))

        # ── JSON ─────────────────────────────────────────────────────
        elif ext == "json":
            with open(file_path, "r", encoding=enc, errors="replace") as f:
                data = _json.load(f)
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list): data = v; break
                else:
                    data = [data]
            items = data if isinstance(data, list) else [data]
            for i, item in enumerate(items):
                if i >= MAX: break
                if isinstance(item, dict):
                    rd      = {k.lower(): str(v).strip() for k, v in item.items() if v is not None}
                    raw     = _json.dumps(item, ensure_ascii=False)[:1000]
                    col_map = {k: _classify_col(k) for k in rd}
                    if i == 0: _reg_cols(col_map)
                    r       = _smart_row(rd, col_map, raw)
                else:
                    raw = str(item)[:1000]
                    f2  = _extract_fields(raw)
                    r   = {"phone": f2["phone"], "username": f2["username"],
                           "email": f2["email"], "tg_id": f2["tg_id"],
                           "name": None, "message": raw[:500], "raw_data": raw, "extra": None}
                r["row_index"] = i
                rows.append(r)

        # ── SQL ──────────────────────────────────────────────────────
        elif ext == "sql":
            with open(file_path, "r", encoding=enc, errors="replace") as f:
                content = f.read()
            tbl_re = _re_udb.compile(r'CREATE TABLE[^(]*\((.+?)\)\s*;', _re_udb.DOTALL|_re_udb.IGNORECASE)
            col_re = _re_udb.compile(r'[`"\']?(\w+)[`"\']?\s+\w')
            ins_re = _re_udb.compile(r'INSERT\s+INTO\s+[`"\']?\w+[`"\']?\s*(?:\(([^)]*)\))?\s*VALUES\s*(.+?)(?=INSERT\s+INTO|\Z)', _re_udb.IGNORECASE|_re_udb.DOTALL)
            row_re = _re_udb.compile(r'\(([^()]+)\)')
            headers = []
            tm = tbl_re.search(content)
            if tm: headers = [m.group(1) for m in col_re.finditer(tm.group(1))]
            col_map = {h.lower(): _classify_col(h) for h in headers}
            _reg_cols(col_map)
            i = 0
            for m in ins_re.finditer(content):
                cols_str, vals_str = m.group(1), m.group(2)
                if cols_str:
                    hdrs = [c.strip().strip('`\'"') for c in cols_str.split(',')]
                    col_map = {h.lower(): _classify_col(h) for h in hdrs}
                    _reg_cols(col_map)
                    headers = hdrs
                for rm in row_re.finditer(vals_str):
                    if i >= MAX: break
                    raw  = rm.group(1)[:1000]
                    vals = [v.strip().strip("'\"") for v in _re_udb.split(r',(?=(?:[^\']*\'[^\']*\')*[^\']*$)', raw)]
                    if headers and len(vals) >= len(headers):
                        rd = {headers[j].lower(): vals[j] for j in range(len(headers))}
                        r  = _smart_row(rd, col_map, raw)
                    else:
                        f2 = _extract_fields(raw)
                        r  = {"phone": f2["phone"], "username": f2["username"],
                              "email": f2["email"], "tg_id": f2["tg_id"],
                              "name": None, "message": None, "raw_data": raw, "extra": None}
                    r["row_index"] = i; rows.append(r); i += 1
                if i >= MAX: break

        # ── SQLite / DB ──────────────────────────────────────────────
        elif ext in ("db", "sqlite"):
            import sqlite3 as _s3
            src = _s3.connect(file_path)
            src.row_factory = _s3.Row
            tables = [r[0] for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            i = 0
            for tbl in tables:
                try:
                    cur = src.execute(f"SELECT * FROM [{tbl}] LIMIT {MAX}")
                    hdrs = [d[0] for d in cur.description] if cur.description else []
                    col_map = {h.lower(): _classify_col(h) for h in hdrs}
                    _reg_cols(col_map)
                    for tr in cur.fetchall():
                        if i >= MAX: break
                        rd  = {hdrs[j].lower(): str(tr[j]).strip() for j in range(len(hdrs)) if tr[j] is not None}
                        raw = " | ".join(f"{k}={v}" for k,v in rd.items())[:1000]
                        r   = _smart_row(rd, col_map, raw)
                        r["extra"] = ((r.get("extra") or "") + f" [tbl:{tbl}]")[:200]
                        r["row_index"] = i; rows.append(r); i += 1
                except Exception: pass
                if i >= MAX: break
            src.close()

        # ── XLSX ─────────────────────────────────────────────────────
        elif ext in ("xlsx", "xls"):
            try:
                import openpyxl as _opx
                wb = _opx.load_workbook(file_path, read_only=True, data_only=True)
                i  = 0
                for ws in wb.worksheets:
                    headers = None; col_map = {}
                    for row in ws.iter_rows(values_only=True):
                        if headers is None:
                            headers = [str(c).lower().strip() if c else f"col{j}" for j,c in enumerate(row)]
                            col_map = {h: _classify_col(h) for h in headers}
                            _reg_cols(col_map)
                            continue
                        if i >= MAX: break
                        rd  = {headers[j]: str(v).strip() for j,v in enumerate(row)
                               if j < len(headers) and v is not None and str(v).strip()}
                        raw = " | ".join(f"{k}={v}" for k,v in rd.items())[:1000]
                        r   = _smart_row(rd, col_map, raw)
                        r["extra"] = ((r.get("extra") or "") + f" [sheet:{ws.title}]")[:200]
                        r["row_index"] = i; rows.append(r); i += 1
                    if i >= MAX: break
                wb.close()
            except ImportError:
                rows.append({"phone":None,"username":None,"email":None,"tg_id":None,
                             "name":None,"message":"Установи openpyxl: pip install openpyxl",
                             "raw_data":"","extra":None,"row_index":0})

        # ── XML ──────────────────────────────────────────────────────
        elif ext == "xml":
            import xml.etree.ElementTree as _ET
            root = _ET.parse(file_path).getroot()
            i = 0
            for elem in root.iter():
                if i >= MAX: break
                rd  = {k.lower(): v for k,v in elem.attrib.items()}
                if elem.text and elem.text.strip():
                    rd["_text"] = elem.text.strip()
                if not rd: continue
                raw     = " ".join(f"{k}={v}" for k,v in rd.items())[:1000]
                col_map = {k: _classify_col(k) for k in rd}
                if i == 0: _reg_cols(col_map)
                r = _smart_row(rd, col_map, raw)
                r["row_index"] = i; rows.append(r); i += 1

    except Exception as e:
        rows.append({"phone":None,"username":None,"email":None,"tg_id":None,
                     "name":None,"message":f"Ошибка парсинга: {e}",
                     "raw_data":str(e),"extra":None,"row_index":0})

    result = [r for r in rows if any([
        r.get("phone"), r.get("username"), r.get("email"),
        r.get("tg_id"), r.get("name"), r.get("message"),
        (r.get("raw_data") or "").strip(),
    ])]
    return result, detected_cols


def _save_udb(db_name: str, orig_name: str, file_ext: str, uploader_id: int, rows: list[dict], progress_cb=None) -> int:
    """Сохранить базу быстро — чанки по 20к строк."""
    now_s = datetime.datetime.now().isoformat(timespec="seconds")
    CHUNK = 20_000

    with db_lock, get_conn() as conn:
        db_id = conn.execute(
            "INSERT INTO uploaded_databases (name, orig_name, file_format, uploaded_by, uploaded_at, total_rows)"
            " VALUES (?,?,?,?,?,?)",
            (db_name, orig_name, file_ext, uploader_id, now_s, len(rows))
        ).lastrowid

    sql = ("INSERT INTO udb_entries "
           "(db_id,phone,username,email,tg_id,name,message,raw_data,extra,row_index)"
           " VALUES (?,?,?,?,?,?,?,?,?,?)")
    for off in range(0, len(rows), CHUNK):
        chunk = rows[off: off + CHUNK]
        data  = [
            (db_id, r.get("phone"), r.get("username"), r.get("email"), r.get("tg_id"),
             r.get("name"), r.get("message"), r.get("raw_data"), r.get("extra"),
             r.get("row_index", off + i))
            for i, r in enumerate(chunk)
        ]
        with db_lock, get_conn() as conn:
            conn.executemany(sql, data)
        if progress_cb:
            try: progress_cb(min(off + CHUNK, len(rows)))
            except Exception: pass

    return db_id


def _udb_list(uploader_id: int = None) -> list:
    with get_conn() as conn:
        if uploader_id:
            return conn.execute(
                "SELECT * FROM uploaded_databases WHERE uploaded_by=? ORDER BY id DESC",
                (uploader_id,)
            ).fetchall()
        return conn.execute("SELECT * FROM uploaded_databases ORDER BY id DESC").fetchall()


def _udb_normalize_phone(ph: str) -> list:
    """Возвращает список вариантов нормализации номера для поиска."""
    digits = _re.sub(r"\D", "", ph)
    variants = set()
    variants.add(ph.strip())
    if len(digits) >= 10:
        variants.add(digits)                     # все цифры: 79161234567
        variants.add(digits[-10:])               # последние 10: 9161234567
        if digits.startswith("7"):
            variants.add("8" + digits[1:])       # 89161234567
            variants.add("+7" + digits[1:])      # +79161234567
        elif digits.startswith("8"):
            variants.add("7" + digits[1:])
            variants.add("+7" + digits[1:])
        elif len(digits) == 10:
            variants.add("7" + digits)
            variants.add("8" + digits)
            variants.add("+7" + digits)
    return list(variants)


def _udb_search(db_id: int, query: str, field: str = "all", limit: int = 50, offset: int = 0) -> list:
    """Поиск внутри конкретной базы. Поддерживает нормализацию номеров и кириллицу."""
    q  = query.strip()
    if not q:
        return []
    ql      = q.lower()
    like    = f"%{q}%"
    likel   = f"%{ql}%"

    with get_conn() as conn:
        if field == "phone":
            # Ищем по всем нормализованным вариантам номера
            ph_variants = _udb_normalize_phone(q)
            conditions  = " OR ".join(["phone LIKE ?" for _ in ph_variants])
            params      = [f"%{v}%" for v in ph_variants] + [db_id, limit, offset]
            return conn.execute(
                f"SELECT * FROM udb_entries WHERE db_id=? AND ({conditions}) LIMIT ? OFFSET ?",
                [db_id] + [f"%{v}%" for v in ph_variants] + [limit, offset]
            ).fetchall()

        elif field == "username":
            return conn.execute(
                "SELECT * FROM udb_entries WHERE db_id=? AND (username LIKE ? OR username LIKE ?) LIMIT ? OFFSET ?",
                (db_id, like, likel, limit, offset)
            ).fetchall()

        elif field == "email":
            return conn.execute(
                "SELECT * FROM udb_entries WHERE db_id=? AND email LIKE ? LIMIT ? OFFSET ?",
                (db_id, likel, limit, offset)
            ).fetchall()

        elif field == "tg_id":
            return conn.execute(
                "SELECT * FROM udb_entries WHERE db_id=? AND tg_id LIKE ? LIMIT ? OFFSET ?",
                (db_id, like, limit, offset)
            ).fetchall()

        elif field == "name":
            # Ищем в name И raw_data — кириллица: оба варианта регистра
            return conn.execute(
                """SELECT * FROM udb_entries WHERE db_id=?
                   AND (name LIKE ? OR name LIKE ?
                        OR raw_data LIKE ? OR raw_data LIKE ?)
                   LIMIT ? OFFSET ?""",
                (db_id, like, likel, like, likel, limit, offset)
            ).fetchall()

        else:  # all — поиск по всем полям
            # Для телефона добавляем нормализованные варианты
            ph_variants = _udb_normalize_phone(q)
            ph_cond     = " OR ".join(["phone LIKE ?" for _ in ph_variants])
            ph_params   = [f"%{v}%" for v in ph_variants]
            rows = conn.execute(
                f"""SELECT * FROM udb_entries WHERE db_id=?
                   AND ({ph_cond}
                        OR username LIKE ? OR username LIKE ?
                        OR email LIKE ?
                        OR tg_id LIKE ?
                        OR name LIKE ? OR name LIKE ?
                        OR message LIKE ? OR message LIKE ?
                        OR raw_data LIKE ? OR raw_data LIKE ?)
                   LIMIT ? OFFSET ?""",
                [db_id] + ph_params + [
                    like, likel,
                    likel,
                    like,
                    like, likel,
                    like, likel,
                    like, likel,
                    limit, offset
                ]
            ).fetchall()
            return rows


def _udb_get_field_data(db_id: int, field: str, limit: int = 200, offset: int = 0) -> list:
    """Получить все записи определённого поля из базы."""
    with get_conn() as conn:
        if field == "phones":
            return conn.execute(
                "SELECT DISTINCT phone FROM udb_entries WHERE db_id=? AND phone IS NOT NULL AND phone!='' LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()
        elif field == "usernames":
            return conn.execute(
                "SELECT DISTINCT username FROM udb_entries WHERE db_id=? AND username IS NOT NULL AND username!='' LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()
        elif field == "emails":
            return conn.execute(
                "SELECT DISTINCT email FROM udb_entries WHERE db_id=? AND email IS NOT NULL AND email!='' LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()
        elif field == "tg_ids":
            return conn.execute(
                "SELECT DISTINCT tg_id FROM udb_entries WHERE db_id=? AND tg_id IS NOT NULL AND tg_id!='' LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()
        elif field == "names":
            return conn.execute(
                "SELECT DISTINCT name FROM udb_entries WHERE db_id=? AND name IS NOT NULL AND name!='' LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()
        elif field == "messages":
            return conn.execute(
                "SELECT message FROM udb_entries WHERE db_id=? AND message IS NOT NULL AND message!='' LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()
        else:  # all
            return conn.execute(
                "SELECT * FROM udb_entries WHERE db_id=? LIMIT ? OFFSET ?",
                (db_id, limit, offset)
            ).fetchall()


def _udb_stats(db_id: int) -> dict:
    with get_conn() as conn:
        db   = conn.execute("SELECT * FROM uploaded_databases WHERE id=?", (db_id,)).fetchone()
        r    = conn.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(phone) AS phones,
                COUNT(username) AS usernames,
                COUNT(email) AS emails,
                COUNT(tg_id) AS tg_ids,
                COUNT(name) AS names,
                COUNT(message) AS messages
            FROM udb_entries WHERE db_id=?
        """, (db_id,)).fetchone()
    return {"db": db, "stats": r}


# ── UDB меню helpers ─────────────────────────────────────────────

def _udb_main_menu_kb() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    dbs = _udb_list()
    if dbs:
        for db in dbs[:20]:
            kb.add(types.InlineKeyboardButton(
                f"📂 {db['name']} ({db['total_rows']:,} записей)",
                callback_data=f"udb_open:{db['id']}"
            ))
    else:
        kb.add(types.InlineKeyboardButton("📭 Баз нет — загрузи файл", callback_data="noop"))
    kb.add(types.InlineKeyboardButton("📤 Загрузить базу (отправь файл)", callback_data="udb_upload_hint"))
    kb.add(types.InlineKeyboardButton("🔍 Поиск по всем базам", callback_data="udb_search_all"))
    kb.add(types.InlineKeyboardButton("◀️ Главное меню", callback_data="main_menu"))
    return kb


def _udb_db_menu_kb(db_id: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("📱 Телефоны",   callback_data=f"udb_view:{db_id}:phones:0"),
        types.InlineKeyboardButton("👤 Юзернеймы",  callback_data=f"udb_view:{db_id}:usernames:0"),
    )
    kb.row(
        types.InlineKeyboardButton("📧 Email",      callback_data=f"udb_view:{db_id}:emails:0"),
        types.InlineKeyboardButton("🆔 Telegram ID", callback_data=f"udb_view:{db_id}:tg_ids:0"),
    )
    kb.row(
        types.InlineKeyboardButton("📛 Имена",      callback_data=f"udb_view:{db_id}:names:0"),
        types.InlineKeyboardButton("💬 Сообщения",  callback_data=f"udb_view:{db_id}:messages:0"),
    )
    kb.add(types.InlineKeyboardButton("📊 Все данные",  callback_data=f"udb_view:{db_id}:all:0"))
    kb.add(types.InlineKeyboardButton("🔍 Поиск в базе", callback_data=f"udb_search:{db_id}:all"))
    kb.row(
        types.InlineKeyboardButton("🗑 Удалить базу", callback_data=f"udb_delete:{db_id}"),
        types.InlineKeyboardButton("◀️ К списку",     callback_data="udb_list"),
    )
    return kb


def _fmt_udb_entry(e) -> str:
    """Форматирует запись из базы — красиво, с копируемыми полями."""
    lines = []
    if e["tg_id"]:
        lines.append(f"  🆔 TG ID: <code>{escape_html(str(e['tg_id']).strip())}</code>")
    if e["phone"]:
        lines.append(f"  📱 Тел: <code>{escape_html(str(e['phone']).strip())}</code>")
    if e["username"]:
        un = str(e["username"]).strip().lstrip("@")
        lines.append(f"  👤 Username: <code>@{escape_html(un)}</code>")
    if e["name"]:
        lines.append(f"  📛 Имя: <b>{escape_html(str(e['name']).strip())}</b>")
    if e["email"]:
        lines.append(f"  📧 Email: <code>{escape_html(str(e['email']).strip())}</code>")
    if e["message"]:
        lines.append(f"  💬 {escape_html(str(e['message']).strip()[:120])}")
    if not lines and e["raw_data"]:
        lines.append(f"  📄 <code>{escape_html(str(e['raw_data']).strip()[:200])}</code>")
    return "\n".join(lines) if lines else "  —"


# ── Обработка загружаемого файла ─────────────────────────────────

UDB_ALLOWED_EXTS = {"txt","csv","json","sql","db","sqlite","xlsx","xls","log","xml"}

@bot.message_handler(content_types=["document"])
def handle_document(msg: types.Message):
    uid = msg.from_user.id if msg.from_user else 0
    if uid not in ADMIN_IDS and not is_staff(uid):
        # Передаём обычным юзерам
        return

    doc  = msg.document
    name = doc.file_name or "unknown"
    ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""

    if ext not in UDB_ALLOWED_EXTS:
        bot.send_message(msg.chat.id,
            f"📎 <b>Файл получен</b>: <code>{escape_html(name)}</code>\n\n"
            f"⚠️ Формат <b>.{ext}</b> не поддерживается для баз данных.\n"
            f"Поддерживаемые: txt, csv, json, sql, db, sqlite, xlsx, log, xml",
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("🗄 Базы данных", callback_data="udb_list"),
                types.InlineKeyboardButton("🏠 Меню",        callback_data="main_menu"),
            ))
        return

    # Определяем имя базы = имя файла без расширения
    db_name = name.rsplit(".", 1)[0] if "." in name else name

    status_msg = bot.send_message(msg.chat.id,
        f"<code>┌──────────────────────────┐</code>\n"
        f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
        f"<code>└──────────────────────────┘</code>\n\n"
        f"📄 Файл: <b>{escape_html(name)}</b>\n"
        f"📋 Формат: <b>.{ext}</b>\n\n"
        f"⏳ Скачиваю файл...")

    def _do_import():
        tmp_path = None
        try:
            file_size_mb = (doc.file_size or 0) / 1024 / 1024

            with _tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tf:
                tmp_path = tf.name

            def _upd_progress(downloaded_mb, total_mb=None):
                try:
                    total_str = f" / {total_mb:.0f}" if total_mb else ""
                    pct = min(1.0, downloaded_mb / total_mb) if total_mb else 0
                    bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
                    safe_edit(
                        f"<code>┌──────────────────────────┐</code>\n"
                        f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                        f"<code>└──────────────────────────┘</code>\n\n"
                        f"📄 <b>{escape_html(name)}</b>\n"
                        f"⬇️ <b>{downloaded_mb:.0f}{total_str} МБ</b>\n"
                        f"<code>{bar}</code>",
                        msg.chat.id, status_msg.message_id
                    )
                except Exception:
                    pass

            safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📄 Файл: <b>{escape_html(name)}</b>\n"
                f"📦 Размер: <b>{file_size_mb:.1f} МБ</b>\n\n"
                f"⏳ Скачиваю...",
                msg.chat.id, status_msg.message_id
            )

            downloaded_ok = False

            # ── МЕТОД 1: стандартный Bot API (работает до ~20 МБ) ────────
            if file_size_mb <= 19:
                try:
                    file_info  = bot.get_file(doc.file_id)
                    file_bytes = bot.download_file(file_info.file_path)
                    with open(tmp_path, "wb") as f:
                        f.write(file_bytes)
                    downloaded_ok = True
                except Exception:
                    pass

            # ── МЕТОД 2: потоковый urllib (иногда работает чуть выше 20 МБ) ──
            if not downloaded_ok:
                try:
                    import urllib.request as _urlreq
                    file_info = bot.get_file(doc.file_id)
                    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"
                    downloaded = 0
                    last_upd   = [0.0]
                    with _urlreq.urlopen(url, timeout=600) as resp, open(tmp_path, "wb") as f:
                        while True:
                            chunk = resp.read(1024 * 512)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                            mb = downloaded / 1024 / 1024
                            if mb - last_upd[0] >= 5:
                                last_upd[0] = mb
                                _upd_progress(mb, file_size_mb)
                    downloaded_ok = True
                except Exception:
                    pass

            # ── МЕТОД 3: Telethon userbot — качает файлы ЛЮБОГО размера ──
            if not downloaded_ok:
                if not (TELETHON_OK and _userbot_client is not None):
                    raise Exception(
                        f"Файл слишком большой ({file_size_mb:.0f} МБ) для Bot API.\n\n"
                        f"⚡ Подключи userbot через /account — он скачивает файлы любого размера автоматически!"
                    )

                safe_edit(
                    f"<code>┌──────────────────────────┐</code>\n"
                    f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                    f"<code>└──────────────────────────┘</code>\n\n"
                    f"📄 <b>{escape_html(name)}</b>\n"
                    f"📦 {file_size_mb:.0f} МБ — скачиваю через userbot...",
                    msg.chat.id, status_msg.message_id
                )

                # Пересылаем файл в Saved Messages userbot'а чтобы он мог его скачать
                forwarded_msg_id = [None]
                try:
                    fwd = bot.forward_message(
                        chat_id=ACCOUNT_USER_ID,
                        from_chat_id=msg.chat.id,
                        message_id=msg.message_id
                    )
                    forwarded_msg_id[0] = fwd.message_id
                except Exception:
                    pass

                import asyncio as _aio
                import concurrent.futures as _cf

                last_upd_ub = [0.0]

                async def _dl_userbot():
                    async def _cb(current, total):
                        mb_c = current / 1024 / 1024
                        mb_t = (total or 1) / 1024 / 1024
                        if mb_c - last_upd_ub[0] >= 5:
                            last_upd_ub[0] = mb_c
                            _upd_progress(mb_c, mb_t)

                    media_obj = None

                    # Способ 1: берём пересланное сообщение из Saved Messages по message_id
                    if forwarded_msg_id[0]:
                        try:
                            tg_msg = await _userbot_client.get_messages(
                                "me", ids=forwarded_msg_id[0]
                            )
                            if tg_msg and tg_msg.media:
                                media_obj = tg_msg
                        except Exception:
                            pass

                    # Способ 2: ищем последние 10 сообщений в Saved Messages с документом
                    if media_obj is None:
                        try:
                            async for saved_msg in _userbot_client.iter_messages("me", limit=10):
                                if saved_msg.document:
                                    media_obj = saved_msg
                                    break
                        except Exception:
                            pass

                    # Способ 3: пробуем напрямую из чата отправителя
                    if media_obj is None:
                        try:
                            tg_msg = await _userbot_client.get_messages(
                                uid, ids=msg.message_id
                            )
                            if tg_msg and tg_msg.media:
                                media_obj = tg_msg
                        except Exception:
                            pass

                    if media_obj is None:
                        raise Exception(
                            "Не удалось получить файл через userbot.\n\n"
                            "Попробуй переслать файл в <b>Избранное</b> (Saved Messages) "
                            "в Telegram и снова отправить боту — или подожди 5 сек и повтори."
                        )

                    await _userbot_client.download_media(
                        media_obj,
                        file=tmp_path,
                        progress_callback=_cb
                    )

                    # Удаляем пересланное сообщение из Saved Messages
                    if forwarded_msg_id[0]:
                        try:
                            await _userbot_client.delete_messages("me", [forwarded_msg_id[0]])
                        except Exception:
                            pass

                # Используем глобальный _userbot_loop — именно в нём работает Telethon
                import asyncio as _aio
                if _userbot_loop is None or not _userbot_loop.is_running():
                    raise Exception(
                        "Userbot не активен. Перезапусти бота и переподключи userbot через /account"
                    )
                fut = _aio.run_coroutine_threadsafe(_dl_userbot(), _userbot_loop)
                fut.result(timeout=1800)

                downloaded_ok = True

            # ── Проверяем результат ───────────────────────────────────────
            if not downloaded_ok or not _os.path.exists(tmp_path) or _os.path.getsize(tmp_path) == 0:
                raise Exception("Файл не скачан или пустой")

            actual_mb = _os.path.getsize(tmp_path) / 1024 / 1024

            safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📄 <b>{escape_html(name)}</b>  ({actual_mb:.1f} МБ)\n\n"
                f"🔍 Анализирую структуру...",
                msg.chat.id, status_msg.message_id
            )

            rows, detected_cols = _parse_uploaded_file(tmp_path, ext,
                progress_cb=lambda done, total: safe_edit(
                    f"<code>┌──────────────────────────┐</code>\n"
                    f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                    f"<code>└──────────────────────────┘</code>\n\n"
                    f"📄 <b>{escape_html(name)}</b>\n"
                    f"🔍 Разбираю: <b>{done:,} / {total:,}</b>\n"
                    f"<code>{'█'*min(20,int(done/total*20))}{'░'*(20-min(20,int(done/total*20)))}</code>",
                    msg.chat.id, status_msg.message_id
                ) if total else None
            )

            safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📄 <b>{escape_html(name)}</b>\n"
                f"📋 Найдено записей: <b>{len(rows):,}</b>\n\n"
                f"💾 Сохраняю в систему...",
                msg.chat.id, status_msg.message_id
            )

            total_rows = len(rows)

            def _save_progress(saved: int):
                if total_rows > 50_000:
                    pct = min(1.0, saved / total_rows)
                    bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
                    try:
                        safe_edit(
                            f"<code>┌──────────────────────────┐</code>\n"
                            f"<code>│  📂  ЗАГРУЗКА БАЗЫ       │</code>\n"
                            f"<code>└──────────────────────────┘</code>\n\n"
                            f"📄 <b>{escape_html(name)}</b>\n"
                            f"💾 Сохраняю: <b>{saved:,} / {total_rows:,}</b>\n"
                            f"<code>{bar}</code>",
                            msg.chat.id, status_msg.message_id
                        )
                    except Exception:
                        pass

            db_id = _save_udb(db_name, name, ext, uid, rows, progress_cb=_save_progress)
            st = _udb_stats(db_id)
            s  = st["stats"]

            # Строим блок обнаруженных колонок
            col_lines = []
            TYPE_EMOJI = {
                "phone": "📱", "email": "📧", "username": "👤", "tg_id": "🆔",
                "name": "📛", "message": "💬", "city": "🏙", "date": "📅", "extra": "📌"
            }
            for ctype, cols in detected_cols.items():
                if cols:
                    emoji = TYPE_EMOJI.get(ctype, "•")
                    cols_str = ", ".join(f"<code>{escape_html(c)}</code>" for c in cols[:5])
                    col_lines.append(f"{emoji} {cols_str}")
            cols_block = "\n".join(col_lines) if col_lines else "<i>структура не определена</i>"

            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton("📂 Открыть базу", callback_data=f"udb_open:{db_id}"),
                types.InlineKeyboardButton("🗄 Все базы",     callback_data="udb_list"),
            )
            kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

            safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  ✅  БАЗА ЗАГРУЖЕНА       │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📂 <b>{escape_html(db_name)}</b>\n"
                f"📋 Формат: <b>.{ext}</b>\n"
                f"📊 Всего записей: <b>{len(rows):,}</b>\n\n"
                f"<code>── Найденные данные ───────</code>\n"
                f"📱 Телефоны:    <b>{s['phones']:,}</b>\n"
                f"👤 Юзернеймы:  <b>{s['usernames']:,}</b>\n"
                f"📧 Email:       <b>{s['emails']:,}</b>\n"
                f"🆔 Telegram ID: <b>{s['tg_ids']:,}</b>\n"
                f"📛 Имена:       <b>{s['names']:,}</b>\n"
                f"💬 Сообщения:   <b>{s['messages']:,}</b>\n\n"
                f"<code>── Колонки файла ──────────</code>\n"
                f"{cols_block}",
                msg.chat.id, status_msg.message_id,
                reply_markup=kb
            )
        except Exception as e:
            try:
                safe_edit(
                    f"❌ <b>Ошибка при загрузке базы</b>\n\n{escape_html(str(e)[:500])}",
                    msg.chat.id, status_msg.message_id,
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu")
                    ))
            except Exception:
                pass
        finally:
            if tmp_path and _os.path.exists(tmp_path):
                try:
                    _os.unlink(tmp_path)
                except Exception:
                    pass

    threading.Thread(target=_do_import, daemon=True).start()


# ── UDB Callback обработчики ─────────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data == "udb_list" or c.data.startswith("udb_"))
def handle_udb_callbacks(call: types.CallbackQuery):
    uid  = call.from_user.id
    data = call.data

    if uid not in ADMIN_IDS and not is_staff(uid):
        bot.answer_callback_query(call.id, "⛔ Доступ запрещён"); return

    if data == "udb_list" or data == "databases_menu":
        dbs = _udb_list()
        text = (
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🗄  БАЗЫ  ДАННЫХ        │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
        )
        if dbs:
            text += f"Загружено баз: <b>{len(dbs)}</b>\n\n"
            for db in dbs[:20]:
                text += f"• <b>{escape_html(db['name'])}</b> <i>(.{db['file_format']})</i>  —  {db['total_rows']:,} записей\n"
        else:
            text += "📭 <i>Баз данных ещё нет.</i>\n\nОтправь боту файл в формате:\n<code>.txt .csv .json .sql .db .sqlite .xlsx .log .xml</code>"
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=_udb_main_menu_kb())
        bot.answer_callback_query(call.id)

    elif data == "udb_upload_hint":
        bot.answer_callback_query(call.id,
            "📤 Просто отправь файл (.txt .csv .json .sql .db .sqlite .xlsx .log .xml) прямо в чат боту!",
            show_alert=True)

    elif data == "udb_search_all":
        user_states[uid] = "udb_search_all"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔍  ПОИСК ПО ВСЕМ БАЗАМ │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Введи запрос — ищем по:\n"
            f"• телефону  • username  • email  • TG ID  • тексту\n\n"
            f"<i>Поиск по всем загруженным базам сразу</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data="udb_list")
            ))
        bot.answer_callback_query(call.id)

    elif data.startswith("udb_open:"):
        db_id = int(data.split(":")[1])
        st = _udb_stats(db_id)
        if not st["db"]:
            bot.answer_callback_query(call.id, "❌ База не найдена"); return
        db = st["db"]
        s  = st["stats"]
        text = (
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  📂  {escape_html(db['name'])[:20]:<20}│</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"📋 Формат: <b>.{db['file_format']}</b>  |  📅 {(db['uploaded_at'] or '')[:10]}\n"
            f"📊 Всего записей: <b>{s['total']:,}</b>\n\n"
            f"<code>──────────────────────────</code>\n"
            f"📱 Телефоны:    <b>{s['phones']:,}</b>\n"
            f"👤 Юзернеймы:  <b>{s['usernames']:,}</b>\n"
            f"📧 Email:       <b>{s['emails']:,}</b>\n"
            f"🆔 Telegram ID: <b>{s['tg_ids']:,}</b>\n"
            f"📛 Имена:       <b>{s['names']:,}</b>\n"
            f"💬 Сообщения:   <b>{s['messages']:,}</b>\n\n"
            f"Выберите тип данных:"
        )
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=_udb_db_menu_kb(db_id))
        bot.answer_callback_query(call.id)

    elif data.startswith("udb_view:"):
        parts  = data.split(":")
        db_id  = int(parts[1])
        field  = parts[2]
        offset = int(parts[3]) if len(parts) > 3 else 0
        PER    = 50

        st = _udb_stats(db_id)
        db = st["db"]
        if not db:
            bot.answer_callback_query(call.id, "❌ База не найдена"); return

        rows = _udb_get_field_data(db_id, field, limit=PER, offset=offset)

        field_label = {
            "phones":"📱 Телефоны","usernames":"👤 Юзернеймы",
            "emails":"📧 Email","tg_ids":"🆔 Telegram ID",
            "names":"📛 Имена","messages":"💬 Сообщения","all":"📊 Все данные"
        }.get(field, field)

        text = (
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  {field_label[:22]:<22}│</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"📂 <b>{escape_html(db['name'])}</b>  |  показ {offset+1}–{offset+len(rows)}\n\n"
        )

        if not rows:
            text += "<i>Данных нет</i>"
        elif field in ("phones","usernames","emails","tg_ids","names"):
            col = field.rstrip("s") if field != "tg_ids" else "tg_id"
            col = {"phone":"phone","username":"username","email":"email","tg_id":"tg_id","name":"name"}.get(col, col)
            items = [escape_html(str(r[0])) for r in rows if r[0]]
            text += "\n".join(f"• <code>{v}</code>" for v in items[:50])
        elif field == "messages":
            for r in rows[:20]:
                text += f"• {escape_html(str(r[0])[:100])}\n"
        else:  # all
            for r in rows[:20]:
                text += _fmt_udb_entry(r) + "\n"
            if len(rows) > 20:
                text += f"\n<i>...и ещё {len(rows)-20} записей</i>"

        kb = types.InlineKeyboardMarkup(row_width=2)
        nav = []
        if offset > 0:
            nav.append(types.InlineKeyboardButton("◀️", callback_data=f"udb_view:{db_id}:{field}:{max(0,offset-PER)}"))
        if len(rows) == PER:
            nav.append(types.InlineKeyboardButton("▶️", callback_data=f"udb_view:{db_id}:{field}:{offset+PER}"))
        if nav:
            kb.row(*nav)
        kb.row(
            types.InlineKeyboardButton("🔍 Поиск", callback_data=f"udb_search:{db_id}:all"),
            types.InlineKeyboardButton("◀️ Назад", callback_data=f"udb_open:{db_id}"),
        )
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb)
        bot.answer_callback_query(call.id)

    elif data.startswith("udb_search:") and not data.startswith("udb_search_all") and ":" in data:
        parts2 = data.split(":")
        db_id = int(parts2[1])
        field = parts2[2] if len(parts2) > 2 else "all"
        user_states[uid] = f"udb_search:{db_id}:{field}"
        st = _udb_stats(db_id)
        db = st["db"]
        field_labels = {
            "all": "🔍 Везде",
            "name": "📛 По имени",
            "phone": "📱 По телефону",
            "username": "👤 По username",
            "tg_id": "🆔 По Telegram ID",
            "email": "📧 По email",
        }
        cur_label = field_labels.get(field, "🔍 Везде")
        kb_sf = types.InlineKeyboardMarkup(row_width=3)
        btns = []
        for f_id, f_lbl in field_labels.items():
            mark = "▪️" if f_id == field else ""
            btns.append(types.InlineKeyboardButton(
                f"{mark}{f_lbl}", callback_data=f"udb_search:{db_id}:{f_id}"
            ))
        kb_sf.add(*btns)
        kb_sf.add(types.InlineKeyboardButton("❌ Отмена", callback_data=f"udb_open:{db_id}"))
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔍  ПОИСК В БАЗЕ        │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"📂 <b>{escape_html(db['name']) if db else '?'}</b>\n"
            f"🎯 Поле: <b>{cur_label}</b>\n\n"
            f"Введи запрос для поиска.\n"
            f"<i>Поиск регистронезависимый, частичное совпадение</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=kb_sf)
        bot.answer_callback_query(call.id)

    elif data.startswith("udb_delete:"):
        db_id = int(data.split(":")[1])
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Только администраторы"); return
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"udb_delete_confirm:{db_id}"),
            types.InlineKeyboardButton("❌ Отмена",       callback_data=f"udb_open:{db_id}"),
        )
        bot.answer_callback_query(call.id)
        safe_edit(
            f"⚠️ <b>Удалить базу?</b>\n\nВсе данные будут безвозвратно удалены.",
            call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data.startswith("udb_delete_confirm:"):
        db_id = int(data.split(":")[1])
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Только администраторы"); return
        with db_lock, get_conn() as conn:
            conn.execute("DELETE FROM udb_entries WHERE db_id=?", (db_id,))
            conn.execute("DELETE FROM uploaded_databases WHERE id=?", (db_id,))
        bot.answer_callback_query(call.id, "✅ База удалена")
        # refresh list
        call.data = "udb_list"
        handle_udb_callbacks(call)

    else:
        bot.answer_callback_query(call.id)



# ─────────────────────────────────────────────
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БД
# ─────────────────────────────────────────────
def upsert_user(user: types.User, phone=None, city=None, country=None):
    now  = datetime.datetime.now().isoformat(timespec="seconds")
    lang = getattr(user, "language_code", None)
    premium = 1 if getattr(user, "is_premium", False) else 0
    with db_lock, get_conn() as conn:
        existing   = conn.execute("SELECT first_seen FROM users WHERE id=?", (user.id,)).fetchone()
        first_seen = existing["first_seen"] if existing else now
        conn.execute("""
            INSERT INTO users (id,username,first_name,last_name,phone,city,country,
                               language_code,is_premium,first_seen,last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                phone=COALESCE(excluded.phone, users.phone),
                city=COALESCE(excluded.city, users.city),
                country=COALESCE(excluded.country, users.country),
                language_code=COALESCE(excluded.language_code, users.language_code),
                is_premium=excluded.is_premium,
                last_seen=excluded.last_seen
        """, (user.id, user.username, user.first_name, user.last_name,
              phone, city, country, lang, premium, first_seen, now))

def upsert_chat(chat: types.Chat):
    now     = datetime.datetime.now().isoformat(timespec="seconds")
    desc    = getattr(chat, "description", None)
    members = getattr(chat, "member_count", None)
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO chats (id,title,chat_type,username,invite_link,description,member_count,joined_at,last_activity)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                chat_type=excluded.chat_type,
                username=COALESCE(excluded.username, chats.username),
                description=COALESCE(excluded.description, chats.description),
                member_count=COALESCE(excluded.member_count, chats.member_count),
                last_activity=excluded.last_activity
        """, (chat.id, chat.title, chat.type,
              getattr(chat,"username",None),
              getattr(chat,"invite_link",None),
              desc, members, now, now))

# ─────────────────────────────────────────────
#  СБОР УЧАСТНИКОВ ЧЕРЕЗ USERBOT
# ─────────────────────────────────────────────

def _upsert_chat_member(chat_id, user, is_admin=False, is_banned=False):
    """Сохранить одного участника в chat_members и обновить users."""
    now  = datetime.datetime.now().isoformat(timespec="seconds")
    uid  = user.id
    un   = getattr(user, "username",   None)
    fn   = getattr(user, "first_name", None) or ""
    ln   = getattr(user, "last_name",  None) or ""
    ph   = getattr(user, "phone",      None)
    bot_ = getattr(user, "bot",        False)
    jd   = None
    if hasattr(user, "participant") and user.participant:
        jd_raw = getattr(user.participant, "date", None)
        if jd_raw:
            jd = jd_raw.isoformat()
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO chat_members (chat_id,user_id,username,first_name,last_name,
                phone,is_bot,is_admin,is_banned,joined_date,scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(chat_id,user_id) DO UPDATE SET
                username=COALESCE(excluded.username, chat_members.username),
                first_name=COALESCE(excluded.first_name, chat_members.first_name),
                last_name=COALESCE(excluded.last_name, chat_members.last_name),
                phone=COALESCE(excluded.phone, chat_members.phone),
                is_admin=excluded.is_admin,
                is_banned=excluded.is_banned,
                scraped_at=excluded.scraped_at
        """, (chat_id, uid, un, fn, ln, ph, int(bool(bot_)),
              int(is_admin), int(is_banned), jd, now))
        # Также обновляем таблицу users
        conn.execute("""
            INSERT INTO users (id,username,first_name,last_name,phone,first_seen,last_seen)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                username=COALESCE(excluded.username, users.username),
                first_name=COALESCE(excluded.first_name, users.first_name),
                last_name=COALESCE(excluded.last_name, users.last_name),
                phone=COALESCE(excluded.phone, users.phone),
                last_seen=excluded.last_seen
        """, (uid, un, fn, ln, ph, now, now))


async def _async_scrape_chat(chat_id: int, status_cid: int, status_mid: int):
    """Собрать всех участников чата через userbot."""
    global _userbot_client
    now = datetime.datetime.now().isoformat(timespec="seconds")

    # Обновляем статус задачи
    def _set_status(status, done=0, total=0, error=None):
        with db_lock, get_conn() as conn:
            conn.execute("""
                INSERT INTO scrape_tasks (chat_id,status,done,total,started_at,error_msg)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    status=excluded.status,done=excluded.done,
                    total=excluded.total,error_msg=excluded.error_msg
            """, (chat_id, status, done, total, now, error))

    def _safe_upd(text, kb=None):
        try:
            safe_edit(text, status_cid, status_mid, reply_markup=kb)
        except Exception:
            pass

    try:
        _set_status("running")
        entity = await _userbot_client.get_entity(chat_id)
        chat_title = getattr(entity, "title", str(chat_id))

        # Обновляем название чата
        with db_lock, get_conn() as conn:
            conn.execute("""
                UPDATE scrape_tasks SET chat_title=? WHERE chat_id=?
            """, (chat_title, chat_id))

        _safe_upd(f"⚙️ <b>Сбор участников</b>\n📂 <b>{chat_title}</b>\n\n🔄 Получаю список...")

        # Собираем администраторов отдельно для флага
        admins = set()
        try:
            from telethon.tl.functions.channels import GetParticipantsRequest
            from telethon.tl.types import ChannelParticipantsAdmins
            result = await _userbot_client(GetParticipantsRequest(
                entity, ChannelParticipantsAdmins(), 0, 200, 0
            ))
            for p in result.users:
                admins.add(p.id)
        except Exception:
            pass

        count    = 0
        errors   = 0
        last_upd = 0
        _TS      = datetime.datetime.now().isoformat(timespec="seconds")
        mb = []; ub = []
        _SQL_CM = """INSERT INTO chat_members
            (chat_id,user_id,username,first_name,last_name,phone,is_bot,is_admin,is_banned,scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(chat_id,user_id) DO UPDATE SET
                username=COALESCE(excluded.username, chat_members.username),
                first_name=COALESCE(excluded.first_name, chat_members.first_name),
                last_name=COALESCE(excluded.last_name, chat_members.last_name),
                phone=COALESCE(excluded.phone, chat_members.phone),
                is_admin=excluded.is_admin, is_banned=excluded.is_banned,
                scraped_at=excluded.scraped_at"""
        _SQL_U = """INSERT INTO users (id,username,first_name,last_name,phone,first_seen,last_seen)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                username=COALESCE(excluded.username, users.username),
                first_name=COALESCE(excluded.first_name, users.first_name),
                last_name=COALESCE(excluded.last_name, users.last_name),
                phone=COALESCE(excluded.phone, users.phone),
                last_seen=excluded.last_seen"""

        async for user in _userbot_client.iter_participants(entity, aggressive=True):
            try:
                is_adm = user.id in admins
                is_ban = "Banned" in type(getattr(user,"participant",None)).__name__
                un = getattr(user,"username",None)
                fn = getattr(user,"first_name",None) or ""
                ln = getattr(user,"last_name",None)  or ""
                ph = getattr(user,"phone",None)
                bt = int(bool(getattr(user,"bot",False)))
                mb.append((chat_id, user.id, un, fn, ln, ph, bt, int(is_adm), int(is_ban), _TS))
                ub.append((user.id, un, fn, ln, ph, _TS, _TS))
                count += 1
            except Exception: errors += 1
            if len(mb) >= 500:
                with db_lock, get_conn() as conn:
                    conn.executemany(_SQL_CM, mb); conn.executemany(_SQL_U, ub)
                mb.clear(); ub.clear()
                _set_status("running", count, 0)
            if count - last_upd >= 500:
                last_upd = count
                _safe_upd(f"⚙️ <b>Парсинг участников</b>\n📂 <b>{chat_title}</b>\n\n"
                          f"✅ Собрано: <b>{count:,}</b>...\n<i>Работаю...</i>")
        if mb:
            with db_lock, get_conn() as conn:
                conn.executemany(_SQL_CM, mb); conn.executemany(_SQL_U, ub)

        # Обновляем member_count в chats
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE chats SET member_count=? WHERE id=?", (count, chat_id))
            conn.execute("""
                UPDATE scrape_tasks SET status='done',done=?,total=?,finished_at=?
                WHERE chat_id=?
            """, (count, count, datetime.datetime.now().isoformat(timespec="seconds"), chat_id))

        kb_done = types.InlineKeyboardMarkup(row_width=1)
        kb_done.add(
            types.InlineKeyboardButton("👥 Посмотреть участников", callback_data=f"chat_members:{chat_id}:0"),
            types.InlineKeyboardButton("◀️ К группе",              callback_data=f"chat_view:{chat_id}"),
        )
        _safe_upd(
            f"✅ <b>Парсинг завершён!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 <b>{chat_title}</b>\n"
            f"👥 Участников собрано: <b>{count}</b>\n"
            f"⚠️ Ошибок: {errors}",
            kb_done
        )

    except Exception as e:
        err = str(e)[:200]
        with db_lock, get_conn() as conn:
            conn.execute("""
                UPDATE scrape_tasks SET status='error',error_msg=? WHERE chat_id=?
            """, (err, chat_id))
        kb_err = types.InlineKeyboardMarkup()
        kb_err.add(types.InlineKeyboardButton("◀️ К группе", callback_data=f"chat_view:{chat_id}"))
        _safe_upd(f"❌ <b>Ошибка парсинга</b>\n<code>{err}</code>", kb_err)


def scrape_chat_members(chat_id: int, status_cid: int, status_mid: int):
    """Запустить парсинг в фоновом потоке."""
    def _run():
        try:
            future = asyncio.run_coroutine_threadsafe(
                _async_scrape_chat(chat_id, status_cid, status_mid),
                _userbot_loop
            )
            future.result(timeout=600)
        except Exception as e:
            try:
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"chat_view:{chat_id}"))
                safe_edit(f"❌ <b>Ошибка:</b> <code>{e}</code>", status_cid, status_mid, reply_markup=kb)
            except Exception:
                pass
    threading.Thread(target=_run, daemon=True).start()


def _probiv_user(target_id: int = None, target_username: str = None) -> dict:
    """
    Полный пробив пользователя:
    - Ищет в users, chat_members, basa_messages
    - Собирает все телефоны, имена, юзернеймы, чаты, сообщения
    - Ищет во ВСЕХ загруженных базах по ВСЕМ терминам
    """
    result = {
        "user":             None,
        "chats":            [],
        "msg_count":        0,
        "basa_count":       0,
        "phones":           [],
        "groups_in":        [],
        "name_history":     [],
        "username_history": [],
        "last_messages":    [],
        "common_users":     [],
        "risk_score":       0,
        "udb_hits":         [],
        "udb_searched_dbs": 0,
        "udb_search_terms": 0,
    }

    with get_conn() as conn:
        # ── Разрешаем пользователя ────────────────────────────────
        u = None
        if target_id:
            u = conn.execute("SELECT * FROM users WHERE id=?", (target_id,)).fetchone()
            if not u:
                cm = conn.execute("SELECT * FROM chat_members WHERE user_id=?", (target_id,)).fetchone()
                if not cm:
                    cm = conn.execute(
                        "SELECT * FROM basa_messages WHERE user_id=? LIMIT 1", (target_id,)
                    ).fetchone()
                if cm:
                    now_s = datetime.datetime.now().isoformat(timespec="seconds")
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO users
                            (id, username, first_name, last_name, phone, first_seen, last_seen)
                            VALUES (?,?,?,?,?,?,?)
                        """, (target_id,
                              cm["username"],
                              cm["first_name"], cm["last_name"],
                              cm.get("phone") if isinstance(cm, dict) else (cm["phone"] if "phone" in cm.keys() else None),
                              now_s, now_s))
                    except Exception:
                        pass
                    u = conn.execute("SELECT * FROM users WHERE id=?", (target_id,)).fetchone()
        else:
            un_clean = (target_username or "").lstrip("@").strip()
            u = conn.execute(
                "SELECT * FROM users WHERE lower(username)=lower(?)", (un_clean,)
            ).fetchone()
            if not u:
                cm = conn.execute(
                    "SELECT * FROM chat_members WHERE lower(username)=lower(?) LIMIT 1", (un_clean,)
                ).fetchone()
                if cm:
                    target_id = cm["user_id"]
                    u = conn.execute("SELECT * FROM users WHERE id=?", (target_id,)).fetchone()
                    if not u:
                        now_s = datetime.datetime.now().isoformat(timespec="seconds")
                        try:
                            conn.execute("""
                                INSERT OR IGNORE INTO users
                                (id, username, first_name, last_name, phone, first_seen, last_seen)
                                VALUES (?,?,?,?,?,?,?)
                            """, (target_id, cm["username"], cm["first_name"],
                                  cm["last_name"], cm["phone"], now_s, now_s))
                        except Exception:
                            pass
                        u = conn.execute("SELECT * FROM users WHERE id=?", (target_id,)).fetchone()
            if not u:
                bm = conn.execute(
                    "SELECT * FROM basa_messages WHERE lower(username)=lower(?) AND user_id IS NOT NULL LIMIT 1",
                    (un_clean,)
                ).fetchone()
                if bm and bm["user_id"]:
                    target_id = bm["user_id"]
                    u = conn.execute("SELECT * FROM users WHERE id=?", (target_id,)).fetchone()
                    if not u:
                        now_s = datetime.datetime.now().isoformat(timespec="seconds")
                        try:
                            conn.execute("""
                                INSERT OR IGNORE INTO users
                                (id, username, first_name, last_name, first_seen, last_seen)
                                VALUES (?,?,?,?,?,?)
                            """, (target_id, bm["username"], bm["first_name"],
                                  bm["last_name"], now_s, now_s))
                        except Exception:
                            pass
                        u = conn.execute("SELECT * FROM users WHERE id=?", (target_id,)).fetchone()

        if u is not None and not isinstance(u, dict):
            u = dict(u)
        result["user"] = u
        uid = (u["id"] if u else None) or target_id
        if not uid and not target_username:
            return result

        # ── Телефоны из ВСЕХ источников ───────────────────────────
        phones = set()
        if u and u["phone"]:
            phones.add(u["phone"])
        if uid:
            for row in conn.execute(
                "SELECT DISTINCT phone FROM chat_members WHERE user_id=? AND phone IS NOT NULL AND phone!=''", (uid,)
            ).fetchall():
                phones.add(row["phone"])
            for row in conn.execute(
                "SELECT DISTINCT phone FROM basa_messages WHERE user_id=? AND phone IS NOT NULL AND phone!=''", (uid,)
            ).fetchall():
                phones.add(row["phone"])
        result["phones"] = sorted(phones)

        # ── История имён и юзернеймов ──────────────────────────────
        names     = set()
        usernames = set()

        def _add_name(fn, ln):
            n = ((fn or "") + " " + (ln or "")).strip()
            if n:
                names.add(n)

        def _add_un(un):
            if un:
                usernames.add(un.lstrip("@"))

        if u:
            _add_name(u["first_name"], u["last_name"])
            _add_un(u["username"])
        if uid:
            for row in conn.execute(
                "SELECT DISTINCT first_name, last_name, username FROM chat_members WHERE user_id=?", (uid,)
            ).fetchall():
                _add_name(row["first_name"], row["last_name"])
                _add_un(row["username"])
            for row in conn.execute(
                "SELECT DISTINCT first_name, last_name, username FROM basa_messages WHERE user_id=?", (uid,)
            ).fetchall():
                _add_name(row["first_name"], row["last_name"])
                _add_un(row["username"])
        if target_username:
            _add_un(target_username)

        result["name_history"]     = sorted(names)
        result["username_history"] = sorted(usernames)

        if not uid:
            return result

        # ── Статистика сообщений ───────────────────────────────────
        result["msg_count"]  = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE user_id=?", (uid,)
        ).fetchone()[0]
        result["basa_count"] = conn.execute(
            "SELECT COUNT(*) FROM basa_messages WHERE user_id=?", (uid,)
        ).fetchone()[0]

        # ── Активность по чатам ────────────────────────────────────
        result["chats"] = conn.execute("""
            SELECT chat_id, chat_title, COUNT(*) as cnt, MAX(timestamp) as last
            FROM messages WHERE user_id=?
            GROUP BY chat_id, chat_title ORDER BY cnt DESC LIMIT 10
        """, (uid,)).fetchall()

        # ── Группы где найден (chat_members) ──────────────────────
        result["groups_in"] = conn.execute("""
            SELECT cm.chat_id, c.title, cm.is_admin, cm.is_banned
            FROM chat_members cm
            LEFT JOIN chats c ON c.id=cm.chat_id
            WHERE cm.user_id=? LIMIT 20
        """, (uid,)).fetchall()

        # ── Последние сообщения ────────────────────────────────────
        result["last_messages"] = conn.execute("""
            SELECT text, chat_title, timestamp, message_id, chat_id,
                   (SELECT username FROM chats WHERE id=messages.chat_id) as chat_un,
                   content_type
            FROM messages WHERE user_id=? AND text IS NOT NULL AND text!=''
            ORDER BY timestamp DESC LIMIT 5
        """, (uid,)).fetchall()

        # ── Оценка риска ───────────────────────────────────────────
        score = 0
        if result["basa_count"] > 100:      score += 1
        if len(result["groups_in"]) > 5:    score += 1
        if u and u["is_banned"]:            score += 3
        if len(result["name_history"]) > 2: score += 1
        result["risk_score"] = score

    # ── Поиск ВО ВСЕХ загружаемых базах ──────────────────────────
    try:
        # Собираем все термины для поиска
        search_terms: set = set()

        if uid:
            search_terms.add(str(uid))

        # Username-ы
        for un in result["username_history"]:
            if un and len(un) >= 3:
                search_terms.add(un.lstrip("@"))
        if target_username and len(target_username) >= 3:
            search_terms.add(target_username.lstrip("@"))

        # Телефоны — все нормализованные варианты
        for ph in result["phones"]:
            if ph:
                for variant in _udb_normalize_phone(ph):
                    search_terms.add(variant)

        # Имена — полное и по частям
        for nm in result["name_history"]:
            if nm and len(nm) >= 3:
                search_terms.add(nm)
                for part in nm.split():
                    if len(part) >= 3:
                        search_terms.add(part)

        dbs = _udb_list()
        hits_dedup: dict = {}   # (db_id, entry_id) -> hit

        for db in dbs:
            db_id_s = db["id"]
            db_name = db["name"]
            for term in search_terms:
                if not term or len(str(term)) < 2:
                    continue
                rows = _udb_search(db_id_s, str(term), "all", limit=100)
                for r in rows:
                    eid = r["id"]
                    key = (db_id_s, eid)
                    if key not in hits_dedup:
                        hits_dedup[key] = {
                            "db_name":     db_name,
                            "db_id":       db_id_s,
                            "entry":       r,
                            "matched_term": str(term),
                        }

        udb_hits = list(hits_dedup.values())
        udb_hits.sort(key=lambda x: (x["db_name"], x["matched_term"]))
        result["udb_hits"]         = udb_hits
        result["udb_searched_dbs"] = len(dbs)
        result["udb_search_terms"] = len(search_terms)
    except Exception:
        pass

    return result


def _send_probiv_card(chat_id_send, msg_id, result: dict, query_str: str = "?", edit: bool = False):
    """Красивая карточка пробива с копируемыми данными."""
    u     = result.get("user")
    uid_  = (u["id"] if u else None) or (int(query_str) if query_str.lstrip("-").isdigit() else None)

    empty = (
        not u
        and not result.get("groups_in")
        and result.get("msg_count", 0) == 0
        and result.get("basa_count", 0) == 0
        and not result.get("udb_hits")
    )

    if empty:
        text = (
            f"<code>╔══════════════════════════════╗\n"
            f"║   🕵️  П Р О Б И В             ║\n"
            f"╚══════════════════════════════╝</code>\n\n"
            f"❌ <b>Ничего не найдено по запросу:</b> <code>{escape_html(query_str)}</code>\n\n"
            f"<i>Попробуй другой формат: @username, числовой ID или номер телефона.</i>"
        )
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            types.InlineKeyboardButton("🔍 Новый пробив", callback_data="probiv_prompt"),
            types.InlineKeyboardButton("🏠 Меню",          callback_data="main_menu"),
        )
        if edit:
            safe_edit(text, chat_id_send, msg_id, reply_markup=kb)
        else:
            bot.send_message(chat_id_send, text, reply_markup=kb)
        return

    # ── Базовые данные ────────────────────────────────────────────
    if u:
        fn      = (u["first_name"] or "").strip()
        ln      = (u["last_name"]  or "").strip()
        name    = f"{fn} {ln}".strip() or "—"
        uid_str = str(u["id"])
        un_raw  = u["username"] or ""
        un_disp = f"@{un_raw}" if un_raw else "—"
    else:
        name    = query_str
        uid_str = str(uid_) if uid_ else "—"
        un_raw  = ""
        un_disp = "—"

    risk       = result.get("risk_score", 0)
    risk_emoji = {0: "🟢", 1: "🟡", 2: "🟠"}.get(min(risk, 2), "🔴")
    risk_label = {0: "НИЗКИЙ", 1: "СРЕДНИЙ", 2: "ВЫСОКИЙ"}.get(min(risk, 2), "КРИТИЧЕСКИЙ")
    total_msgs = result.get("msg_count", 0) + result.get("basa_count", 0)

    lines = [
        f"<code>╔══════════════════════════════╗",
        f"║   🕵️  П Р О Б И В             ║",
        f"╚══════════════════════════════╝</code>",
        f"",
    ]

    # ── Личность ──────────────────────────────────────────────────
    if uid_:
        name_link = f'<a href="tg://user?id={uid_}">{escape_html(name)}</a>'
    else:
        name_link = f"<b>{escape_html(name)}</b>"

    lines.append(f"👤 <b>Имя:</b> {name_link}")

    # Username — копируемый
    if un_raw:
        lines.append(f"🔗 <b>Username:</b> <code>@{escape_html(un_raw)}</code>")
    else:
        lines.append(f"🔗 <b>Username:</b> —")

    # TG ID — копируемый
    lines.append(f"🆔 <b>TG ID:</b> <code>{escape_html(uid_str)}</code>")

    if u:
        prem = " ⭐ Premium" if u["is_premium"] else ""
        ban  = "  🚫 <b>ЗАБАНЕН</b>" if u["is_banned"] else ""
        lang = u["language_code"] or "—"
        lines.append(f"🌐 <b>Язык:</b> {lang}{prem}{ban}")
        fs = (u["first_seen"] or "")[:16].replace("T", " ")
        ls = (u["last_seen"]  or "")[:16].replace("T", " ")
        if fs or ls:
            lines.append(f"📅 <b>Период:</b> {fs or "?"} → {ls or "?"}")

    lines.append("")
    lines.append(f"<code>──────────────────────────────</code>")

    # ── Телефоны — все копируемые ─────────────────────────────────
    phones = result.get("phones", [])
    if phones:
        lines.append(f"")
        lines.append(f"📱 <b>Телефоны ({len(phones)}):</b>")
        for ph in phones:
            lines.append(f"  • <code>{escape_html(ph)}</code>")
    else:
        lines.append(f"")
        lines.append(f"📱 <b>Телефон:</b> не найден")

    # ── История имён ──────────────────────────────────────────────
    names_h = result.get("name_history", [])
    nicks_h = result.get("username_history", [])
    if len(names_h) > 1:
        lines.append(f"📛 <b>История имён:</b> {'  ·  '.join(escape_html(n) for n in names_h[:6])}")
    if len(nicks_h) > 1:
        nicks_str = "  ·  ".join(f"<code>@{escape_html(n)}</code>" for n in nicks_h[:6])
        lines.append(f"🔗 <b>Все никнеймы:</b> {nicks_str}")

    # ── Статистика ────────────────────────────────────────────────
    lines.append(f"")
    lines.append(f"<code>──────────────────────────────</code>")
    lines.append(f"")
    lines.append(
        f"💬 <b>Сообщений:</b> <b>{total_msgs:,}</b>  "
        f"<i>(собственных: {result.get('msg_count',0):,}  из баз: {result.get('basa_count',0):,})</i>"
    )
    lines.append(f"{risk_emoji} <b>Риск:</b> {risk_label}")

    # ── Активность в чатах ────────────────────────────────────────
    chats = result.get("chats", [])
    if chats:
        lines.append(f"")
        lines.append(f"📝 <b>Активность ({len(chats)} чатов):</b>")
        for ch in chats[:6]:
            ct   = escape_html((ch["chat_title"] or str(ch["chat_id"]))[:30])
            last = (ch["last"] or "")[:10]
            lines.append(f"  ├ {ct}  — <b>{ch['cnt']:,}</b> сообщ.  [{last}]")

    # ── Группы-участник ───────────────────────────────────────────
    groups = result.get("groups_in", [])
    if groups:
        lines.append(f"")
        lines.append(f"👥 <b>Группы ({len(groups)}):</b>")
        for g in groups[:8]:
            role   = " 👑" if g["is_admin"]  else ""
            banned = " 🚫" if g["is_banned"] else ""
            gt = escape_html((g["title"] or str(g["chat_id"]))[:30])
            lines.append(f"  ├ {gt}{role}{banned}")

    # ── Последние сообщения ───────────────────────────────────────
    lm_list = []
    with get_conn() as conn:
        for row in conn.execute("""
            SELECT text, timestamp, chat_title,
                   COALESCE((SELECT username FROM chats WHERE id=messages.chat_id),'') as chat_un,
                   chat_id, message_id FROM messages
            WHERE user_id=? AND text IS NOT NULL AND text!='' AND text NOT LIKE '[%]'
            ORDER BY timestamp DESC LIMIT 10
        """, (uid_,)).fetchall():
            lm_list.append(dict(row))
        for row in conn.execute("""
            SELECT text, timestamp, chat_title,
                   COALESCE(chat_username,'') as chat_un,
                   chat_id, message_id FROM basa_messages
            WHERE user_id=? AND text IS NOT NULL AND text!='' AND text NOT LIKE '[%]'
            ORDER BY timestamp DESC LIMIT 10
        """, (uid_,)).fetchall():
            lm_list.append(dict(row))

    seen_lm = set()
    dedup_lm = []
    for m in lm_list:
        k = (m["text"][:40], m["chat_title"])
        if k not in seen_lm:
            seen_lm.add(k)
            dedup_lm.append(m)
    dedup_lm.sort(key=lambda x: x.get("timestamp") or "", reverse=True)

    if dedup_lm:
        lines.append(f"")
        lines.append(f"💬 <b>Последние сообщения:</b>")
        for m in dedup_lm[:4]:
            ts  = (m.get("timestamp") or "")[:16].replace("T", " ")
            txt = escape_html((m.get("text") or "")[:80])
            ct  = escape_html((m.get("chat_title") or "?")[:25])
            lines.append(f"  [{ts}] <i>{ct}</i>")
            lines.append(f"  <i>{txt}</i>")

    lines.append(f"")
    lines.append(f"<code>══════════════════════════════</code>")

    # ── Совпадения в загруженных базах ───────────────────────────
    udb_hits         = result.get("udb_hits", [])
    udb_searched_dbs = result.get("udb_searched_dbs", 0)
    udb_terms        = result.get("udb_search_terms", 0)

    if udb_hits:
        lines.append(f"")
        lines.append(
            f"🗄 <b>Найдено в базах данных: {len(udb_hits)} записей</b>"
            + (f"  <i>({udb_searched_dbs} баз · {udb_terms} терминов)</i>" if udb_searched_dbs else "")
        )
        # Группируем по базе
        by_db: dict = {}
        for hit in udb_hits:
            by_db.setdefault(hit["db_name"], []).append(hit["entry"])

        for dn, entries in by_db.items():
            lines.append(f"")
            lines.append(f"  📂 <b>{escape_html(dn)}</b>  <i>({len(entries)} записей)</i>")
            for e in entries[:10]:
                lines.append(_fmt_udb_entry(e))
            if len(entries) > 10:
                lines.append(f"  <i>  ...ещё {len(entries)-10} записей</i>")
    elif udb_searched_dbs > 0:
        lines.append(f"")
        lines.append(f"🗄 <i>Проверено {udb_searched_dbs} баз — совпадений не найдено</i>")
    else:
        lines.append(f"")
        lines.append(f"🗄 <i>Загруженных баз нет</i>")

    text = "\n".join(lines)

    # ── Кнопки ───────────────────────────────────────────────────
    kb = types.InlineKeyboardMarkup(row_width=2)
    if u:
        kb.row(
            types.InlineKeyboardButton("👤 Профиль",  callback_data=f"uprofile:{uid_}:info:0:0"),
            types.InlineKeyboardButton("🚫 Забанить", callback_data=f"ban_prompt:{uid_}"),
        )
    kb.row(
        types.InlineKeyboardButton("🔍 Новый пробив", callback_data="probiv_prompt"),
        types.InlineKeyboardButton("🏠 Меню",          callback_data="main_menu"),
    )
    if udb_hits:
        kb.add(types.InlineKeyboardButton("🗄 Все базы данных", callback_data="udb_list"))

    if edit:
        safe_edit(text, chat_id_send, msg_id, reply_markup=kb)
    else:
        bot.send_message(chat_id_send, text, reply_markup=kb)


def _send_partner_probiv_card(chat_id_send, msg_id, result: dict, query_str: str, pb_id: int):
    """Облегчённая карточка пробива для партнёров — без приватных данных админов."""
    u   = result["user"]
    uid_ = u["id"] if u else None

    empty = (not u and not result["groups_in"]
             and result["msg_count"] == 0 and result["basa_count"] == 0)
    if empty:
        text = (
            f"<code>🔍 ПРОБИВ: {query_str}</code>\n\n"
            f"❌ <b>Не найден.</b>\n"
            f"<i>Пользователь не в базе — данных нет.</i>"
        )
        bot.edit_message_text(text, chat_id_send, msg_id, parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("🔍 Ещё раз", callback_data=f"p_probiv_prompt:{pb_id}"),
                types.InlineKeyboardButton("◀️ Меню",    callback_data=f"p_menu:{pb_id}"),
            ))
        return

    lines = [f"<code>┌──────────────────────────┐</code>",
             f"<code>│  🔍  ПРОБИВ              │</code>",
             f"<code>└──────────────────────────┘</code>\n"]

    if u:
        fn   = (u["first_name"] or "").strip()
        ln   = (u["last_name"]  or "").strip()
        name = f"{fn} {ln}".strip() or "—"
        un_  = f"@{u['username']}" if u["username"] else "—"
        lines.append(f"👤 <b>{name}</b>  {un_}")
        lines.append(f"🆔 <code>{u['id']}</code>")
        if u["is_banned"]:
            lines.append(f"🚫 <b>Забанен</b>")
        fs = (u["first_seen"] or "")[:10]
        ls = (u["last_seen"]  or "")[:10]
        lines.append(f"📅 {fs} → {ls}")
    else:
        lines.append(f"👤 <b>{query_str}</b>")

    if result["phones"]:
        lines.append(f"\n📱 <b>Телефон:</b> {', '.join(f'<code>{p}</code>' for p in result['phones'])}")

    names_h = result.get("name_history", [])
    if len(names_h) > 1:
        lines.append(f"📛 Имена: {' · '.join(names_h[:4])}")

    lines.append(f"\n<code>──────────────────────────</code>")
    total = result["msg_count"] + result["basa_count"]
    lines.append(f"💬 Активность: <b>{total}</b> сообщений")

    if result["chats"]:
        lines.append(f"📝 Чатов: <b>{len(result['chats'])}</b>")
        top = result["chats"][0]
        lines.append(f"  Самый активный: {(top['chat_title'] or '?')[:25]} ({top['cnt']})")

    if result["groups_in"]:
        lines.append(f"👥 Найден в <b>{len(result['groups_in'])}</b> группах")
        for g in result["groups_in"][:4]:
            role = " 👑" if g["is_admin"] else ""
            lines.append(f"  ├ {(g['title'] or str(g['chat_id']))[:25]}{role}")

    lines.append(f"\n<code>──────────────────────────</code>")
    text = "\n".join(lines)

    kb = types.InlineKeyboardMarkup(row_width=2)
    if pb_id == -1:
        kb.row(
            types.InlineKeyboardButton("🔍 Ещё пробив", callback_data="u_probiv_prompt"),
            types.InlineKeyboardButton("◀️ Меню",        callback_data="u_menu"),
        )
    else:
        kb.row(
            types.InlineKeyboardButton("🔍 Ещё пробив", callback_data=f"p_probiv_prompt:{pb_id}"),
            types.InlineKeyboardButton("◀️ Меню",        callback_data=f"p_menu:{pb_id}"),
        )
    bot.edit_message_text(text, chat_id_send, msg_id, parse_mode="HTML", reply_markup=kb)


def log_message(user_id, chat_id, chat_title, chat_type, text, message_id,
                content_type="text", reply_to_msg_id=None, fwd_from_id=None):
    if get_setting("logging_enabled") != "1":
        return
    now = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO messages (user_id,chat_id,chat_title,chat_type,text,
                                  content_type,message_id,reply_to_msg_id,fwd_from_id,timestamp)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (user_id,chat_id,chat_title,chat_type,text,
              content_type,message_id,reply_to_msg_id,fwd_from_id,now))

def log_event(event_type, user_id=None, chat_id=None, description=""):
    if get_setting("event_log") != "1":
        return
    now = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO events (event_type,user_id,chat_id,description,timestamp)
            VALUES (?,?,?,?,?)
        """, (event_type,user_id,chat_id,description,now))

_banned_cache: set = set()
_banned_cache_loaded = False
_banned_cache_lock   = threading.Lock()

_broadcast_pending: dict = {}   # key -> {content_type, text, file_id, caption}

def _do_broadcast(bc_data: dict, admin_id: int, report_chat_id: int):
    """Выполняет рассылку параллельно через ThreadPoolExecutor."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading as _thr

    with get_conn() as conn:
        user_ids = [r[0] for r in conn.execute(
            "SELECT DISTINCT id FROM users WHERE id IS NOT NULL"
        ).fetchall()]
    total  = len(user_ids)
    now_s  = datetime.datetime.now().isoformat(timespec="seconds")
    ct     = bc_data.get("content_type", "text")
    text_  = bc_data.get("text", "")
    file_  = bc_data.get("file_id")
    cap_   = bc_data.get("caption", "")
    prev_  = (text_ or cap_ or "")[:80]

    with db_lock, get_conn() as conn:
        bc_log_id = conn.execute(
            "INSERT INTO broadcast_log (admin_id, preview, sent, failed, started_at) VALUES (?,?,0,0,?)",
            (admin_id, prev_, now_s)
        ).lastrowid

    try:
        status_msg = bot.send_message(report_chat_id,
            f"📢 <b>Рассылка запущена</b>\n👥 Всего: <b>{total:,}</b>\n⚡ Параллельная отправка...")
        status_mid = status_msg.message_id
    except Exception:
        status_mid = None

    sent   = 0
    failed = 0
    lock   = _thr.Lock()

    def _send_one(user_id):
        try:
            if ct == "text":
                bot.send_message(user_id, text_, parse_mode="HTML")
            elif ct == "photo":
                bot.send_photo(user_id, file_, caption=cap_, parse_mode="HTML")
            elif ct == "video":
                bot.send_video(user_id, file_, caption=cap_, parse_mode="HTML")
            elif ct == "document":
                bot.send_document(user_id, file_, caption=cap_, parse_mode="HTML")
            elif ct == "voice":
                bot.send_voice(user_id, file_, caption=cap_, parse_mode="HTML")
            elif ct == "audio":
                bot.send_audio(user_id, file_, caption=cap_, parse_mode="HTML")
            return True
        except Exception:
            return False

    # 30 потоков — максимально быстро без бана от Telegram
    WORKERS = 30
    completed = 0
    last_upd  = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_send_one, uid): uid for uid in user_ids}
        for future in as_completed(futures):
            ok = future.result()
            with lock:
                if ok:
                    sent += 1
                else:
                    failed += 1
                completed += 1
            # Обновляем статус каждые 200 отправок
            if status_mid and completed - last_upd >= 200:
                last_upd = completed
                try:
                    safe_edit(
                        f"📢 <b>Рассылка...</b>\n"
                        f"👥 {completed:,}/{total:,}  ✅ {sent:,}  ❌ {failed:,}",
                        report_chat_id, status_mid
                    )
                except Exception:
                    pass

    fin_s = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE broadcast_log SET sent=?, failed=?, finished_at=? WHERE id=?",
            (sent, failed, fin_s, bc_log_id)
        )
    try:
        safe_edit(
            f"📢 <b>Рассылка завершена!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Отправлено: <b>{sent:,}</b>\n"
            f"❌ Ошибок: <b>{failed:,}</b>\n"
            f"👥 Всего: <b>{total:,}</b>",
            report_chat_id, status_mid,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            )
        )
    except Exception:
        pass


# ─────────────────────────────────────────────
#  ЧАТ МЕЖДУ АДМИНАМИ
# ─────────────────────────────────────────────
# Состояние: None = чат выключен, True = в чате
_adminchat_active: dict = {}   # admin_id -> bool

def _adminchat_names() -> dict:
    """Возвращает {admin_id: имя} для всех админов."""
    names = {}
    for aid in ADMIN_IDS:
        try:
            u = get_user_by_id(aid)
            if u:
                fn = (u["first_name"] or "").strip()
                un = f"@{u['username']}" if u["username"] else ""
                names[aid] = f"{fn} {un}".strip() or str(aid)
            else:
                names[aid] = str(aid)
        except Exception:
            names[aid] = str(aid)
    return names

def _adminchat_send(sender_id: int, text: str = None, msg=None):
    """Переслать сообщение всем активным админам кроме отправителя."""
    names  = _adminchat_names()
    sender = names.get(sender_id, str(sender_id))
    kb_stop = types.InlineKeyboardMarkup(row_width=2)
    kb_stop.row(
        types.InlineKeyboardButton("👥 Статус", callback_data="adminchat_status"),
        types.InlineKeyboardButton("🔴 Выйти",  callback_data="adminchat_leave"),
    )
    for aid in ADMIN_IDS:
        if aid == sender_id:
            continue
        if not _adminchat_active.get(aid):
            continue
        try:
            header = f"<code>💬 {escape_html(sender)}:</code>\n"
            if msg and msg.content_type == "text":
                bot.send_message(aid, header + escape_html(text or ""), reply_markup=kb_stop)
            elif msg and msg.content_type == "photo":
                bot.send_photo(aid, msg.photo[-1].file_id,
                               caption=header + escape_html(msg.caption or ""),
                               reply_markup=kb_stop)
            elif msg and msg.content_type == "video":
                bot.send_video(aid, msg.video.file_id,
                               caption=header + escape_html(msg.caption or ""),
                               reply_markup=kb_stop)
            elif msg and msg.content_type == "voice":
                bot.send_voice(aid, msg.voice.file_id, reply_markup=kb_stop)
            elif msg and msg.content_type == "sticker":
                bot.send_sticker(aid, msg.sticker.file_id)
            elif msg and msg.content_type == "document":
                bot.send_document(aid, msg.document.file_id,
                                  caption=header + escape_html(msg.caption or ""),
                                  reply_markup=kb_stop)
            elif msg and msg.content_type == "audio":
                bot.send_audio(aid, msg.audio.file_id,
                               caption=header + escape_html(msg.caption or ""),
                               reply_markup=kb_stop)
            elif msg and msg.content_type == "video_note":
                bot.send_video_note(aid, msg.video_note.file_id)
            elif text:
                bot.send_message(aid, header + escape_html(text), reply_markup=kb_stop)
        except Exception:
            pass


def _load_banned_cache():
    global _banned_cache_loaded
    with get_conn() as conn:
        rows = conn.execute("SELECT user_id FROM banned_users").fetchall()
    with _banned_cache_lock:
        _banned_cache.update(r["user_id"] for r in rows)
        _banned_cache_loaded = True

def is_banned(user_id) -> bool:
    with _banned_cache_lock:
        if _banned_cache_loaded:
            return user_id in _banned_cache
    # Fallback to DB if cache not loaded
    with get_conn() as conn:
        return conn.execute("SELECT user_id FROM banned_users WHERE user_id=?", (user_id,)).fetchone() is not None

def ban_user(user_id, reason, banned_by):
    if user_id in ADMIN_IDS:
        return False
    now = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO banned_users (user_id,reason,banned_at,banned_by)
            VALUES (?,?,?,?)
        """, (user_id,reason,now,banned_by))
        conn.execute("UPDATE users SET is_banned=1, ban_reason=? WHERE id=?", (reason, user_id))
    with _banned_cache_lock:
        _banned_cache.add(user_id)
    u       = get_user_by_id(user_id)
    name    = (u["first_name"] or str(user_id)) if u else str(user_id)
    uname   = f"@{u['username']}" if u and u["username"] else "—"
    banner  = get_user_by_id(banned_by)
    b_name  = (banner["first_name"] or str(banned_by)) if banner else str(banned_by)
    text    = (
        f"🚫 <b>Пользователь забанен</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {name} | {uname} | <code>{user_id}</code>\n"
        f"📌 Причина: <b>{reason}</b>\n"
        f"👮 Забанил: {b_name} (<code>{banned_by}</code>)\n"
        f"📅 {now[:16]}\n━━━━━━━━━━━━━━━━━━━━"
    )
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("✅ Разбанить", callback_data=f"unban:{user_id}"))
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, text, reply_markup=kb)
        except Exception:
            pass
    return True

def unban_user(user_id, unbanned_by=None):
    with db_lock, get_conn() as conn:
        conn.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
        conn.execute("UPDATE users SET is_banned=0, ban_reason=NULL WHERE id=?", (user_id,))
    with _banned_cache_lock:
        _banned_cache.discard(user_id)
    u    = get_user_by_id(user_id)
    name = (u["first_name"] or str(user_id)) if u else str(user_id)
    uname= f"@{u['username']}" if u and u["username"] else "—"
    ub_part = ""
    if unbanned_by:
        ub = get_user_by_id(unbanned_by)
        ub_part = f"\n👮 Разбанил: {(ub['first_name'] or str(unbanned_by)) if ub else str(unbanned_by)}"
    text = f"✅ <b>Пользователь разбанен</b>\n👤 {name} | {uname} | <code>{user_id}</code>{ub_part}"
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, text)
        except Exception:
            pass

def make_msg_link_safe(chat_id, message_id, chat_username=None):
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}", True
    cid = str(chat_id)
    if cid.startswith("-100"):
        cid = cid[4:]
    elif cid.startswith("-"):
        cid = cid[1:]
    return f"https://t.me/c/{cid}/{message_id}", False

_settings_cache: dict = {}
_settings_cache_lock = threading.Lock()

def get_setting(key: str) -> str:
    with _settings_cache_lock:
        if key in _settings_cache:
            return _settings_cache[key]
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        val = row["value"] if row else "0"
    with _settings_cache_lock:
        _settings_cache[key] = val
    return val

def set_setting(key: str, value: str):
    with _settings_cache_lock:
        _settings_cache[key] = value
    with db_lock, get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key,value))

def get_all_users():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users ORDER BY last_seen DESC").fetchall()

def get_user_by_id(uid):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()

def _normalize_phones_for_search(q: str) -> list:
    """Возвращает LIKE-паттерны для поиска номера во всех форматах."""
    variants = _udb_normalize_phone(q)
    return [f"%{v}%" for v in variants]


def search_users(query: str):
    """
    Быстрый универсальный поиск.
    Находит по: TG ID, @username, имени, фамилии, имени+фамилии, телефону.
    Поддерживает кириллицу, нормализацию телефонов.
    """
    q = query.strip().lstrip("@")
    if not q:
        return []

    ql    = q.lower()
    like  = f"%{q}%"
    likel = f"%{ql}%"

    # Определяем тип запроса
    is_id    = q.lstrip("-").isdigit()
    is_phone = bool(_re.match(r"^[\+\d][\d\s\-\(\)]{6,}$", q))

    with get_conn() as conn:
        # ── 1. Поиск в таблице users ──────────────────────────────
        if is_id:
            results = conn.execute(
                "SELECT * FROM users WHERE id=? LIMIT 1", (int(q),)
            ).fetchall()
        elif is_phone:
            ph_patterns = _normalize_phones_for_search(q)
            cond = " OR ".join(["phone LIKE ?" for _ in ph_patterns])
            results = conn.execute(
                f"SELECT * FROM users WHERE {cond} LIMIT 50", ph_patterns
            ).fetchall()
        else:
            results = conn.execute("""
                SELECT DISTINCT u.*
                FROM users u
                WHERE lower(u.username)=?
                   OR lower(u.username) LIKE ?
                   OR u.first_name LIKE ? OR u.first_name LIKE ?
                   OR u.last_name  LIKE ? OR u.last_name  LIKE ?
                   OR (u.first_name || ' ' || COALESCE(u.last_name,'')) LIKE ?
                   OR (u.first_name || ' ' || COALESCE(u.last_name,'')) LIKE ?
                ORDER BY u.last_seen DESC
                LIMIT 100
            """, (ql, likel, like, likel, like, likel, like, likel)).fetchall()

        # ── 2. Ищем дополнительные ID в chat_members ─────────────
        extra_ids = set(r["id"] for r in results)

        if is_id:
            cm_ids = set()
            bm_ids = {int(q)} if int(q) not in extra_ids else set()
        elif is_phone:
            ph_patterns = _normalize_phones_for_search(q)
            cond = " OR ".join(["phone LIKE ?" for _ in ph_patterns])
            cm_rows = conn.execute(
                f"SELECT DISTINCT user_id FROM chat_members WHERE {cond} AND user_id IS NOT NULL LIMIT 50",
                ph_patterns
            ).fetchall()
            cm_ids = {r["user_id"] for r in cm_rows} - extra_ids
            bm_ids = set()
        else:
            cm_rows = conn.execute("""
                SELECT DISTINCT user_id FROM chat_members
                WHERE user_id IS NOT NULL
                  AND (lower(username)=? OR lower(username) LIKE ?
                       OR first_name LIKE ? OR first_name LIKE ?
                       OR last_name  LIKE ? OR last_name  LIKE ?
                       OR (first_name || ' ' || COALESCE(last_name,'')) LIKE ?)
                LIMIT 50
            """, (ql, likel, like, likel, like, likel, like)).fetchall()

            bm_rows = conn.execute("""
                SELECT DISTINCT user_id FROM basa_messages
                WHERE user_id IS NOT NULL AND user_id != 0
                  AND (lower(username) LIKE ?
                       OR first_name LIKE ? OR first_name LIKE ?
                       OR last_name  LIKE ? OR last_name  LIKE ?
                       OR (first_name || ' ' || COALESCE(last_name,'')) LIKE ?)
                LIMIT 50
            """, (likel, like, likel, like, likel, like)).fetchall()

            cm_ids = {r["user_id"] for r in cm_rows} - extra_ids
            bm_ids = {r["user_id"] for r in bm_rows} - extra_ids - cm_ids

        # ── 3. Догружаем найденные по chat_members/basa_messages ──
        all_extra = list(cm_ids | bm_ids)
        if all_extra:
            placeholders = ",".join("?" * len(all_extra))
            extra_rows = conn.execute(
                f"SELECT * FROM users WHERE id IN ({placeholders}) LIMIT 100",
                all_extra
            ).fetchall()
            results = list(results) + list(extra_rows)
            extra_ids.update(r["id"] for r in extra_rows)

        # ── 4. Если совсем ничего — синтезируем из chat_members/basa ──
        if not results:
            synth_ids = list(cm_ids | bm_ids)
            if not synth_ids and is_id:
                synth_ids = [int(q)]
            synth = []
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            for uid2 in synth_ids[:20]:
                cm = conn.execute(
                    "SELECT * FROM chat_members WHERE user_id=? LIMIT 1", (uid2,)
                ).fetchone()
                bm = conn.execute(
                    "SELECT * FROM basa_messages WHERE user_id=? LIMIT 1", (uid2,)
                ).fetchone()
                if not cm and not bm:
                    continue
                fn  = (cm["first_name"] if cm else None) or (bm["first_name"] if bm else "") or ""
                ln  = (cm["last_name"]  if cm else None) or (bm["last_name"]  if bm else "") or ""
                un  = (cm["username"]   if cm else None) or (bm["username"]   if bm else None)
                ph  = (cm["phone"]      if cm else None)
                mc  = conn.execute(
                    "SELECT COUNT(*) as c FROM basa_messages WHERE user_id=?", (uid2,)
                ).fetchone()["c"]
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO users
                        (id, username, first_name, last_name, phone, first_seen, last_seen)
                        VALUES (?,?,?,?,?,?,?)
                    """, (uid2, un, fn, ln, ph, now_s, now_s))
                except Exception:
                    pass
                synth.append({
                    "id": uid2, "username": un, "first_name": fn, "last_name": ln,
                    "phone": ph, "language_code": None, "is_premium": 0, "is_banned": 0,
                    "first_seen": now_s, "last_seen": now_s, "msg_count": mc,
                })
            return synth

    # Убираем дубли по id
    seen = set()
    out  = []
    for r in results:
        rid = r["id"]
        if rid not in seen:
            seen.add(rid)
            out.append(r)
    # Сортируем: по количеству сообщений убывает
    out.sort(key=lambda r: (-(r["msg_count"] if "msg_count" in r.keys() else 0), r["id"] or 0)
             if hasattr(r, "keys") else 0)
    return out


def get_user_message_count_split(uid):
    with get_conn() as conn:
        main = conn.execute("SELECT COUNT(*) as c FROM messages WHERE user_id=?", (uid,)).fetchone()["c"]
        basa = conn.execute("SELECT COUNT(*) as c FROM basa_messages WHERE user_id=?", (uid,)).fetchone()["c"]
        return main, basa

def get_user_message_count(uid):
    m, b = get_user_message_count_split(uid)
    return m + b

def get_user_last_messages(uid, limit=5):
    with get_conn() as conn:
        main_m = conn.execute("""
            SELECT user_id, chat_id, chat_title, chat_type,
                   text, content_type, message_id, timestamp, 'main' AS source
            FROM messages WHERE user_id=? ORDER BY timestamp DESC LIMIT ?
        """, (uid, limit)).fetchall()
        basa_m = conn.execute("""
            SELECT user_id, chat_id, chat_title, chat_username AS chat_type,
                   text, content_type, message_id, timestamp, 'basa' AS source
            FROM basa_messages WHERE user_id=? ORDER BY timestamp DESC LIMIT ?
        """, (uid, limit)).fetchall()
    combined = list(main_m) + list(basa_m)
    combined.sort(key=lambda r: r["timestamp"] or "", reverse=True)
    return combined[:limit]

def get_user_last_messages_basa(uid, limit=10):
    with get_conn() as conn:
        return conn.execute("""
            SELECT bm.*, bs.chat_title AS session_chat_title
            FROM basa_messages bm
            LEFT JOIN basa_sessions bs ON bs.session_id=bm.session_id
            WHERE bm.user_id=? ORDER BY bm.timestamp DESC LIMIT ?
        """, (uid, limit)).fetchall()

def get_user_chats(uid):
    with get_conn() as conn:
        main_c = conn.execute("""
            SELECT DISTINCT m.chat_id, m.chat_title, m.chat_type,
                            c.username as chat_username, 'main' as source
            FROM messages m LEFT JOIN chats c ON c.id=m.chat_id WHERE m.user_id=?
        """, (uid,)).fetchall()
        basa_c = conn.execute("""
            SELECT DISTINCT chat_id, chat_title, chat_type,
                            chat_username, 'basa' as source
            FROM basa_messages WHERE user_id=?
        """, (uid,)).fetchall()
    seen = {r["chat_id"] for r in main_c}
    merged = list(main_c)
    for c in basa_c:
        if c["chat_id"] not in seen:
            merged.append(c)
            seen.add(c["chat_id"])
    return merged

def get_stats():
    with get_conn() as conn:
        r = conn.execute("""SELECT
            (SELECT COUNT(*) FROM users)                          AS users,
            (SELECT COUNT(*) FROM users WHERE phone IS NOT NULL)  AS phones,
            (SELECT COUNT(*) FROM messages)                       AS msgs,
            (SELECT COUNT(*) FROM basa_messages)                  AS basa_msgs,
            (SELECT COUNT(*) FROM chat_members)                   AS members,
            (SELECT COUNT(*) FROM chats)                          AS chats,
            (SELECT COUNT(*) FROM basa_sessions)                  AS basa_sess,
            (SELECT COUNT(*) FROM banned_users)                   AS banned,
            (SELECT COUNT(*) FROM partner_bots WHERE is_active=1) AS pbots,
            (SELECT COUNT(*) FROM users WHERE DATE(first_seen)=CURRENT_DATE) AS today_users,
            (SELECT COUNT(*) FROM messages WHERE DATE(timestamp)=CURRENT_DATE) AS today_msgs
        """).fetchone()
        top_users = conn.execute("""
            SELECT u.first_name, u.username, COUNT(m.id) as cnt
            FROM messages m JOIN users u ON u.id=m.user_id
            GROUP BY m.user_id, u.first_name, u.username ORDER BY cnt DESC LIMIT 5
        """).fetchall()
    stats = dict(r)
    stats["top_users"] = [dict(row) for row in top_users]
    stats["live"] = len(_active_basa_sessions)
    return stats

# ─────────────────────────────────────────────
#  ТЕЛЕФОНЫ — РЕГУЛЯРКА + ПОИСК
# ─────────────────────────────────────────────
import re as _GLOBAL_RE_MOD

_PHONE_RE_INTL = _GLOBAL_RE_MOD.compile(
    r"(?<![\d+])"
    r"("
      r"\+(?:[17]\d{6,14}|[2-9]\d{6,13})"
      r"|(?:[78]|0)\d{9,10}"
    r")"
    r"(?![\d])"
)

def _clean_phone_digits(ph: str) -> str:
    return _GLOBAL_RE_MOD.sub(r"[^\d+]", "", ph)

def _is_valid_phone(ph: str) -> bool:
    digits = _clean_phone_digits(ph).lstrip("+")
    return 7 <= len(digits) <= 15

def get_user_phones(uid):
    phones = []
    with get_conn() as conn:
        u = conn.execute("SELECT phone FROM users WHERE id=?", (uid,)).fetchone()
        if u and u["phone"] and _is_valid_phone(u["phone"]):
            phones.append({"phone": u["phone"], "source": "account",
                           "chat_id":None,"chat_title":None,"chat_username":None,
                           "message_id":None,"timestamp":None,"text":None})

        for r in conn.execute("""
            SELECT m.text, m.chat_id, m.chat_title, m.message_id, m.timestamp, c.username as chat_username
            FROM messages m LEFT JOIN chats c ON c.id=m.chat_id
            WHERE m.user_id=? AND m.text IS NOT NULL
        """, (uid,)).fetchall():
            for ph in _PHONE_RE_INTL.findall(r["text"] or ""):
                ph = ph.strip()
                if _is_valid_phone(ph):
                    phones.append({"phone":ph,"source":"message",
                                   "chat_id":r["chat_id"],"chat_title":r["chat_title"],
                                   "chat_username":r["chat_username"],
                                   "message_id":r["message_id"],"timestamp":r["timestamp"],
                                   "text":r["text"]})

        for r in conn.execute("""
            SELECT text, chat_id, chat_title, chat_username, message_id, timestamp
            FROM basa_messages WHERE user_id=? AND text IS NOT NULL
        """, (uid,)).fetchall():
            for ph in _PHONE_RE_INTL.findall(r["text"] or ""):
                ph = ph.strip()
                if _is_valid_phone(ph):
                    phones.append({"phone":ph,"source":"basa",
                                   "chat_id":r["chat_id"],"chat_title":r["chat_title"],
                                   "chat_username":r["chat_username"],
                                   "message_id":r["message_id"],"timestamp":r["timestamp"],
                                   "text":r["text"]})

    seen = set()
    deduped = []
    for p in phones:
        key = _clean_phone_digits(p["phone"])
        if key not in seen:
            seen.add(key)
            deduped.append(p)
    return deduped


# ─────────────────────────────────────────────
#  ФОРМАТИРОВАНИЕ ПРОФИЛЯ
# ─────────────────────────────────────────────
_LANG_FLAGS = {
    "ru":"🇷🇺","uk":"🇺🇦","en":"🇬🇧","de":"🇩🇪",
    "fr":"🇫🇷","es":"🇪🇸","tr":"🇹🇷","uz":"🇺🇿",
    "kk":"🇰🇿","az":"🇦🇿","hy":"🇦🇲","ka":"🇬🇪",
}

_CT_ICONS = {
    "text":"✉️","photo":"🖼","video":"🎬","document":"📄",
    "audio":"🎵","voice":"🎤","sticker":"🎭","video_note":"📹",
}

def ct_icon(ct): return _CT_ICONS.get(ct or "text", "📎")

def user_link(row) -> str:
    uid  = row["id"]
    name = ((row["first_name"] or "") + (" " + row["last_name"] if row["last_name"] else "")).strip() or str(uid)
    if row["username"]:
        return f'<a href="https://t.me/{row["username"]}">{name}</a>'
    return f'<a href="tg://user?id={uid}">{name}</a>'

def _user_header(uid, u=None) -> str:
    if not u:
        u = get_user_by_id(uid)
    if not u:
        return f"👤 <b>ID {uid}</b>\n"
    fname    = u["first_name"] or ""
    lname    = u["last_name"]  or ""
    fullname = f"{fname} {lname}".strip() or "?"
    uname_raw= u["username"] if "username" in u.keys() else None
    tg_link  = f'<a href="https://t.me/{uname_raw}">@{uname_raw}</a>' if uname_raw \
               else f'<a href="tg://user?id={uid}">профиль</a>'
    premium  = " ⭐" if (u["is_premium"] if "is_premium" in u.keys() else 0) else ""
    banned   = "🚫 " if (u["is_banned"]  if "is_banned"  in u.keys() else 0) else ""
    lang     = u["language_code"] if "language_code" in u.keys() else None
    flag     = _LANG_FLAGS.get(lang or "", "") + " " if lang else ""
    return (
        f"{banned}👤 <b>{fullname}</b>{premium}\n"
        f"🆔 <code>{uid}</code>  {tg_link}  {flag}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
    )

def kb_profile_tabs(uid: int, active: str = "info", list_index: int = 0) -> types.InlineKeyboardMarkup:
    tabs = [("ℹ️","info"),("📱","phones"),("💬","chats"),("📩","msgs"),("📊","activity")]
    kb   = types.InlineKeyboardMarkup(row_width=5)
    btns = []
    for label, tab in tabs:
        prefix = "›" if tab == active else ""
        btns.append(types.InlineKeyboardButton(
            f"{prefix}{label}",
            callback_data=f"uprofile:{uid}:{tab}:0:{list_index}"
        ))
    kb.add(*btns)
    u      = get_user_by_id(uid)
    banned = (u["is_banned"] if u and "is_banned" in u.keys() else 0) or 0
    ban_btn= types.InlineKeyboardButton("✅ Разбанить", callback_data=f"unban:{uid}") if banned \
             else types.InlineKeyboardButton("🚫 Забанить", callback_data=f"ban_prompt:{uid}")
    kb.row(ban_btn, types.InlineKeyboardButton("📝 Заметка", callback_data=f"note_prompt:{uid}"))
    kb.add(types.InlineKeyboardButton("◀️ К списку", callback_data=f"users_list:{list_index}"))
    kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    return kb


# ─────────────────────────────────────────────
#  КЛАВИАТУРЫ
# ─────────────────────────────────────────────
def kb_main_menu():
    with get_conn() as conn:
        r = conn.execute("""SELECT
            (SELECT COUNT(*) FROM users)                         AS uc,
            (SELECT COUNT(*) FROM users WHERE phone IS NOT NULL) AS pc,
            (SELECT COUNT(*) FROM chats)                         AS cc,
            (SELECT COUNT(*) FROM basa_messages)                 AS bc,
            (SELECT COUNT(*) FROM basa_sessions WHERE is_live=1) AS lc
        """).fetchone()
    ub_ico = "🟢" if userbot_is_connected() else "🔴"
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🕵️ Пробив",        callback_data="probiv_prompt"),
        types.InlineKeyboardButton("🔍 Поиск по базе", callback_data="search_prompt"),
    )
    kb.row(
        types.InlineKeyboardButton(f"👥 Люди [{r['uc']:,}]",   callback_data="users_list:0"),
        types.InlineKeyboardButton(f"📱 Номера [{r['pc']:,}]", callback_data="phones_list:0:all"),
    )
    kb.row(
        types.InlineKeyboardButton(f"🗂 Чаты [{r['cc']:,}]",  callback_data="chats_list"),
        types.InlineKeyboardButton("📊 Статистика",             callback_data="stats"),
    )
    kb.row(
        types.InlineKeyboardButton(f"📥 Basa [{r['bc']:,}]",       callback_data="basa_sessions"),
        types.InlineKeyboardButton(f"🔴 Live [{r['lc']} акт.]",     callback_data="live_panel"),
    )
    kb.add(types.InlineKeyboardButton(
        "🔄 Выжать ВСЕ чаты (история + Live)", callback_data="mass_scrape_all"))
    kb.row(
        types.InlineKeyboardButton("🧹 Слить базы",  callback_data="merge_db_confirm"),
        types.InlineKeyboardButton("🚫 Баны",         callback_data="banned_list"),
    )
    kb.row(
        types.InlineKeyboardButton("🔔 Уведомления", callback_data="notif_chats_list"),
        types.InlineKeyboardButton("🔌 Партнёры",     callback_data="partners_panel"),
    )
    kb.row(
        types.InlineKeyboardButton("⚙️ Настройки",  callback_data="settings"),
        types.InlineKeyboardButton(f"{ub_ico} Аккаунт",  callback_data="account_info"),
    )
    kb.row(
        types.InlineKeyboardButton("📢 Рассылка",      callback_data="broadcast_menu"),
        types.InlineKeyboardButton("💬 Техподдержка",  callback_data="support_list:0"),
    )
    kb.row(
        types.InlineKeyboardButton("👮 Сотрудники",    callback_data="staff_panel"),
        types.InlineKeyboardButton("👥 Чат с админами", callback_data="adminchat_open"),
    )
    with get_conn() as _mc:
        _udb_c = _mc.execute("SELECT COUNT(*) FROM uploaded_databases").fetchone()[0]
    kb.add(types.InlineKeyboardButton(f"🗄 Базы данных [{_udb_c}]", callback_data="udb_list"))
    return kb
def kb_back_menu():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    return kb

def kb_users_nav(index, total, user_id):
    kb = types.InlineKeyboardMarkup(row_width=3)
    prev_b = types.InlineKeyboardButton("◀️", callback_data=f"users_list:{max(0,index-1)}") \
             if index > 0 else types.InlineKeyboardButton("·", callback_data="noop")
    next_b = types.InlineKeyboardButton("▶️", callback_data=f"users_list:{min(total-1,index+1)}") \
             if index < total-1 else types.InlineKeyboardButton("·", callback_data="noop")
    kb.add(prev_b, types.InlineKeyboardButton(f"🏠 {index+1}/{total}", callback_data="main_menu"), next_b)
    # Быстрые прыжки
    jump_row = []
    if index >= 10:
        jump_row.append(types.InlineKeyboardButton("⏮ -10", callback_data=f"users_list:{max(0,index-10)}"))
    if index < total - 10:
        jump_row.append(types.InlineKeyboardButton("⏭ +10", callback_data=f"users_list:{min(total-1,index+10)}"))
    if index > 0:
        jump_row.append(types.InlineKeyboardButton("⏮ Начало", callback_data="users_list:0"))
    if jump_row:
        kb.row(*jump_row)
    kb.add(types.InlineKeyboardButton("🔎 Детали", callback_data=f"uprofile:{user_id}:info:0:0"))
    return kb

def kb_settings():
    anon = get_setting("anonymous_mode")
    log  = get_setting("logging_enabled")
    fwd  = get_setting("forward_to_admin")
    ban  = get_setting("ban_block")
    kb   = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(f"{'✅' if anon=='1' else '❌'} Анонимный режим",       callback_data="toggle:anonymous_mode"),
        types.InlineKeyboardButton(f"{'✅' if log=='1'  else '❌'} Логирование сообщений", callback_data="toggle:logging_enabled"),
        types.InlineKeyboardButton(f"{'✅' if fwd=='1'  else '❌'} Пересылка сообщений",   callback_data="toggle:forward_to_admin"),
        types.InlineKeyboardButton(f"{'✅' if ban=='1'  else '❌'} Блокировать банов",      callback_data="toggle:ban_block"),
        types.InlineKeyboardButton("🏠 Главное меню",                                      callback_data="main_menu"),
    )
    return kb

def _kb_basa_view(session_id, chat_id=0):
    kb      = types.InlineKeyboardMarkup(row_width=1)
    is_live = chat_id and chat_id in _active_basa_sessions
    live_btn= "⏹ Остановить Live" if is_live else "🔴 Запустить Live"
    live_cb = f"basa_stop:{session_id}:{chat_id}" if is_live else f"basa_start:{session_id}:{chat_id}"

    # Проверяем статус копирования — показываем кнопку Продолжить если прервано
    copy_status = "idle"
    last_mid    = 0
    total_msgs  = 0
    if chat_id:
        with get_conn() as conn:
            sr = conn.execute(
                "SELECT copy_status, last_message_id, total_messages FROM basa_sessions WHERE session_id=?",
                (session_id,)
            ).fetchone()
        if sr:
            copy_status = sr["copy_status"] or "idle"
            last_mid    = sr["last_message_id"] or 0
            total_msgs  = sr["total_messages"] or 0

    # Показываем кнопку если: статус 'copying' ИЛИ есть сообщения но статус не 'done'
    show_resume = (copy_status == "copying") or (copy_status not in ("done",) and total_msgs > 0)
    if show_resume:
        lbl = f"▶️ Продолжить копирование (с #{last_mid})" if last_mid else f"▶️ Продолжить копирование ({total_msgs:,} уже есть)"
        kb.add(types.InlineKeyboardButton(lbl, callback_data=f"basa_resume:{session_id}:{chat_id}"))

    kb.add(
        types.InlineKeyboardButton("📋 Просмотр сообщений", callback_data=f"basa_view:{session_id}:0"),
        types.InlineKeyboardButton(live_btn,                 callback_data=live_cb),
        types.InlineKeyboardButton("🗑 Удалить сессию",      callback_data=f"basa_del_confirm:{session_id}:{chat_id}"),
    )
    kb.row(
        types.InlineKeyboardButton("🔴 Live панель", callback_data="live_panel"),
        types.InlineKeyboardButton("📊 Все сессии",  callback_data="basa_sessions"),
    )
    kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    return kb


# ─────────────────────────────────────────────
#  ФОРМАТИРОВАНИЕ КАРТОЧКИ ПОЛЬЗОВАТЕЛЯ
# ─────────────────────────────────────────────
def fmt_user_card_short(user_row, index=None, total=None) -> str:
    uid       = user_row["id"]
    uname_raw = user_row["username"] or ""
    fname     = user_row["first_name"] or ""
    lname     = user_row["last_name"]  or ""
    fullname  = f"{fname} {lname}".strip() or "—"
    phone_raw = user_row["phone"] or ""
    keys      = user_row.keys() if hasattr(user_row, "keys") else {}
    premium   = " ⭐" if (user_row["is_premium"] if "is_premium" in keys else 0) else ""
    banned    = (user_row["is_banned"] if "is_banned" in keys else 0) or 0
    ban_line  = "  🚫 <b>ЗАБАНЕН</b>" if banned else ""

    mc_main, mc_basa = get_user_message_count_split(uid)
    mc_total = mc_main + mc_basa

    # Ссылки и копируемые данные
    if uname_raw:
        tg_link = f'<a href="https://t.me/{uname_raw}">@{uname_raw}</a>'
    else:
        tg_link = f'<a href="tg://user?id={uid}">открыть профиль</a>'

    phone_line = f"📱 <code>{escape_html(phone_raw)}</code>" if phone_raw else "📱 номер скрыт"
    un_line    = f"🔗 <code>@{escape_html(uname_raw)}</code>  {tg_link}" if uname_raw else f"🔗 username отсутствует"
    counter    = f"<b>[{index}/{total}]</b>  " if index and total else ""

    return (
        f"{counter}👤 <b>{escape_html(fullname)}</b>{premium}{ban_line}\n"
        f"🆔 <code>{uid}</code>\n"
        f"{un_line}\n"
        f"{phone_line}  💬 сообщ: <b>{mc_total:,}</b>  <i>(бот:{mc_main} basa:{mc_basa})</i>\n"
        f"📅 {fmt_date(user_row['first_seen'] or '')} → {fmt_date(user_row['last_seen'] or '')}"
    )


# ─────────────────────────────────────────────
#  СОСТОЯНИЯ
# ─────────────────────────────────────────────
user_states: dict[int, str] = {}


# ═══════════════════════════════════════════════════════════════
#  USERBOT ENGINE
# ═══════════════════════════════════════════════════════════════
_userbot_client  = None
_userbot_loop    = None
_userbot_thread  = None
_userbot_info    = {}

_auth_loop: asyncio.AbstractEventLoop = None
_auth_loop_thread: threading.Thread   = None
_account_auth: dict = {}

def _ensure_auth_loop():
    global _auth_loop, _auth_loop_thread
    if _auth_loop is not None and _auth_loop.is_running():
        return _auth_loop
    _auth_loop = asyncio.new_event_loop()
    def _run():
        asyncio.set_event_loop(_auth_loop)
        _auth_loop.run_forever()
    _auth_loop_thread = threading.Thread(target=_run, daemon=True)
    _auth_loop_thread.start()
    import time; time.sleep(0.3)
    return _auth_loop

def _run_in_auth_loop(coro):
    loop   = _ensure_auth_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=60)

def _get_session_path(phone: str) -> str:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    phone_clean = phone.replace("+","").replace(" ","")
    return os.path.join(SESSIONS_DIR, f"user_{phone_clean}")

def userbot_is_connected() -> bool:
    return _userbot_client is not None and _userbot_client.is_connected()

def userbot_run_sync(coro):
    if _userbot_loop is None:
        raise RuntimeError("Userbot не запущен")
    return asyncio.run_coroutine_threadsafe(coro, _userbot_loop).result(timeout=60)

_active_basa_sessions: dict[int, str] = {}

def _live_set(chat_id: int, session_id: str = None):
    """Запустить live — сохранить в памяти и в БД. Автосоздаёт сессию если нет."""
    now = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        # Если session_id не передан — берём существующую или создаём новую
        if not session_id:
            row = conn.execute(
                "SELECT session_id FROM basa_sessions WHERE chat_id=? ORDER BY created_at DESC LIMIT 1",
                (chat_id,)
            ).fetchone()
            if row:
                session_id = row["session_id"]
            else:
                # Создаём сессию автоматически
                chat_row = conn.execute("SELECT * FROM chats WHERE id=?", (chat_id,)).fetchone()
                title = (chat_row["title"] if chat_row else None) or f"Чат {chat_id}"
                uname = (chat_row["username"] if chat_row else None) or ""
                session_id = f"{chat_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                conn.execute("""
                    INSERT OR IGNORE INTO basa_sessions
                    (session_id, chat_id, chat_title, chat_username, is_live, started_at, created_at)
                    VALUES (?,?,?,?,1,?,?)
                """, (session_id, chat_id, title, uname, now, now))
        conn.execute("UPDATE basa_sessions SET is_live=0 WHERE chat_id=?", (chat_id,))
        conn.execute("UPDATE basa_sessions SET is_live=1 WHERE session_id=?", (session_id,))
    _active_basa_sessions[chat_id] = session_id
    return session_id

def _live_unset(chat_id: int):
    """Остановить live — убрать из памяти и из БД."""
    _active_basa_sessions.pop(chat_id, None)
    with db_lock, get_conn() as conn:
        conn.execute("UPDATE basa_sessions SET is_live=0 WHERE chat_id=?", (chat_id,))

def _live_restore_from_db():
    """При старте бота — восстанавливаем активные live-сессии из БД."""
    try:
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT session_id, chat_id FROM basa_sessions WHERE is_live=1"
            ).fetchall()
        for r in rows:
            _active_basa_sessions[r["chat_id"]] = r["session_id"]
        if rows:
            print(f"Live sessions restored: {len(rows)}")
    except Exception as e:
        print(f"Warning: could not restore live sessions: {e}")

async def _async_start_userbot(api_id, api_hash, session_path):
    global _userbot_client, _userbot_info
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        return client, False
    me = await client.get_me()
    _userbot_client = client
    _userbot_info   = {
        "phone":    getattr(me,"phone","?"),
        "name":     f"{me.first_name or ''} {me.last_name or ''}".strip(),
        "id":       me.id,
        "username": getattr(me,"username",None),
    }
    @client.on(events.NewMessage)
    async def _live_handler(event):
        try:
            m          = event.message
            cid        = event.chat_id
            session_id = _active_basa_sessions.get(cid)
            if not session_id:
                return
            sender = None
            try:
                sender = await event.get_sender()
            except Exception:
                pass
            now    = datetime.datetime.now().isoformat(timespec="seconds")
            chat   = await event.get_chat()
            text   = m.text or m.message or "[медиа]"

            uid        = getattr(sender, "id", 0) or 0
            username   = getattr(sender, "username", None)
            first_name = getattr(sender, "first_name", None)
            last_name  = getattr(sender, "last_name", None)
            phone      = getattr(sender, "phone", None)
            is_bot     = getattr(sender, "bot", False)
            chat_title = getattr(chat, "title", None) or getattr(chat, "first_name", "?")
            chat_uname = getattr(chat, "username", None)

            with db_lock, get_conn() as conn:
                # 1. Сохраняем/обновляем пользователя
                if uid:
                    conn.execute("""
                        INSERT INTO users (id, username, first_name, last_name, phone, is_bot, last_seen)
                        VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT(id) DO UPDATE SET
                            username=COALESCE(excluded.username, users.username),
                            first_name=COALESCE(excluded.first_name, users.first_name),
                            last_name=COALESCE(excluded.last_name, users.last_name),
                            phone=COALESCE(excluded.phone, users.phone),
                            last_seen=excluded.last_seen
                    """, (uid, username, first_name, last_name, phone, 1 if is_bot else 0, now))

                    # 2. Обновляем chat_members
                    conn.execute("""
                        INSERT INTO chat_members (chat_id, user_id, joined_at, phone)
                        VALUES (?,?,?,?)
                        ON CONFLICT(chat_id, user_id) DO UPDATE SET
                            phone=COALESCE(excluded.phone, chat_members.phone)
                    """, (cid, uid, now, phone))

                # 3. Сохраняем в messages (основной лог бота)
                conn.execute("""
                    INSERT OR IGNORE INTO messages
                    (chat_id, chat_title, chat_type, user_id, message_id, text, content_type, timestamp)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (
                    cid, chat_title,
                    "supergroup" if hasattr(chat, "megagroup") else "group",
                    uid, m.id, text, "text" if m.text else "media", now
                ))

                # 4. Сохраняем в basa_messages
                conn.execute("""
                    INSERT OR IGNORE INTO basa_messages
                    (session_id,chat_id,chat_title,chat_username,chat_type,
                     user_id,username,first_name,last_name,
                     message_id,text,content_type,timestamp,saved_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    session_id, cid, chat_title, chat_uname,
                    "supergroup" if hasattr(chat, "megagroup") else "group",
                    uid, username, first_name, last_name,
                    m.id, text, "text" if m.text else "media", now, now
                ))
                conn.execute(
                    "UPDATE basa_sessions SET total_messages=total_messages+1 WHERE session_id=?",
                    (session_id,)
                )
                # 5. Обновляем last_activity чата
                conn.execute(
                    "UPDATE chats SET last_activity=? WHERE id=?", (now, cid)
                )
        except Exception:
            pass
    return client, True


# ─────────────────────────────────────────────
#  /ACCOUNT — АВТОРИЗАЦИЯ
# ─────────────────────────────────────────────
@bot.message_handler(commands=["account"])
def cmd_account(msg: types.Message):
    if msg.from_user.id != ACCOUNT_USER_ID:
        return
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception:
        pass
    if not TELETHON_OK:
        bot.send_message(msg.chat.id, "❌ Telethon не установлен.\n<code>pip install telethon</code>")
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    if userbot_is_connected():
        info = _userbot_info
        kb.add(
            types.InlineKeyboardButton("🔄 Сменить аккаунт", callback_data="account_reauth"),
            types.InlineKeyboardButton("🔌 Отключить",        callback_data="account_disconnect"),
            types.InlineKeyboardButton("❌ Закрыть",          callback_data="account_close"),
        )
        un = f"@{info['username']}" if info.get("username") else "—"
        bot.send_message(msg.chat.id,
            f"👤 <b>Аккаунт подключён</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"🏷 {info.get('name','?')} | {un}\n"
            f"📱 <code>{info.get('phone','?')}</code>\n"
            f"🆔 <code>{info.get('id','?')}</code>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ <i>Полное копирование истории доступно</i>",
            reply_markup=kb)
    else:
        kb.add(
            types.InlineKeyboardButton("🔑 Войти в аккаунт", callback_data="account_start_auth"),
            types.InlineKeyboardButton("❌ Закрыть",          callback_data="account_close"),
        )
        bot.send_message(msg.chat.id,
            f"👤 <b>Аккаунт не подключён</b>\n\n"
            f"Подключи аккаунт чтобы /basa копировал полную историю.\n"
            f"<i>Нужны API ID и API Hash с my.telegram.org</i>",
            reply_markup=kb)

def _account_send_step(chat_id, text):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("❌ Отмена", callback_data="account_cancel"))
    return bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)

@bot.message_handler(
    func=lambda m: (
        m.from_user and m.from_user.id == ACCOUNT_USER_ID and
        _account_auth.get(ACCOUNT_USER_ID, {}).get("step") is not None
    ),
    content_types=["text"]
)
def handle_account_steps(msg: types.Message):
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception:
        pass
    uid   = msg.from_user.id
    state = _account_auth.get(uid, {})
    step  = state.get("step")
    text  = (msg.text or "").strip()

    if step == "wait_phone":
        phone    = text
        api_id   = state["api_id"]
        api_hash = state["api_hash"]
        session_path = _get_session_path(phone)
        _account_auth[uid]["phone"] = phone
        wait_msg = bot.send_message(msg.chat.id, "⏳ Отправляю код...")

        def _send():
            loop = _ensure_auth_loop()
            async def _do():
                try:
                    client = TelegramClient(session_path, api_id, api_hash, loop=loop)
                    await client.connect()
                    result = await client.send_code_request(phone)
                    _account_auth[uid]["client"]     = client
                    _account_auth[uid]["phone_hash"] = result.phone_code_hash
                    _account_auth[uid]["step"]       = "wait_code"
                    try:
                        bot.delete_message(msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    _account_send_step(msg.chat.id, "📲 Код отправлен!\nВведи код (только цифры):")
                except PhoneNumberInvalidError:
                    try:
                        bot.edit_message_text("❌ Неверный номер.", msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    _account_auth.pop(uid, None)
                except Exception as e:
                    try:
                        bot.edit_message_text(f"❌ Ошибка: {str(e)[:100]}", msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    _account_auth.pop(uid, None)
            asyncio.run_coroutine_threadsafe(_do(), loop)
        threading.Thread(target=_send, daemon=True).start()

    elif step == "wait_code":
        code       = text.replace(" ", "")
        phone      = state["phone"]
        phone_hash = state["phone_hash"]
        client     = state["client"]
        if not client:
            bot.send_message(msg.chat.id, "❌ Сессия потеряна. Попробуй /account снова.")
            _account_auth.pop(uid, None)
            return
        wait_msg = bot.send_message(msg.chat.id, "⏳ Проверяю...")

        def _sign():
            loop = _ensure_auth_loop()
            async def _do():
                try:
                    await client.sign_in(phone, code, phone_code_hash=phone_hash)
                except SessionPasswordNeededError:
                    _account_auth[uid]["step"] = "wait_2fa"
                    try:
                        bot.delete_message(msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    _account_send_step(msg.chat.id, "🔐 Включена двухфакторная аутентификация.\nВведи пароль 2FA:")
                    return
                except PhoneCodeInvalidError:
                    try:
                        bot.edit_message_text("❌ Неверный код.", msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    _account_auth.pop(uid, None)
                    return
                except Exception as e:
                    try:
                        bot.edit_message_text(f"❌ Ошибка: {str(e)[:100]}", msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    _account_auth.pop(uid, None)
                    return
                await _finish_auth(client, uid, msg.chat.id, wait_msg.message_id)
            asyncio.run_coroutine_threadsafe(_do(), loop)
        threading.Thread(target=_sign, daemon=True).start()

    elif step == "wait_2fa":
        password = text
        client   = state.get("client")
        if not client:
            bot.send_message(msg.chat.id, "❌ Сессия потеряна.")
            _account_auth.pop(uid, None)
            return
        wait_msg = bot.send_message(msg.chat.id, "⏳ Проверяю пароль...")

        def _sign2fa():
            loop = _ensure_auth_loop()
            async def _do():
                try:
                    await client.sign_in(password=password)
                except Exception as e:
                    try:
                        bot.edit_message_text(f"❌ Неверный пароль: {str(e)[:80]}", msg.chat.id, wait_msg.message_id)
                    except Exception:
                        pass
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    _account_auth.pop(uid, None)
                    return
                await _finish_auth(client, uid, msg.chat.id, wait_msg.message_id)
            asyncio.run_coroutine_threadsafe(_do(), loop)
        threading.Thread(target=_sign2fa, daemon=True).start()

async def _finish_auth(client, uid, chat_id, wait_msg_id):
    global _userbot_client, _userbot_loop, _userbot_thread, _userbot_info
    me = await client.get_me()
    _userbot_info = {
        "phone":    getattr(me,"phone","?"),
        "name":     f"{me.first_name or ''} {me.last_name or ''}".strip(),
        "id":       me.id,
        "username": getattr(me,"username",None),
    }
    session_file = ""
    if hasattr(client.session, "filename"):
        session_file = client.session.filename
    api_id_val   = client.api_id
    api_hash_val = client.api_hash
    try:
        await client.disconnect()
    except Exception:
        pass
    _account_auth.pop(uid, None)
    try:
        bot.delete_message(chat_id, wait_msg_id)
    except Exception:
        pass
    un = f"@{_userbot_info['username']}" if _userbot_info.get("username") else "—"
    bot.send_message(chat_id,
        f"✅ <b>Аккаунт подключён!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {_userbot_info.get('name','?')} | {un}\n"
        f"📱 <code>{_userbot_info.get('phone','?')}</code>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🎉 Теперь /basa копирует <b>всю историю</b> групп!")

    def _run_ub():
        global _userbot_client, _userbot_loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _userbot_loop = loop
        async def _boot():
            global _userbot_client
            sess = session_file.replace(".session","") if session_file else "userbot_session"
            nc, ok = await _async_start_userbot(api_id_val, api_hash_val, sess)
            _userbot_client = nc
            if ok:
                await nc.run_until_disconnected()
        loop.run_until_complete(_boot())
    global _userbot_thread
    _userbot_thread = threading.Thread(target=_run_ub, daemon=True)
    _userbot_thread.start()

@bot.callback_query_handler(func=lambda c: c.data.startswith("account_") and c.from_user.id == ACCOUNT_USER_ID)
def handle_account_callbacks(call):
    uid  = call.from_user.id
    data = call.data
    if data == "account_close":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
    elif data == "account_cancel":
        state  = _account_auth.get(uid, {})
        client = state.get("client")
        if client and _auth_loop and _auth_loop.is_running():
            asyncio.run_coroutine_threadsafe(client.disconnect(), _auth_loop)
        _account_auth.pop(uid, None)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.send_message(call.message.chat.id, "❌ Авторизация отменена.")
    elif data in ("account_start_auth", "account_reauth"):
        state  = _account_auth.get(uid, {})
        client = state.get("client")
        if client and _auth_loop and _auth_loop.is_running():
            asyncio.run_coroutine_threadsafe(client.disconnect(), _auth_loop)
        _ensure_auth_loop()
        _account_auth[uid] = {"step":"wait_phone","api_id":TELE_API_ID,"api_hash":TELE_API_HASH,"client":None}
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        _account_send_step(call.message.chat.id,
            "📱 <b>Авторизация</b>\n━━━━━━━━━━━━━━━━━━━━\nВведи номер телефона:\n<code>+79001234567</code>")
    elif data == "account_disconnect":
        global _userbot_client, _userbot_info
        if _userbot_client:
            try:
                asyncio.run_coroutine_threadsafe(_userbot_client.disconnect(), _userbot_loop).result(timeout=5)
            except Exception:
                pass
        _userbot_client = None
        _userbot_info   = {}
        bot.answer_callback_query(call.id, "✅ Аккаунт отключён")
        try:
            safe_edit("🔌 <b>Аккаунт отключён.</b>", call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        return
    bot.answer_callback_query(call.id)


# ═══════════════════════════════════════════════════════════════
#  /BASA — КОПИРОВАНИЕ БАЗЫ
# ═══════════════════════════════════════════════════════════════
@bot.message_handler(commands=["basa"])
def cmd_basa(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    user_states[msg.from_user.id] = "basa_wait_link"
    bot.send_message(msg.chat.id,
        "📥 <b>Копирование базы группы</b>\n\n"
        "Отправьте ссылку или @username группы.\n"
        "<i>Бот должен состоять в группе.</i>",
        reply_markup=kb_back_menu())

def basa_save_message(chat_id, msg):
    session_id = _active_basa_sessions.get(chat_id)
    if not session_id: return
    now_ = datetime.datetime.now().isoformat(timespec="seconds")
    u    = msg.from_user
    txt  = msg.text or msg.caption or f"[{msg.content_type}]"
    with db_lock, get_conn() as conn:
        conn.execute("""INSERT OR IGNORE INTO basa_messages
            (session_id,chat_id,chat_title,chat_username,chat_type,
             user_id,username,first_name,last_name,
             message_id,text,content_type,timestamp,saved_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (session_id, msg.chat.id, msg.chat.title or "?",
             getattr(msg.chat,"username",None), msg.chat.type,
             u.id if u else 0, u.username if u else None,
             u.first_name if u else None, u.last_name if u else None,
             msg.message_id, txt, msg.content_type, now_, now_))
        conn.execute("UPDATE basa_sessions SET total_messages=total_messages+1 WHERE session_id=?",
                     (session_id,))
        if u:
            conn.execute("""INSERT OR IGNORE INTO users
                (id,username,first_name,last_name,first_seen,last_seen)
                VALUES (?,?,?,?,?,?)""",
                (u.id, u.username, u.first_name, u.last_name, now_, now_))
            conn.execute("""INSERT OR IGNORE INTO chat_members
                (chat_id,user_id,username,first_name,last_name,scraped_at)
                VALUES (?,?,?,?,?,?)""",
                (chat_id, u.id, u.username, u.first_name, u.last_name, now_))
def _basa_copy_from_bot_log(chat_id, chat_info, session_id, now):
    """Bulk-insert из лога бота — один запрос вместо N."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT m.chat_id, m.chat_title, m.chat_type, m.user_id,
                   m.message_id, m.text, m.timestamp,
                   COALESCE(m.content_type,'text') AS content_type,
                   u.username, u.first_name, u.last_name
            FROM messages m LEFT JOIN users u ON u.id=m.user_id
            WHERE m.chat_id=? ORDER BY m.timestamp ASC
        """, (chat_id,)).fetchall()
    uname = chat_info.get("username")
    batch = [(session_id, r["chat_id"], r["chat_title"], uname, r["chat_type"],
              r["user_id"], r["username"], r["first_name"], r["last_name"],
              r["message_id"], r["text"], r["content_type"], r["timestamp"], now)
             for r in rows]
    with db_lock, get_conn() as conn:
        conn.executemany("""INSERT OR IGNORE INTO basa_messages
            (session_id,chat_id,chat_title,chat_username,chat_type,
             user_id,username,first_name,last_name,
             message_id,text,content_type,timestamp,saved_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch)
        conn.execute("UPDATE basa_sessions SET total_messages=? WHERE session_id=?",
                     (len(batch), session_id))
    return len(batch)
def basa_copy_messages(chat_id, chat_info, session_id, admin_id, status_msg_id, status_chat_id, start_live=True):
    saved = 0
    now   = datetime.datetime.now().isoformat(timespec="seconds")
    title = chat_info.get("title","?")
    uname = chat_info.get("username","")
    chat_link = f'<a href="https://t.me/{uname}">{title}</a>' if uname else f"<b>{title}</b>"

    # ── Checkpoint: откуда продолжать ──────────────────────────────
    with get_conn() as conn:
        sess_row = conn.execute(
            "SELECT last_message_id, total_messages, copy_status FROM basa_sessions WHERE session_id=?",
            (session_id,)
        ).fetchone()
    resume_from   = 0
    already_saved = 0
    if sess_row:
        resume_from   = sess_row["last_message_id"] or 0
        already_saved = sess_row["total_messages"]  or 0
        if sess_row["copy_status"] == "done":
            # Уже скопировано полностью — просто запускаем live если нужно
            if start_live:
                _live_set(chat_id, session_id)
            return
    saved = already_saved

    if TELETHON_OK and userbot_is_connected():
        try:
            resume_txt = f"\n🔄 <i>Продолжаю с #{resume_from} ({already_saved:,} уже есть)</i>" if resume_from else ""
            safe_edit(
                f"⏳ <b>Копирую через аккаунт...</b>\n📂 {chat_link}\n<i>Загружаю участников для телефонов...</i>{resume_txt}",
                status_chat_id, status_msg_id
            )
        except Exception:
            pass

        # Отмечаем что копирование запущено
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE basa_sessions SET copy_status='copying' WHERE session_id=?", (session_id,))

        async def _copy():
            nonlocal saved
            client = _userbot_client

            # ── Получаем entity ──────────────────────────────────────
            try:
                entity = await client.get_entity(chat_id)
            except Exception:
                try:
                    entity = await client.get_entity(uname if uname else chat_id)
                except Exception as e:
                    raise RuntimeError(f"Не могу найти чат: {e}")

            # ── ШАГ 1: Собираем участников → кэш user_id → данные ────
            # Это даёт телефоны, bio, premium и другие данные
            phone_cache: dict = {}   # user_id -> {phone, bio, premium, lang}
            members_count = 0
            try:
                _SQL_CM = """INSERT INTO chat_members
                    (chat_id,user_id,username,first_name,last_name,phone,is_bot,is_admin,is_banned,scraped_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(chat_id,user_id) DO UPDATE SET
                        username=COALESCE(excluded.username, chat_members.username),
                        first_name=COALESCE(excluded.first_name, chat_members.first_name),
                        last_name=COALESCE(excluded.last_name, chat_members.last_name),
                        phone=COALESCE(excluded.phone, chat_members.phone),
                        is_admin=excluded.is_admin,is_banned=excluded.is_banned,
                        scraped_at=excluded.scraped_at"""
                _SQL_U2 = """INSERT INTO users (id,username,first_name,last_name,phone,language_code,is_premium,first_seen,last_seen)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET
                        username=COALESCE(excluded.username, users.username),
                        first_name=COALESCE(excluded.first_name, users.first_name),
                        last_name=COALESCE(excluded.last_name, users.last_name),
                        phone=COALESCE(excluded.phone, users.phone),
                        language_code=COALESCE(excluded.language_code, users.language_code),
                        is_premium=COALESCE(excluded.is_premium, users.is_premium),
                        last_seen=excluded.last_seen"""
                mb2 = []; ub2 = []
                _ts2 = datetime.datetime.now().isoformat(timespec="seconds")
                async for user in client.iter_participants(entity, aggressive=True):
                    uid2 = user.id
                    un2  = getattr(user, "username",   None)
                    fn2  = getattr(user, "first_name", None) or ""
                    ln2  = getattr(user, "last_name",  None) or ""
                    ph2  = getattr(user, "phone",      None)
                    lg2  = getattr(user, "lang_code",  None)
                    pr2  = int(bool(getattr(user, "premium", False)))
                    bt2  = int(bool(getattr(user, "bot",     False)))
                    is_ban2 = "Banned" in type(getattr(user,"participant",None)).__name__
                    phone_cache[uid2] = {"phone": ph2, "lang": lg2, "premium": pr2,
                                         "username": un2, "first_name": fn2, "last_name": ln2}
                    mb2.append((chat_id, uid2, un2, fn2, ln2, ph2, bt2, 0, int(is_ban2), _ts2))
                    ub2.append((uid2, un2, fn2, ln2, ph2, lg2, pr2, _ts2, _ts2))
                    members_count += 1
                    if len(mb2) >= 500:
                        with db_lock, get_conn() as conn:
                            conn.executemany(_SQL_CM, mb2)
                            conn.executemany(_SQL_U2, ub2)
                        mb2.clear(); ub2.clear()
                        try:
                            safe_edit(
                                f"⏳ <b>Копирую через аккаунт...</b>\n📂 {chat_link}\n"
                                f"👥 Участников загружено: <b>{members_count:,}</b>\n"
                                f"<i>Перехожу к сообщениям...</i>",
                                status_chat_id, status_msg_id)
                        except Exception: pass
                if mb2:
                    with db_lock, get_conn() as conn:
                        conn.executemany(_SQL_CM, mb2)
                        conn.executemany(_SQL_U2, ub2)
            except Exception:
                pass  # Если нет прав на participants — продолжаем без телефонов

            # ── ШАГ 2: Копируем сообщения ────────────────────────────
            try:
                safe_edit(
                    f"⏳ <b>Копирую сообщения...</b>\n📂 {chat_link}\n"
                    f"👥 Участников: <b>{members_count:,}</b>\n💬 Сообщений: <b>0</b>",
                    status_chat_id, status_msg_id)
            except Exception: pass

            _SQL_MSG = """INSERT OR IGNORE INTO basa_messages
                (session_id,chat_id,chat_title,chat_username,chat_type,
                 user_id,username,first_name,last_name,
                 message_id,text,content_type,timestamp,saved_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

            # sender cache: id → объект (для юзеров не в participants)
            _sc: dict = {}
            batch = []
            last_upd_saved = 0
            last_checkpoint_id = resume_from  # последний сохранённый message_id

            # Если продолжаем — стартуем с resume_from, иначе с начала
            iter_kwargs = {"reverse": True, "limit": None}
            if resume_from:
                iter_kwargs["min_id"] = resume_from  # берём только сообщения ПОСЛЕ checkpoint

            async for message in client.iter_messages(entity, **iter_kwargs):
                sid_ = getattr(message, "sender_id", None) or 0

                # Берём данные из phone_cache (участники) или sender cache
                if sid_ and sid_ in phone_cache:
                    pc   = phone_cache[sid_]
                    un_  = pc["username"]
                    fn_  = pc["first_name"]
                    ln_  = pc["last_name"]
                else:
                    sender = _sc.get(sid_)
                    if sender is None and sid_:
                        try:
                            sender = await message.get_sender()
                            _sc[sid_] = sender
                        except Exception:
                            sender = None
                    un_ = getattr(sender, "username",   None) if sender else None
                    fn_ = getattr(sender, "first_name", None) if sender else None
                    ln_ = getattr(sender, "last_name",  None) if sender else None

                ts  = message.date.isoformat() if message.date else now
                txt = message.text or message.message or ""
                if not txt and message.media:
                    txt = f"[{type(message.media).__name__}]"

                ctype = "text" if message.text else (
                    type(message.media).__name__.lower().replace("messagemedia","") if message.media else "text"
                )

                batch.append((
                    session_id, chat_id, title, uname, "supergroup",
                    sid_ or 0, un_, fn_, ln_,
                    message.id, txt, ctype, ts, now
                ))
                last_checkpoint_id = message.id  # обновляем checkpoint

                if len(batch) >= 500:
                    with db_lock, get_conn() as conn:
                        conn.executemany(_SQL_MSG, batch)
                        # Сохраняем checkpoint — message_id последнего сохранённого
                        conn.execute(
                            "UPDATE basa_sessions SET total_messages=?, last_message_id=? WHERE session_id=?",
                            (saved + len(batch), last_checkpoint_id, session_id)
                        )
                    saved += len(batch)
                    batch.clear()
                    if saved - last_upd_saved >= 500:
                        last_upd_saved = saved
                        resume_info = f" (продолжено)" if resume_from else ""
                        try:
                            safe_edit(
                                f"⏳ <b>Копирую сообщения...</b>\n📂 {chat_link}\n"
                                f"👥 Участников: <b>{members_count:,}</b>\n"
                                f"💬 Скопировано: <b>{saved:,}</b>{resume_info}\n"
                                f"🔖 Checkpoint: #{last_checkpoint_id}",
                                status_chat_id, status_msg_id)
                        except Exception: pass

            if batch:
                with db_lock, get_conn() as conn:
                    conn.executemany(_SQL_MSG, batch)
                    conn.execute(
                        "UPDATE basa_sessions SET total_messages=?, last_message_id=? WHERE session_id=?",
                        (saved + len(batch), last_checkpoint_id, session_id)
                    )
                saved += len(batch)

            # Копирование завершено — отмечаем done
            with db_lock, get_conn() as conn:
                conn.execute(
                    "UPDATE basa_sessions SET copy_status='done' WHERE session_id=?",
                    (session_id,)
                )

        try:
            asyncio.run_coroutine_threadsafe(_copy(), _userbot_loop).result(timeout=7200)
        except Exception as e:
            try:
                safe_edit(
                    f"⚠️ Ошибка копирования через аккаунт: {str(e)[:80]}\nКопирую из лога бота...",
                    status_chat_id, status_msg_id
                )
            except Exception:
                pass
            saved = _basa_copy_from_bot_log(chat_id, chat_info, session_id, now)
    else:
        saved = _basa_copy_from_bot_log(chat_id, chat_info, session_id, now)

    if start_live:
        _live_set(chat_id, session_id)

    mode_str = "👤 через аккаунт" if (TELETHON_OK and userbot_is_connected()) else "🤖 из лога бота"
    live_str = "🔴 <b>активен</b>" if start_live else "⏹ выключен"
    try:
        safe_edit(
            f"✅ <b>История скопирована!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 {chat_link}\n💬 Скопировано: <b>{saved}</b> {mode_str}\n"
            f"🔴 Live-мониторинг: {live_str}\n━━━━━━━━━━━━━━━━━━━━",
            status_chat_id, status_msg_id,
            reply_markup=_kb_basa_view(session_id, chat_id)
        )
    except Exception:
        pass

def _resolve_chat_entity(link_text: str):
    import re as re2
    link = link_text.strip().rstrip("/")
    link = re2.sub(r"^https?://t\.me/", "", link)
    link = re2.sub(r"^@", "", link)

    # Числовой ID (может быть -1001234567890 или 1234567890)
    numeric_id = None
    try:
        raw = int(link)
        # Нормализуем: если > 0 и похож на supergroup ID → добавляем -100
        if raw > 0 and len(str(raw)) >= 10:
            numeric_id = int(f"-100{raw}")
        else:
            numeric_id = raw
    except (ValueError, TypeError):
        pass

    with get_conn() as conn:
        # 1. Ищем по username в chats
        found = None
        if link and not numeric_id:
            found = conn.execute("SELECT * FROM chats WHERE LOWER(username)=LOWER(?)", (link,)).fetchone()
        # 2. Ищем по числовому ID в chats
        if not found and numeric_id:
            found = conn.execute("SELECT * FROM chats WHERE id=?", (numeric_id,)).fetchone()
        if found:
            return found["id"], found["title"] or "Без названия", found["username"] or ""

        # 3. Фолбэк: если числовой ID есть в basa_messages — группа известна боту по логу
        if numeric_id:
            bm = conn.execute(
                "SELECT chat_id, chat_title, chat_username FROM basa_messages WHERE chat_id=? LIMIT 1",
                (numeric_id,)
            ).fetchone()
            if bm:
                return bm["chat_id"], bm["chat_title"] or "Без названия", bm["chat_username"] or ""

    # 4. Пробуем через Bot API
    try:
        target   = f"@{link}" if not numeric_id else numeric_id
        chat_obj = bot.get_chat(target)
        upsert_chat(chat_obj)
        return chat_obj.id, chat_obj.title or "Без названия", getattr(chat_obj,"username","") or ""
    except Exception:
        pass

    # 5. Пробуем через userbot (Telethon)
    if TELETHON_OK and userbot_is_connected():
        try:
            async def _get():
                # Для числовых ID используем InputPeerChannel если возможно
                target_ub = numeric_id if numeric_id else f"@{link}"
                return await _userbot_client.get_entity(target_ub)
            entity = asyncio.run_coroutine_threadsafe(_get(), _userbot_loop).result(timeout=10)
            title  = getattr(entity,"title",None) or getattr(entity,"first_name","?")
            uname  = getattr(entity,"username","") or ""
            cid    = entity.id
            if cid > 0 and len(str(cid)) >= 10:
                cid = int(f"-100{cid}")
            return cid, title, uname
        except Exception:
            pass

    return None, None, None

def _process_basa_link(msg: types.Message, link_text: str):
    if link_text.startswith("+") or "joinchat" in link_text:
        bot.send_message(msg.chat.id,
            "❌ <b>Инвайт-ссылки не поддерживаются</b>\nИспользуй @username или числовой ID.",
            reply_markup=kb_back_menu())
        return
    wait = bot.send_message(msg.chat.id, "🔍 Ищу группу...")
    chat_id, chat_title, chat_uname = _resolve_chat_entity(link_text)
    try:
        bot.delete_message(msg.chat.id, wait.message_id)
    except Exception:
        pass
    if not chat_id:
        bot.send_message(msg.chat.id,
            "❌ <b>Группа не найдена</b>\n\n"
            "Варианты доступа:\n"
            "• <b>Бот</b> состоит в группе → введи @username или числовой ID\n"
            "• <b>Аккаунт</b> (/account) состоит в группе → введи числовой ID\n"
            "• Группа уже была в базе → введи числовой ID (из истории)\n\n"
            "<i>Числовой ID можно получить переслав сообщение из группы боту @username_to_id_bot</i>",
            reply_markup=kb_back_menu())
        return
    uname_display = f"@{chat_uname}" if chat_uname else f"<code>{chat_id}</code>"
    ub_ok = TELETHON_OK and userbot_is_connected()
    ub_note = "\n✅ <i>Аккаунт подключён — полная история</i>" if ub_ok \
              else "\n⚠️ <i>Аккаунт не подключён — только лог. /account для полной истории</i>"
    user_states[msg.from_user.id] = f"basa_chosen:{chat_id}:{chat_title}:{chat_uname}"
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("📥 Скопировать всю историю",       callback_data=f"basa_do:copy:{chat_id}"),
        types.InlineKeyboardButton("🔴 Только Live-мониторинг",        callback_data=f"basa_do:live:{chat_id}"),
        types.InlineKeyboardButton("📥 + 🔴 Скопировать + Live",       callback_data=f"basa_do:both:{chat_id}"),
        types.InlineKeyboardButton("❌ Отмена",                         callback_data="main_menu"),
    )
    bot.send_message(msg.chat.id,
        f"📂 <b>{chat_title}</b> {uname_display}{ub_note}\n\nВыбери действие:",
        reply_markup=kb)


# ─────────────────────────────────────────────
#  СЛИЯНИЕ И ОЧИСТКА БАЗ
# ─────────────────────────────────────────────
def do_merge_databases(status_chat_id, status_msg_id):
    """
    Сливает basa_messages → users, messages, chat_members, chats.
    БЫСТРО: каждый шаг — один bulk SQL без коррелированных подзапросов.
    Бот не зависает — merge идёт в отдельном потоке через run_async_merge().
    """
    stats = {
        "new_users": 0, "updated_users": 0,
        "merged_msgs": 0, "dup_msgs": 0,
        "empty_sessions": 0, "chat_members_added": 0,
    }

    def _upd(text):
        try: safe_edit(text, status_chat_id, status_msg_id)
        except Exception: pass

    t0 = datetime.datetime.now()

    # ═══════════════════════════════════════════════════════
    # ШАГ 1: Пользователи — INSERT + UPDATE через temp таблицу
    # Старый способ: UPDATE с подзапросом на каждую строку = O(N²) — МЕДЛЕННО
    # Новый способ: агрегируем в temp, потом один JOIN-UPDATE = O(N log N)
    # ═══════════════════════════════════════════════════════
    _upd("🧹 <b>Слияние баз</b>\n<i>Шаг 1/5 — пользователи...</i>")

    conn = get_conn()
    try:
        conn.execute("BEGIN")
        # Создаём temp таблицу с лучшими данными из basa_messages
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _merge_users AS
            SELECT
                user_id                          AS id,
                MAX(username)                    AS bm_username,
                MAX(first_name)                  AS bm_first_name,
                MAX(last_name)                   AS bm_last_name,
                MIN(timestamp)                   AS bm_first_seen,
                MAX(timestamp)                   AS bm_last_seen
            FROM basa_messages
            WHERE user_id IS NOT NULL AND user_id != 0
            GROUP BY user_id
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS _mu_id ON _merge_users(id)")

        # INSERT новых
        conn.execute("""
            INSERT INTO users (id, username, first_name, last_name, first_seen, last_seen)
            SELECT id, bm_username, bm_first_name, bm_last_name, bm_first_seen, bm_last_seen
            FROM _merge_users
            ON CONFLICT (id) DO NOTHING
        """)
        stats["new_users"] = conn.execute("SELECT changes()").fetchone()[0]

        # UPDATE существующих — используем FROM синтаксис PostgreSQL
        conn.execute("""
            UPDATE users SET
                username   = COALESCE(users.username,   mu.bm_username),
                first_name = COALESCE(users.first_name, mu.bm_first_name),
                last_name  = COALESCE(users.last_name,  mu.bm_last_name),
                last_seen  = GREATEST(COALESCE(users.last_seen,''), COALESCE(mu.bm_last_seen,''))
            FROM _merge_users mu
            WHERE users.id = mu.id
        """)
        stats["updated_users"] = conn.execute("SELECT changes()").fetchone()[0]
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise

    elapsed1 = (datetime.datetime.now() - t0).seconds
    _upd(f"🧹 <b>Слияние баз</b>\n"
         f"✅ Шаг 1: Люди +{stats['new_users']} новых ({elapsed1}с)\n"
         f"<i>Шаг 2/5 — участники чатов...</i>")

    # ═══════════════════════════════════════════════════════
    # ШАГ 2: chat_members + chats — быстрые bulk INSERT
    # ═══════════════════════════════════════════════════════
    conn.execute("BEGIN")
    try:
        # Temp таблица для чатов
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _merge_chats AS
            SELECT
                chat_id                         AS id,
                MAX(chat_title)                 AS title,
                COALESCE(MAX(chat_type),'supergroup') AS chat_type,
                MAX(chat_username)              AS username,
                MIN(timestamp)                  AS first_ts,
                MAX(timestamp)                  AS last_ts
            FROM basa_messages
            WHERE chat_id IS NOT NULL
            GROUP BY chat_id
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS _mc_id ON _merge_chats(id)")

        # chat_members — используем данные юзеров из _merge_users
        conn.execute("""
            INSERT INTO chat_members (chat_id, user_id, username, first_name, last_name, scraped_at)
            SELECT bm.chat_id, bm.user_id,
                MAX(mu.bm_username), MAX(mu.bm_first_name), MAX(mu.bm_last_name),
                MAX(bm.timestamp)
            FROM basa_messages bm
            LEFT JOIN _merge_users mu ON mu.id = bm.user_id
            WHERE bm.user_id IS NOT NULL AND bm.user_id != 0
            GROUP BY bm.chat_id, bm.user_id
            ON CONFLICT (chat_id, user_id) DO NOTHING
        """)
        stats["chat_members_added"] = conn.execute("SELECT changes()").fetchone()[0]

        # Телефоны: chat_members → users (быстро через JOIN)
        conn.execute("""
            UPDATE users SET phone = (
                SELECT cm.phone FROM chat_members cm
                WHERE cm.user_id=users.id AND cm.phone IS NOT NULL
                ORDER BY cm.scraped_at DESC LIMIT 1
            )
            WHERE phone IS NULL
              AND id IN (SELECT DISTINCT user_id FROM chat_members WHERE phone IS NOT NULL)
        """)

        # Чаты — INSERT новых
        conn.execute("""
            INSERT INTO chats (id, title, chat_type, username, joined_at, last_activity)
            SELECT id, title, chat_type, username, first_ts, last_ts FROM _merge_chats
            ON CONFLICT (id) DO NOTHING
        """)

        # Чаты — UPDATE существующих без подзапроса на каждую строку
        conn.execute("""
            UPDATE chats SET
                title         = COALESCE(chats.title,    mc.title),
                username      = COALESCE(chats.username,  mc.username),
                last_activity = CASE
                    WHEN COALESCE(chats.last_activity,'') > COALESCE(mc.last_ts,'')
                    THEN chats.last_activity
                    ELSE mc.last_ts
                END
            FROM _merge_chats mc
            WHERE chats.id = mc.id
        """)
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise

    elapsed2 = (datetime.datetime.now() - t0).seconds
    _upd(f"🧹 <b>Слияние баз</b>\n"
         f"✅ Шаг 1: Люди ({elapsed1}с)\n"
         f"✅ Шаг 2: Участники +{stats['chat_members_added']} ({elapsed2}с)\n"
         f"<i>Шаг 3/5 — переношу сообщения...</i>")

    # ═══════════════════════════════════════════════════════
    # ШАГ 3: basa_messages → messages
    # Один INSERT OR IGNORE — самый быстрый способ
    # ═══════════════════════════════════════════════════════
    conn.execute("BEGIN")
    try:
        conn.execute("""
            INSERT OR IGNORE INTO messages
                (user_id, chat_id, chat_title, chat_type, text, content_type, message_id, timestamp)
            SELECT
                bm.user_id, bm.chat_id, bm.chat_title,
                COALESCE(bm.chat_type,'supergroup'),
                bm.text, COALESCE(bm.content_type,'text'),
                bm.message_id, bm.timestamp
            FROM basa_messages bm
            WHERE bm.user_id IS NOT NULL AND bm.user_id != 0
              AND bm.message_id IS NOT NULL
        """)
        stats["merged_msgs"] = conn.execute("SELECT changes()").fetchone()[0]
        total_basa = conn.execute(
            "SELECT COUNT(*) FROM basa_messages WHERE user_id IS NOT NULL AND user_id != 0"
        ).fetchone()[0]
        stats["dup_msgs"] = max(0, total_basa - stats["merged_msgs"])
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise

    elapsed3 = (datetime.datetime.now() - t0).seconds
    _upd(f"🧹 <b>Слияние баз</b>\n"
         f"✅ Шаг 1+2: Люди+Чаты ({elapsed2}с)\n"
         f"✅ Шаг 3: Сообщений +{stats['merged_msgs']:,} ({elapsed3}с)\n"
         f"<i>Шаг 4/5 — обновляю счётчики сессий...</i>")

    # ═══════════════════════════════════════════════════════
    # ШАГ 4: Обновляем total_messages в basa_sessions одним UPDATE
    # ═══════════════════════════════════════════════════════
    conn.execute("BEGIN")
    try:
        conn.execute("""
            UPDATE basa_sessions SET total_messages = (
                SELECT COUNT(*) FROM basa_messages
                WHERE session_id = basa_sessions.session_id
            )
            WHERE session_id IN (
                SELECT DISTINCT session_id FROM basa_messages
            )
        """)
        # Удаляем пустые сессии одним DELETE
        conn.execute("""
            DELETE FROM basa_messages
            WHERE session_id IN (
                SELECT session_id FROM basa_sessions WHERE total_messages = 0
            )
        """)
        res = conn.execute("DELETE FROM basa_sessions WHERE total_messages = 0")
        stats["empty_sessions"] = res.rowcount if res.rowcount >= 0 else 0
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise

    elapsed4 = (datetime.datetime.now() - t0).seconds
    _upd(f"🧹 <b>Слияние баз</b>\n"
         f"✅ Шаги 1-4 готовы ({elapsed4}с)\n"
         f"<i>Шаг 5/5 — финальная очистка...</i>")

    # ═══════════════════════════════════════════════════════
    # ШАГ 5: Очистка мусора + temp таблицы
    # ═══════════════════════════════════════════════════════
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM messages WHERE user_id IS NULL OR user_id = 0")
        conn.execute("DELETE FROM events WHERE description IS NULL OR TRIM(description)=''")
        # Удаляем пустышек только если они вообще нигде не встречаются
        conn.execute("""
            DELETE FROM users WHERE
                (first_name IS NULL OR TRIM(first_name)='') AND
                (last_name  IS NULL OR TRIM(last_name)='')  AND
                (username   IS NULL OR TRIM(username)='')   AND
                id NOT IN (SELECT DISTINCT user_id FROM messages   WHERE user_id IS NOT NULL) AND
                id NOT IN (SELECT DISTINCT user_id FROM chat_members WHERE user_id IS NOT NULL)
        """)
        conn.execute("DROP TABLE IF EXISTS _merge_users")
        conn.execute("DROP TABLE IF EXISTS _merge_chats")
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")

    elapsed5 = (datetime.datetime.now() - t0).seconds
    stats["total_seconds"] = elapsed5
    return stats


# ─────────────────────────────────────────────
#  ОБРАБОТКА СООБЩЕНИЙ
# ─────────────────────────────────────────────
def _hacker_banner() -> str:
    import datetime as _dt
    now_str = _dt.datetime.now().strftime("%d.%m.%Y  %H:%M")
    ub      = "🟢 ONLINE" if userbot_is_connected() else "🔴 OFFLINE"
    ub_acc  = ""
    if userbot_is_connected():
        _i = _userbot_info
        ub_acc = f"  (@{_i.get('username') or _i.get('name','?')[:12]})"
    with get_conn() as conn:
        r = conn.execute("""SELECT
            (SELECT COUNT(*) FROM users)                              AS u_cnt,
            (SELECT COUNT(*) FROM users WHERE phone IS NOT NULL)      AS ph_cnt,
            (SELECT COUNT(*) FROM messages)                           AS m_cnt,
            (SELECT COUNT(*) FROM basa_messages)                      AS bm_cnt,
            (SELECT COUNT(*) FROM chat_members)                       AS cm_cnt,
            (SELECT COUNT(*) FROM chats)                              AS ch_cnt,
            (SELECT COUNT(*) FROM basa_sessions)                      AS sess_cnt,
            (SELECT COUNT(*) FROM banned_users)                       AS bn_cnt,
            (SELECT COUNT(*) FROM partner_bots WHERE is_active=1)     AS pb_cnt,
            (SELECT COUNT(*) FROM uploaded_databases)                 AS udb_cnt,
            (SELECT COALESCE(SUM(total_rows),0) FROM uploaded_databases) AS udb_rows
        """).fetchone()
    live_cnt = len(_active_basa_sessions)
    pbot_cnt = len([t for t in _active_partner_bots.values() if t.is_alive()])
    return (
        f"<code>┌───────────────────────────────────┐</code>\n"
        f"<code>│   🕵️   ADMIN INTELLIGENCE BOT    │</code>\n"
        f"<code>│          v3.0  PRO  MASTER        │</code>\n"
        f"<code>└───────────────────────────────────┘</code>\n"
        f"<code>  📅 {now_str}</code>\n"
        f"<code>  👤 Userbot: {ub}{ub_acc}</code>\n"
        f"<code>  🤖 Партн. ботов: {pbot_cnt}/{r['pb_cnt']}</code>\n"
        f"<code>───────────────────────────────────</code>\n"
        f"<code>  👥  Люди           {r['u_cnt']:>9,}</code>\n"
        f"<code>  📱  С номером      {r['ph_cnt']:>9,}</code>\n"
        f"<code>  💬  Сообщений      {r['m_cnt']:>9,}</code>\n"
        f"<code>  📥  Basa msgs      {r['bm_cnt']:>9,}</code>\n"
        f"<code>  👤  Участников     {r['cm_cnt']:>9,}</code>\n"
        f"<code>  🗂   Чатов          {r['ch_cnt']:>9,}</code>\n"
        f"<code>  📦  Basa-сессий    {r['sess_cnt']:>9,}</code>\n"
        f"<code>  🚫  Забанено       {r['bn_cnt']:>9,}</code>\n"
        f"<code>  🗄   Баз данных     {r['udb_cnt']:>9,}</code>\n"
        f"<code>  📊  Записей в БД   {r['udb_rows']:>9,}</code>\n"
        f"<code>  🔴  Live активных  {live_cnt:>9,}</code>\n"
        f"<code>───────────────────────────────────</code>"
    )
@bot.message_handler(commands=["import_local"])
def cmd_import_local(msg: types.Message):
    """Импорт файла напрямую с сервера по пути. Только для ADMIN_IDS."""
    uid = msg.from_user.id
    if uid not in ADMIN_IDS:
        return
    args = (msg.text or "").split(maxsplit=1)
    if len(args) < 2:
        bot.send_message(msg.chat.id,
            "<b>📂 Импорт файла с сервера</b>\n\n"
            "Использование:\n"
            "<code>/import_local /путь/к/файлу.csv</code>\n\n"
            "Это позволяет загружать файлы любого размера напрямую с сервера.\n"
            "Поддерживаемые форматы: txt, csv, json, sql, db, sqlite, xlsx, log, xml\n\n"
            "<i>Например: /import_local /home/user/base.csv</i>",
            reply_markup=kb_back_menu())
        return
    file_path = args[1].strip()
    if not _os.path.exists(file_path):
        bot.send_message(msg.chat.id,
            f"❌ Файл не найден: <code>{escape_html(file_path)}</code>",
            reply_markup=kb_back_menu())
        return
    name = _os.path.basename(file_path)
    ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext not in UDB_ALLOWED_EXTS:
        bot.send_message(msg.chat.id,
            f"⚠️ Формат <b>.{ext}</b> не поддерживается.\n"
            f"Поддерживаемые: {', '.join(sorted(UDB_ALLOWED_EXTS))}",
            reply_markup=kb_back_menu())
        return
    db_name = name.rsplit(".", 1)[0] if "." in name else name
    size_mb = _os.path.getsize(file_path) / 1024 / 1024
    status_msg = bot.send_message(msg.chat.id,
        f"<code>┌──────────────────────────┐</code>\n"
        f"<code>│  📂  ИМПОРТ С СЕРВЕРА    │</code>\n"
        f"<code>└──────────────────────────┘</code>\n\n"
        f"📄 Файл: <b>{escape_html(name)}</b>\n"
        f"📦 Размер: <b>{size_mb:.1f} МБ</b>\n\n"
        f"🔍 Анализирую структуру...")
    def _do_local_import():
        try:
            rows, _ = _parse_uploaded_file(file_path, ext)
            safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  📂  ИМПОРТ С СЕРВЕРА    │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📄 Файл: <b>{escape_html(name)}</b>\n"
                f"📋 Найдено записей: <b>{len(rows):,}</b>\n\n"
                f"💾 Сохраняю в систему...",
                msg.chat.id, status_msg.message_id)
            db_id = _save_udb(db_name, name, ext, uid, rows)
            st = _udb_stats(db_id); s = st["stats"]
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton("📂 Открыть базу", callback_data=f"udb_open:{db_id}"),
                types.InlineKeyboardButton("🗄 Все базы",     callback_data="udb_list"),
            )
            safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  ✅  БАЗА ИМПОРТИРОВАНА   │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📂 <b>{escape_html(db_name)}</b>\n"
                f"📊 Всего: <b>{len(rows):,}</b>  📱 {s['phones']:,}  👤 {s['usernames']:,}  📧 {s['emails']:,}",
                msg.chat.id, status_msg.message_id, reply_markup=kb)
        except Exception as e:
            try:
                safe_edit(f"❌ Ошибка импорта: {escape_html(str(e)[:400])}",
                          msg.chat.id, status_msg.message_id, reply_markup=kb_back_menu())
            except Exception:
                pass
    threading.Thread(target=_do_local_import, daemon=True).start()


@bot.message_handler(commands=["start"])
def cmd_start(msg: types.Message):
    upsert_user(msg.from_user)
    uid_s = msg.from_user.id

    # ── Обработка реферальной ссылки (/start ref_XXXXX) ──────────
    args = (msg.text or "").split()
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1][4:])
            # Обрабатываем только если юзер новый (первый старт = первый insert)
            with get_conn() as conn:
                first_seen = conn.execute(
                    "SELECT first_seen FROM users WHERE id=?", (uid_s,)
                ).fetchone()
            # Считаем новым если first_seen = only seconds ago (запись только что создана)
            if first_seen:
                ts = first_seen["first_seen"] or ""
                now_check = datetime.datetime.now()
                try:
                    dt = datetime.datetime.fromisoformat(ts[:19])
                    if (now_check - dt).total_seconds() < 60:  # новый юзер (< 60 сек)
                        _process_referral(referrer_id, uid_s)
                except Exception:
                    pass
        except Exception:
            pass

    if uid_s in ADMIN_IDS:
        bot.send_message(msg.chat.id, _hacker_banner(), reply_markup=kb_main_menu())
    else:
        # Проверяем — есть ли у него подключённый бот (он партнёр)
        with get_conn() as conn:
            pb = conn.execute(
                "SELECT * FROM partner_bots WHERE owner_id=? AND is_active=1", (uid_s,)
            ).fetchone()
        if pb:
            _send_partner_menu(msg.chat.id, uid_s, pb)
        else:
            # Пробуем начислить ежедневные звёзды
            ok, awarded, balance = _claim_daily_stars(uid_s)
            bonus_note = f"\n🎁 <b>+{awarded} ⭐ за первый вход сегодня!</b>" if ok else ""
            _send_user_menu(msg.chat.id, uid_s)


def _send_partner_menu(chat_id, user_id, pb=None):
    """Главное меню для партнёра (не-админа)."""
    with get_conn() as conn:
        if not pb:
            pb = conn.execute(
                "SELECT * FROM partner_bots WHERE owner_id=? AND is_active=1", (user_id,)
            ).fetchone()
        if not pb:
            bot.send_message(chat_id,
                "❌ Твой бот не найден или деактивирован.\n"
                "Подключи заново: /connect")
            return
        stars    = pb["stars"] or 0
        plan     = pb["plan"] or "free"
        lim      = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
        bot_un   = pb["bot_username"] or "?"
        today    = datetime.datetime.now().strftime("%Y-%m-%d")
        used_today = conn.execute(
            "SELECT COUNT(*) FROM stars_log WHERE bot_id=? AND reason='probiv' AND ts LIKE ?",
            (pb["id"], f"{today}%")
        ).fetchone()[0]
        chats_cnt = conn.execute(
            "SELECT COUNT(*) FROM partner_chats WHERE bot_id=?", (pb["id"],)
        ).fetchone()[0]

    remaining = lim["daily_probiv"] - used_today
    plan_icons = {"free": "🆓", "basic": "⚡", "pro": "💎", "admin": "👑"}
    pi = plan_icons.get(plan, "📋")

    text = (
        f"<code>┌─────────────────────────┐</code>\n"
        f"<code>│  🤖  @{bot_un:<18}│</code>\n"
        f"<code>└─────────────────────────┘</code>\n\n"
        f"⭐ Баланс звёзд: <b>{stars}</b>  (1⭐ = 1 пробив)\n"
        f"{pi} План: <b>{plan}</b>  |  Лимит: {lim['daily_probiv']}/день\n"
        f"🔍 Сегодня: <b>{used_today}</b> / {lim['daily_probiv']} (осталось: {max(0,remaining)})\n"
        f"🗂 Подключено чатов: <b>{chats_cnt}</b>\n\n"
        f"<b>Как работает:</b>\n"
        f"Нажми «Пробив» → введи @username, ID или номер.\n"
        f"Каждый пробив стоит 1⭐. Чтобы заработать звёзды — нажми «Заработать».\n\n"
        f"<i>Управление ботом, пополнение звёзд и смена плана — только через администратора.</i>"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🔍 Пробив",         callback_data=f"p_probiv_prompt:{pb['id']}"),
        types.InlineKeyboardButton("⭐ Заработать",      callback_data=f"p_earn:{pb['id']}"),
    )
    kb.row(
        types.InlineKeyboardButton("🗂 Мои чаты",        callback_data=f"p_chats:{pb['id']}"),
        types.InlineKeyboardButton("📊 Статистика",      callback_data=f"p_stats:{pb['id']}"),
    )
    kb.row(
        types.InlineKeyboardButton("🎁 Бонус дня",       callback_data="u_daily_stars"),
        types.InlineKeyboardButton("🔗 Пригласить",      callback_data="u_referral"),
    )
    kb.row(
        types.InlineKeyboardButton("💬 Поддержка",       callback_data="u_support"),
        types.InlineKeyboardButton("ℹ️ Справка",         callback_data=f"p_help:{pb['id']}"),
    )
    bot.send_message(chat_id, text, reply_markup=kb)


# ─────────────────────────────────────────────
#  СИСТЕМА ЗВЁЗД ДЛЯ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ
# ─────────────────────────────────────────────
DAILY_STARS_AMOUNT = 3        # звёзд в день
USER_PROBIV_COST   = 1        # стоимость пробива в звёздах
REFERRAL_STARS     = 10       # звёзд за реферала

def _get_user_stars(user_id: int) -> int:
    with get_conn() as conn:
        row = conn.execute("SELECT stars FROM user_stars WHERE user_id=?", (user_id,)).fetchone()
        return row["stars"] if row else 0

def _claim_daily_stars(user_id: int) -> tuple:
    """Попытка получить ежедневные звёзды. Возвращает (ok, stars_awarded, current_balance)."""
    today = datetime.date.today().isoformat()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM user_stars WHERE user_id=?", (user_id,)).fetchone()
    if row and (row["last_daily"] or "") >= today:
        balance = row["stars"] if row else 0
        return False, 0, balance
    # Начисляем
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO user_stars (user_id, stars, last_daily)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                stars = stars + ?,
                last_daily = ?
        """, (user_id, DAILY_STARS_AMOUNT, today, DAILY_STARS_AMOUNT, today))
    balance = _get_user_stars(user_id)
    return True, DAILY_STARS_AMOUNT, balance

def _spend_user_stars(user_id: int, amount: int) -> bool:
    """Списать звёзды. Возвращает True если успешно."""
    with db_lock, get_conn() as conn:
        row = conn.execute("SELECT stars FROM user_stars WHERE user_id=?", (user_id,)).fetchone()
        if not row or row["stars"] < amount:
            return False
        conn.execute("UPDATE user_stars SET stars=stars-? WHERE user_id=?", (amount, user_id))
    return True


def _add_user_stars(user_id: int, amount: int, reason: str = "manual"):
    """Начислить звёзды обычному пользователю."""
    now = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO user_stars (user_id, stars, last_daily)
            VALUES (?, ?, NULL)
            ON CONFLICT(user_id) DO UPDATE SET stars = stars + ?
        """, (user_id, amount, amount))


def _process_referral(referrer_id: int, new_user_id: int):
    """Обработать реферальный переход. Начисляем звёзды если новый юзер."""
    if referrer_id == new_user_id:
        return
    now = datetime.datetime.now().isoformat(timespec="seconds")
    reward = int(get_setting("referral_stars") or REFERRAL_STARS)
    with get_conn() as conn:
        # Проверяем что этот referred_id ещё не зарегистрировался по рефералу
        existing = conn.execute(
            "SELECT id FROM user_referrals WHERE referred_id=?", (new_user_id,)
        ).fetchone()
    if existing:
        return
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO user_referrals (referrer_id, referred_id, stars_awarded, created_at)
            VALUES (?,?,?,?)
        """, (referrer_id, new_user_id, reward, now))
    # Начисляем звёзды рефереру
    _add_user_stars(referrer_id, reward, "referral")
    # Уведомляем реферера
    try:
        bot.send_message(referrer_id,
            f"🎉 <b>По твоей ссылке зарегистрировался новый пользователь!</b>\n"
            f"⭐ +{reward} звёзд начислено на твой баланс.\n"
            f"<i>Баланс: {_get_user_stars(referrer_id)} ⭐</i>")
    except Exception:
        pass


# ─────────────────────────────────────────────
#  ТЕХПОДДЕРЖКА
# ─────────────────────────────────────────────
_support_admin_states: dict = {}   # admin_id -> {"ticket_id": ..., "user_id": ...}

def _get_auto_reply(text: str):
    """Проверить авто-ответ по ключевому слову."""
    text_lower = (text or "").lower()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT keyword, reply FROM support_auto_replies WHERE is_active=1"
        ).fetchall()
    for r in rows:
        if r["keyword"].lower() in text_lower:
            return r["reply"]
    return None

def _send_support_to_admins(ticket_id, user_id, text, content_type, file_id=None):
    """Переслать сообщение тех.поддержки всем администраторам."""
    with get_conn() as conn:
        u = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        total_tickets = conn.execute(
            "SELECT COUNT(*) FROM support_tickets WHERE user_id=?", (user_id,)
        ).fetchone()[0]
    name = ((u["first_name"] or "") + " " + (u["last_name"] or "")).strip() if u else str(user_id)
    un   = f"@{u['username']}" if u and u["username"] else "—"
    ph   = u["phone"] if u and u["phone"] else "—"
    pr_  = " ✨" if u and u["is_premium"] else ""
    first_seen = (u["first_seen"] or "")[:10] if u else "—"
    caption = (
        f"<code>┌──────────────────────────────┐</code>\n"
        f"<code>│  💬  ТИКЕТ #{ticket_id:<5} ПОДДЕРЖКА │</code>\n"
        f"<code>└──────────────────────────────┘</code>\n\n"
        f"👤 <a href=\"tg://user?id={user_id}\">{escape_html(name)}</a>{pr_}  {un}\n"
        f"🆔 <code>{user_id}</code>  📱 <code>{ph}</code>\n"
        f"📅 В базе с: {first_seen}  |  Тикетов: {total_tickets}\n\n"
        f"💬 <b>Сообщение:</b>\n{escape_html(text or '[медиа]')}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("✉️ Ответить",    callback_data=f"support_reply:{ticket_id}:{user_id}"),
        types.InlineKeyboardButton("🚫 Забанить",     callback_data=f"support_ban:{user_id}:{ticket_id}"),
    )
    kb.row(
        types.InlineKeyboardButton("✅ Закрыть",     callback_data=f"support_close:{ticket_id}"),
        types.InlineKeyboardButton("📋 Все тикеты",  callback_data="support_list:0"),
    )
    kb.add(types.InlineKeyboardButton("👤 Профиль",  callback_data=f"uprofile:{user_id}:info:0:0"))
    for aid in ADMIN_IDS:
        try:
            if file_id and content_type == "photo":
                bot.send_photo(aid, file_id, caption=caption, reply_markup=kb)
            elif file_id and content_type == "video":
                bot.send_video(aid, file_id, caption=caption, reply_markup=kb)
            elif file_id and content_type == "document":
                bot.send_document(aid, file_id, caption=caption, reply_markup=kb)
            elif file_id and content_type == "voice":
                bot.send_voice(aid, file_id, caption=caption, reply_markup=kb)
            else:
                bot.send_message(aid, caption, reply_markup=kb)
        except Exception:
            pass

def _send_user_menu(chat_id, user_id, msg_id=None, edit=False):
    """Меню для обычного пользователя (не партнёра)."""
    stars  = _get_user_stars(user_id)
    today  = datetime.date.today().isoformat()
    with get_conn() as conn:
        daily_row = conn.execute("SELECT last_daily FROM user_stars WHERE user_id=?", (user_id,)).fetchone()
        used_today = conn.execute(
            "SELECT COUNT(*) FROM user_probiv_log WHERE user_id=? AND ts LIKE ?",
            (user_id, f"{today}%")
        ).fetchone()[0]
        ref_count = conn.execute(
            "SELECT COUNT(*) FROM user_referrals WHERE referrer_id=?", (user_id,)
        ).fetchone()[0]
    daily_available = not daily_row or (daily_row["last_daily"] or "") < today

    # Получаем username бота
    try:
        bot_info = bot.get_me()
        bot_username = bot_info.username
    except Exception:
        bot_username = None

    ref_line = f"👥 Рефералов: <b>{ref_count}</b>" if ref_count else ""

    text = (
        f"<code>┌─────────────────────────┐</code>\n"
        f"<code>│  👤  М О Ё  М Е Н Ю    │</code>\n"
        f"<code>└─────────────────────────┘</code>\n\n"
        f"⭐ Баланс: <b>{stars}</b> звёзд\n"
        f"🔍 Пробив стоит: <b>{USER_PROBIV_COST} ⭐</b> | Сегодня: <b>{used_today}</b>\n"
        f"{'🎁 Ежедневный бонус: <b>доступен!</b>' if daily_available else f'✅ Бонус получен. Следующий — завтра'}\n"
        + (f"{ref_line}\n" if ref_line else "")
        + f"\n<b>Что такое пробив?</b>\n"
        f"Поиск пользователя по базе: @username, ID или номер телефона.\n"
        f"Показывает: имя, группы, активность, телефон.\n\n"
        f"<b>Хочешь больше звёзд?</b> Пригласи друга!\n"
        f"Зарабатывай <b>{REFERRAL_STARS} ⭐</b> за каждого нового пользователя.\n\n"
        f"<i>⚠️ Данные администраторов недоступны</i>"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🔍 Пробив",         callback_data="u_probiv_prompt"),
        types.InlineKeyboardButton("⭐ Получить звёзды", callback_data="u_daily_stars"),
    )
    kb.row(
        types.InlineKeyboardButton("🔗 Пригласить",     callback_data="u_referral"),
        types.InlineKeyboardButton("💬 Поддержка",       callback_data="u_support"),
    )
    kb.add(types.InlineKeyboardButton("📊 Моя статистика", callback_data="u_my_stats"))
    kb.add(types.InlineKeyboardButton("🔌 Подключить бота", callback_data="u_connect_bot"))
    if edit and msg_id:
        safe_edit(text, chat_id, msg_id, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)


@bot.message_handler(commands=["menu"])
def cmd_menu(msg: types.Message):
    upsert_user(msg.from_user)
    uid_m = msg.from_user.id
    if uid_m in ADMIN_IDS:
        bot.send_message(msg.chat.id, _hacker_banner(), reply_markup=kb_main_menu())
    else:
        with get_conn() as conn:
            pb = conn.execute(
                "SELECT * FROM partner_bots WHERE owner_id=? AND is_active=1", (uid_m,)
            ).fetchone()
        if pb:
            _send_partner_menu(msg.chat.id, uid_m, pb)
        else:
            # Обычный пользователь — показываем меню с пробивом за звёзды
            _send_user_menu(msg.chat.id, uid_m)


@bot.message_handler(commands=["connect"])
def cmd_connect(msg: types.Message):
    """Подключение бота партнёром (не-админом)."""
    upsert_user(msg.from_user)
    user_states[msg.from_user.id] = "partner_add"
    bot.send_message(msg.chat.id,
        f"<code>┌──────────────────────────┐</code>\n"
        f"<code>│  🔌  ПОДКЛЮЧЕНИЕ         │</code>\n"
        f"<code>└──────────────────────────┘</code>\n\n"
        f"Отправь <b>токен своего бота</b> от @BotFather\n\n"
        f"📋 Формат: <code>1234567890:ABCdef...</code>\n\n"
        f"✅ После подключения получишь <b>100 ⭐ звёзд</b>\n"
        f"🔍 Сможешь делать пробивы по базе\n"
        f"📱 Узнавать телефоны, группы, активность\n\n"
        f"<i>Тариф free: 5 пробивов/день бесплатно</i>"
    )



@bot.message_handler(commands=["staff"])
def cmd_staff(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    _show_staff_panel(msg.chat.id)

@bot.message_handler(commands=["addstaff"])
def cmd_addstaff(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    # /addstaff @username admin|moderator
    parts = (msg.text or "").split()
    if len(parts) < 2:
        bot.send_message(msg.chat.id,
            "📋 Использование:\n"
            "<code>/addstaff @username admin</code>\n"
            "<code>/addstaff @username moderator</code>\n\n"
            "Или через меню: /staff",
            reply_markup=kb_back_menu())
        return
    uname_raw = parts[1].lstrip("@")
    role      = parts[2].lower() if len(parts) > 2 else "moderator"
    if role not in ("admin", "moderator"):
        role = "moderator"
    _do_add_staff(msg.chat.id, msg.from_user.id, uname_raw, role)

def _show_staff_panel(chat_id, msg_id=None, edit=False):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT sr.*, u.first_name, u.last_name
            FROM staff_roles sr
            LEFT JOIN users u ON u.id = sr.user_id
            ORDER BY sr.added_at DESC
        """).fetchall()
    lines = [
        "<code>┌──────────────────────────┐</code>",
        "<code>│  👮  СОТРУДНИКИ          │</code>",
        "<code>└──────────────────────────┘</code>\n",
    ]
    kb = types.InlineKeyboardMarkup(row_width=1)
    role_icons = {"admin": "🔑", "moderator": "🛡"}
    for r in rows:
        icon     = role_icons.get(r["role"], "👤")
        name     = ((r["first_name"] or "") + " " + (r["last_name"] or "")).strip() or "?"
        un       = f"@{r['username']}" if r["username"] else f"ID {r['user_id']}"
        date_str = (r["added_at"] or "")[:10]
        lines.append(f"{icon} <b>{name}</b>  {un}  — <i>{r['role']}</i>  <code>{date_str}</code>")
        # Кнопки: сменить роль + убрать
        other_role = "admin" if r["role"] == "moderator" else "moderator"
        other_icon = "🔑" if other_role == "admin" else "🛡"
        kb.row(
            types.InlineKeyboardButton(
                f"{other_icon} → {other_role}",
                callback_data=f"staff_role:{r['user_id']}:{other_role}"
            ),
            types.InlineKeyboardButton(
                f"❌ Убрать {name[:15]}",
                callback_data=f"staff_remove:{r['user_id']}"
            ),
        )
    if not rows:
        lines.append("<i>Сотрудников пока нет.</i>")
    lines.append(f"\n<b>Всего:</b> {len(rows)}")
    lines.append("\n🔑 <b>admin</b> — всё кроме управления суперадминами")
    lines.append("🛡 <b>moderator</b> — просмотр базы, пробив, поиск")
    lines.append("\n<i>Добавить: отправь @username или ID после нажатия кнопки</i>")
    kb.row(
        types.InlineKeyboardButton("➕ Добавить admin",     callback_data="staff_add:admin"),
        types.InlineKeyboardButton("➕ Добавить moderator", callback_data="staff_add:moderator"),
    )
    kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    text = "\n".join(lines)
    if edit and msg_id:
        safe_edit(text, chat_id, msg_id, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)

def _do_add_staff(chat_id, admin_id, username_or_id: str, role: str):
    """Найти юзера и добавить в staff_roles."""
    # Ищем по username или ID
    target_id = None
    target_un = None
    with get_conn() as conn:
        if username_or_id.isdigit():
            u = conn.execute("SELECT * FROM users WHERE id=?", (int(username_or_id),)).fetchone()
        else:
            u = conn.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(?)", (username_or_id,)).fetchone()
    if u:
        target_id = u["id"]
        target_un = u["username"]
    else:
        # Пробуем через Bot API
        try:
            chat_obj = bot.get_chat(f"@{username_or_id}" if not username_or_id.isdigit() else int(username_or_id))
            target_id = chat_obj.id
            target_un = getattr(chat_obj, "username", None)
        except Exception:
            pass
    if not target_id:
        bot.send_message(chat_id,
            f"❌ Пользователь <code>{escape_html(username_or_id)}</code> не найден в базе.\n"
            f"<i>Пользователь должен хотя бы раз написать боту.</i>",
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("👮 Сотрудники", callback_data="staff_panel")
            ))
        return
    if target_id in ADMIN_IDS:
        bot.send_message(chat_id, "⚠️ Этот пользователь уже суперадмин.")
        return
    now_s = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO staff_roles (user_id, role, username, added_by, added_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, username=excluded.username, added_at=excluded.added_at
        """, (target_id, role, target_un, admin_id, now_s))
    with _staff_lock:
        _staff_cache[target_id] = role
    role_icon = "🔑" if role == "admin" else "🛡"
    un_str = f"@{target_un}" if target_un else f"ID {target_id}"
    bot.send_message(chat_id,
        f"✅ {role_icon} <b>{un_str}</b> добавлен как <b>{role}</b>",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("👮 Сотрудники", callback_data="staff_panel")
        ))
    # Уведомляем нового сотрудника
    try:
        bot.send_message(target_id,
            f"🎉 Тебе выдана роль <b>{role_icon} {role}</b> в боте!\n"
            f"<i>Используй /menu для входа.</i>")
    except Exception:
        pass

@bot.message_handler(commands=["banned"])
def cmd_banned(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT b.*, u.username, u.first_name FROM banned_users b
            LEFT JOIN users u ON u.id=b.user_id ORDER BY b.banned_at DESC
        """).fetchall()
    if not rows:
        bot.send_message(msg.chat.id, "🚫 Список банов пуст.", reply_markup=kb_back_menu())
        return
    lines = []
    for r in rows:
        tl = f'<a href="tg://user?id={r["user_id"]}">{r["first_name"] or "—"}</a>'
        un = f"@{r['username']}" if r["username"] else "—"
        lines.append(f"• {tl} | {un} | <code>{r['user_id']}</code>\n  📌 {r['reason'] or '—'}")
    bot.send_message(msg.chat.id,
        f"🚫 <b>Забаненные ({len(rows)})</b>\n\n" + "\n\n".join(lines),
        reply_markup=kb_back_menu())

@bot.message_handler(content_types=["text","photo","video","document","audio","sticker","voice"])
def handle_all_messages(msg: types.Message):
    uid = msg.from_user.id if msg.from_user else 0

    if msg.text and msg.text.startswith("/"):
        cmd = msg.text.split()[0].lower().split("@")[0]
        if cmd in ("/start","/menu","/basa","/banned","/account"):
            return

    if (msg.from_user and msg.from_user.id == ACCOUNT_USER_ID and
            _account_auth.get(ACCOUNT_USER_ID, {}).get("step") is not None):
        return

    if msg.from_user and not msg.from_user.is_bot:
        upsert_user(msg.from_user)

    if uid and uid not in ADMIN_IDS and is_banned(uid):
        if get_setting("ban_block") == "1":
            try:
                bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
        return

    if msg.chat.type in ("group","supergroup","channel"):
        upsert_chat(msg.chat)

    text        = msg.text or msg.caption or f"[{msg.content_type}]"
    reply_to    = msg.reply_to_message.message_id if msg.reply_to_message else None
    fwd_from_id = msg.forward_from.id if msg.forward_from else None
    log_message(uid, msg.chat.id, msg.chat.title or "ЛС", msg.chat.type,
                text, msg.message_id, msg.content_type, reply_to, fwd_from_id)

    if msg.chat.id in _active_basa_sessions:
        try:
            basa_save_message(msg.chat.id, msg)
        except Exception:
            pass

    # ── СОСТОЯНИЯ ДЛЯ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ (включая не-админов) ───────
    if msg.from_user:
        state_all = user_states.get(msg.from_user.id)

        # ── ТЕХПОДДЕРЖКА — пользователь пишет в поддержку ───────────
        if state_all == "support_wait_msg" and uid not in ADMIN_IDS:
            user_states.pop(msg.from_user.id, None)
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            txt = msg.text or msg.caption or f"[{msg.content_type}]"
            file_id = None
            if msg.photo:
                file_id = msg.photo[-1].file_id
            elif msg.video:
                file_id = msg.video.file_id
            elif msg.document:
                file_id = msg.document.file_id
            elif msg.voice:
                file_id = msg.voice.file_id
            # Проверяем авто-ответ
            auto = _get_auto_reply(txt)
            # Сохраняем тикет
            with db_lock, get_conn() as conn:
                cur = conn.execute("""
                    INSERT INTO support_tickets
                    (user_id, text, content_type, file_id, status, created_at, updated_at)
                    VALUES (?,?,?,?,'open',?,?)
                """, (uid, txt, msg.content_type, file_id, now_s, now_s))
                ticket_id = cur.lastrowid
            if auto:
                bot.send_message(msg.chat.id,
                    f"💬 <b>Автоответ поддержки:</b>\n\n{auto}\n\n"
                    f"<i>Если это не помогло — ваш запрос передан оператору.</i>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ В меню", callback_data="u_menu")
                    ))
            else:
                bot.send_message(msg.chat.id,
                    f"✅ <b>Сообщение отправлено в поддержку!</b>\n"
                    f"Тикет #{ticket_id}. Мы ответим вам здесь.\n\n"
                    f"<i>Ожидайте ответа от администратора.</i>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ В меню", callback_data="u_menu")
                    ))
            _send_support_to_admins(ticket_id, uid, txt, msg.content_type, file_id)
            return

        # ── ДОБАВЛЕНИЕ СОТРУДНИКА ────────────────────────────────────
        if state_all and state_all.startswith("staff_add:") and uid in ADMIN_IDS:
            role = state_all.split(":")[1]
            user_states.pop(uid, None)
            raw_input = (msg.text or "").strip().lstrip("@")
            if not raw_input:
                bot.send_message(msg.chat.id, "❌ Пустой ввод.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Назад", callback_data="staff_panel")
                    ))
                return
            _do_add_staff(msg.chat.id, uid, raw_input, role)
            return

        # ── ЧАТ АДМИНОВ — пересылаем сообщение ──────────────────────
        if state_all == "adminchat" and uid in ADMIN_IDS:
            _adminchat_send(uid, text=msg.text, msg=msg)
            # Подтверждение отправителю
            names  = _adminchat_names()
            online = [names[a] for a in ADMIN_IDS if _adminchat_active.get(a) and a != uid]
            kb_ac  = types.InlineKeyboardMarkup(row_width=2)
            kb_ac.row(
                types.InlineKeyboardButton("👥 Статус", callback_data="adminchat_status"),
                types.InlineKeyboardButton("🔴 Выйти",  callback_data="adminchat_leave"),
            )
            try:
                bot.send_message(uid,
                    f"✅ <i>Отправлено → {', '.join(online) if online else 'нет активных'}</i>",
                    reply_markup=kb_ac)
            except Exception: pass
            return

        # ── РАССЫЛКА — admin ввёл сообщение ──────────────────────────
        if state_all == "broadcast_wait_msg" and uid in ADMIN_IDS:
            user_states.pop(uid, None)
            import uuid as _uuid
            bc_key = _uuid.uuid4().hex[:12]
            ct = msg.content_type
            file_id = None
            text_bc = ""
            cap_bc  = ""
            if ct == "text":
                text_bc = msg.text or ""
                preview = text_bc[:60]
            elif ct == "photo":
                file_id = msg.photo[-1].file_id
                cap_bc  = msg.caption or ""
                preview = f"[фото] {cap_bc[:50]}"
            elif ct == "video":
                file_id = msg.video.file_id
                cap_bc  = msg.caption or ""
                preview = f"[видео] {cap_bc[:50]}"
            elif ct == "document":
                file_id = msg.document.file_id
                cap_bc  = msg.caption or ""
                preview = f"[документ] {cap_bc[:50]}"
            elif ct == "voice":
                file_id = msg.voice.file_id
                preview = "[голосовое]"
            elif ct == "audio":
                file_id = msg.audio.file_id
                preview = "[аудио]"
            else:
                bot.send_message(msg.chat.id, "❌ Неподдерживаемый тип. Отправь текст, фото, видео или документ.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Назад", callback_data="broadcast_menu")
                    ))
                return
            _broadcast_pending[bc_key] = {
                "content_type": ct, "text": text_bc, "file_id": file_id, "caption": cap_bc
            }
            with get_conn() as conn:
                total_u = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            kb_conf = types.InlineKeyboardMarkup(row_width=2)
            kb_conf.row(
                types.InlineKeyboardButton("✅ Запустить", callback_data=f"broadcast_confirm:{bc_key}"),
                types.InlineKeyboardButton("❌ Отмена",    callback_data=f"broadcast_cancel:{bc_key}"),
            )
            bot.send_message(msg.chat.id,
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  📢  ПОДТВЕРЖДЕНИЕ       │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"👥 Получателей: <b>{total_u:,}</b>\n"
                f"📋 Тип: <b>{ct}</b>\n"
                f"📝 Превью: <i>{escape_html(preview)}</i>\n\n"
                f"<b>Запустить рассылку?</b>",
                reply_markup=kb_conf
            )
            return

        # ── ТЕХПОДДЕРЖКА — админ пишет ответ ───────────────────────
        if state_all and state_all.startswith("support_admin_reply:") and uid in ADMIN_IDS:
            parts = state_all.split(":")
            ticket_id = int(parts[1])
            target_uid = int(parts[2])
            user_states.pop(uid, None)
            reply_text = msg.text or "[медиа]"
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute("UPDATE support_tickets SET status='answered', updated_at=? WHERE id=?",
                             (now_s, ticket_id))
            try:
                bot.send_message(target_uid,
                    f"<code>┌──────────────────────────┐</code>\n"
                    f"<code>│  📩  ОТВЕТ ПОДДЕРЖКИ     │</code>\n"
                    f"<code>└──────────────────────────┘</code>\n\n"
                    f"{escape_html(reply_text)}\n\n"
                    f"<i>Тикет #{ticket_id}</i>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("💬 Написать ещё", callback_data="u_support"),
                        types.InlineKeyboardButton("◀️ Меню", callback_data="u_menu"),
                    ))
                bot.send_message(msg.chat.id,
                    f"✅ <b>Ответ на тикет #{ticket_id} отправлен!</b>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("📋 Все тикеты", callback_data="support_list:0"),
                        types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"),
                    ))
            except Exception as e:
                bot.send_message(msg.chat.id, f"❌ Не удалось отправить: {str(e)[:100]}")
            return

        # ── АВТО-ОТВЕТ ADMIN — добавить ─────────────────────────────
        if state_all == "support_auto_add" and uid in ADMIN_IDS:
            user_states.pop(uid, None)
            raw_ar = (msg.text or "").strip()
            if "|" not in raw_ar:
                bot.send_message(msg.chat.id,
                    "❌ Неверный формат. Используй: <code>ключевое слово|текст ответа</code>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Назад", callback_data="support_auto_list")
                    ))
                return
            keyword, reply_ar = raw_ar.split("|", 1)
            keyword = keyword.strip(); reply_ar = reply_ar.strip()
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute(
                    "INSERT INTO support_auto_replies (keyword, reply, is_active, created_at) VALUES (?,?,1,?)",
                    (keyword, reply_ar, now_s)
                )
            bot.send_message(msg.chat.id, f"✅ Авто-ответ добавлен!\n<code>{keyword}</code> → <i>{reply_ar[:80]}</i>",
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("📋 Авто-ответы", callback_data="support_auto_list")
                ))
            return

        if state_all == "partner_add":
            user_states.pop(msg.from_user.id, None)
            token = (msg.text or "").strip()
            tmp = bot.send_message(msg.chat.id, "🔌 <b>Подключаю...</b>")
            result_str = _connect_partner_bot(token, msg.from_user.id)
            if msg.from_user.id not in ADMIN_IDS:
                bot.edit_message_text(result_str, msg.chat.id, tmp.message_id, parse_mode="HTML")
                with get_conn() as conn:
                    pb_new = conn.execute(
                        "SELECT * FROM partner_bots WHERE owner_id=? AND is_active=1",
                        (msg.from_user.id,)
                    ).fetchone()
                if pb_new:
                    _send_partner_menu(msg.chat.id, msg.from_user.id, pb_new)
                for aid in ADMIN_IDS:
                    try:
                        bot.send_message(aid,
                            f"🔌 <b>Новый партнёрский бот</b>\n"
                            f"👤 <a href=\"tg://user?id={msg.from_user.id}\">{msg.from_user.first_name}</a>\n"
                            f"{result_str}",
                            reply_markup=types.InlineKeyboardMarkup().add(
                                types.InlineKeyboardButton("🔌 Панель партнёров", callback_data="partners_panel")
                            ))
                    except Exception:
                        pass
            else:
                kb_p = types.InlineKeyboardMarkup(row_width=1)
                kb_p.add(
                    types.InlineKeyboardButton("🔌 Все партнёры", callback_data="partners_panel"),
                    types.InlineKeyboardButton("🏠 Меню",          callback_data="main_menu"),
                )
                bot.edit_message_text(result_str, msg.chat.id, tmp.message_id,
                                      reply_markup=kb_p, parse_mode="HTML")
            return

        if state_all and state_all.startswith("p_probiv:"):
            pb_id = int(state_all.split(":")[1])
            user_states.pop(msg.from_user.id, None)
            raw = (msg.text or "").strip().lstrip("@").strip()
            if not raw:
                bot.send_message(msg.chat.id, "❌ Введи username, ID или телефон.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
                    ))
                return
            ok, msg_err = _check_plan_limit(pb_id, "probiv")
            if not ok:
                bot.send_message(msg.chat.id, f"⛔ {msg_err}",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("⭐ Заработать звёзды", callback_data=f"p_earn:{pb_id}"),
                        types.InlineKeyboardButton("◀️ Меню",              callback_data=f"p_menu:{pb_id}"),
                    ))
                return
            # Проверяем баланс звёзд
            with get_conn() as conn:
                pb_check = conn.execute("SELECT stars FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
            if not pb_check or pb_check["stars"] < 1:
                bot.send_message(msg.chat.id,
                    f"⛔ <b>Недостаточно звёзд для пробива</b>\n\nНа балансе: <b>{pb_check['stars'] if pb_check else 0} ⭐</b>\n\nПополни баланс чтобы делать пробивы.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("⭐ Как заработать звёзды", callback_data=f"p_earn:{pb_id}"),
                        types.InlineKeyboardButton("◀️ Меню",              callback_data=f"p_menu:{pb_id}"),
                    ))
                return
            tmp = bot.send_message(msg.chat.id, "🔍 <b>Ищу...</b>")
            target_id = int(raw) if raw.lstrip("+-").isdigit() else None
            target_un = None if target_id else raw
            if raw.startswith("+") or (raw.isdigit() and len(raw) >= 10):
                with get_conn() as conn:
                    u_ph = conn.execute("SELECT id FROM users WHERE phone=?", (raw,)).fetchone()
                    if not u_ph:
                        u_ph = conn.execute("SELECT user_id as id FROM chat_members WHERE phone=?", (raw,)).fetchone()
                if u_ph:
                    target_id = u_ph["id"]; target_un = None
                else:
                    bot.edit_message_text("❌ Телефон не найден в базе.", msg.chat.id, tmp.message_id, parse_mode="HTML",
                        reply_markup=types.InlineKeyboardMarkup().add(
                            types.InlineKeyboardButton("🔍 Ещё раз", callback_data=f"p_probiv_prompt:{pb_id}"),
                            types.InlineKeyboardButton("◀️ Меню",    callback_data=f"p_menu:{pb_id}"),
                        ))
                    return
            result = _probiv_user(target_id, target_un)
            u_res  = result["user"]
            if u_res and u_res["id"] in ADMIN_IDS:
                bot.edit_message_text(
                    "🚫 <b>Доступ запрещён</b>\nДанные этого пользователя защищены.",
                    msg.chat.id, tmp.message_id, parse_mode="HTML",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
                    ))
                return
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute(
                    "INSERT INTO stars_log (user_id, bot_id, amount, reason, ts) VALUES (?,?,?,?,?)",
                    (msg.from_user.id, pb_id, -1, "probiv", now_s)
                )
                conn.execute("UPDATE partner_bots SET stars=MAX(0, stars-1) WHERE id=?", (pb_id,))
            _send_partner_probiv_card(msg.chat.id, tmp.message_id, result, raw, pb_id)
            return

        if state_all == "u_probiv":
            user_states.pop(msg.from_user.id, None)
            raw = (msg.text or "").strip().lstrip("@").strip()
            if not raw:
                bot.send_message(msg.chat.id, "❌ Введи username, ID или телефон.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data="u_menu")
                    ))
                return

            tmp = bot.send_message(msg.chat.id, "🔍 <b>Ищу в базе...</b>")

            # Определяем тип запроса
            target_id  = None
            target_un  = None
            is_phone   = raw.startswith("+") or (raw.replace("+","").isdigit() and len(raw) >= 10)

            if is_phone:
                with get_conn() as conn:
                    u_ph = conn.execute("SELECT id FROM users WHERE phone=?", (raw,)).fetchone()
                    if not u_ph:
                        u_ph = conn.execute("SELECT user_id as id FROM chat_members WHERE phone=?", (raw,)).fetchone()
                if u_ph:
                    target_id = u_ph["id"]
                else:
                    bot.edit_message_text(
                        f"<code>┌──────────────────────────┐</code>\n"
                        f"<code>│  🔍  ПРОБИВ              │</code>\n"
                        f"<code>└──────────────────────────┘</code>\n\n"
                        f"❌ <b>Не найден</b>\n"
                        f"📱 <code>{escape_html(raw)}</code>\n\n"
                        f"<i>Номер не в базе — звёзды не списаны.</i>",
                        msg.chat.id, tmp.message_id, parse_mode="HTML",
                        reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                            types.InlineKeyboardButton("🔍 Ещё пробив", callback_data="u_probiv_prompt"),
                            types.InlineKeyboardButton("◀️ Меню",        callback_data="u_menu"),
                        ))
                    return
            elif raw.lstrip("+-").isdigit():
                target_id = int(raw)
            else:
                target_un = raw

            # Проверяем что пользователь есть в базе ДО списания звёзд
            result = _probiv_user(target_id, target_un)
            u_res  = result["user"]

            if u_res and u_res["id"] in ADMIN_IDS:
                bot.edit_message_text("🚫 <b>Доступ запрещён</b>\nДанные этого пользователя защищены.",
                    msg.chat.id, tmp.message_id, parse_mode="HTML",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data="u_menu")
                    ))
                return

            # Проверяем что хоть что-то найдено
            empty = (not u_res and not result["groups_in"]
                     and result["msg_count"] == 0 and result["basa_count"] == 0
                     and not result["phones"])
            if empty:
                bot.edit_message_text(
                    f"<code>┌──────────────────────────┐</code>\n"
                    f"<code>│  🔍  ПРОБИВ              │</code>\n"
                    f"<code>└──────────────────────────┘</code>\n\n"
                    f"❌ <b>Не найден</b>\n"
                    f"🔎 <code>{escape_html(raw)}</code>\n\n"
                    f"<i>Пользователь не в базе — звёзды не списаны.</i>",
                    msg.chat.id, tmp.message_id, parse_mode="HTML",
                    reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                        types.InlineKeyboardButton("🔍 Ещё пробив", callback_data="u_probiv_prompt"),
                        types.InlineKeyboardButton("◀️ Меню",        callback_data="u_menu"),
                    ))
                return

            # Только теперь списываем звёзды
            stars = _get_user_stars(uid)
            if stars < USER_PROBIV_COST:
                bot.edit_message_text(
                    f"⛔ <b>Недостаточно звёзд</b>\n"
                    f"Нужно: {USER_PROBIV_COST} ⭐  |  У тебя: {stars} ⭐\n\n"
                    f"Получи ежедневный бонус!",
                    msg.chat.id, tmp.message_id, parse_mode="HTML",
                    reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                        types.InlineKeyboardButton("🎁 Бонус дня",  callback_data="u_daily_stars"),
                        types.InlineKeyboardButton("◀️ Меню",       callback_data="u_menu"),
                    ))
                return

            if not _spend_user_stars(uid, USER_PROBIV_COST):
                bot.edit_message_text("⛔ Недостаточно звёзд.", msg.chat.id, tmp.message_id, parse_mode="HTML",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data="u_menu")
                    ))
                return

            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute("INSERT INTO user_probiv_log (user_id, ts) VALUES (?,?)", (uid, now_s))

            # Формируем БАЗОВУЮ карточку (бесплатно показываем)
            found_uid = u_res["id"] if u_res else target_id
            fn   = (u_res["first_name"] or "").strip() if u_res else ""
            ln   = (u_res["last_name"]  or "").strip() if u_res else ""
            name = f"{fn} {ln}".strip() or "—"
            un_  = f"@{u_res['username']}" if (u_res and u_res["username"]) else "—"
            pr_  = "✨ Premium" if (u_res and u_res["is_premium"]) else ""
            bot_ = "🤖 Бот" if (u_res and u_res["is_bot"]) else ""

            lines = [
                f"<code>┌──────────────────────────┐</code>",
                f"<code>│  🔍  ПРОБИВ              │</code>",
                f"<code>└──────────────────────────┘</code>\n",
                f"👤 <b>{name}</b>  {un_}",
            ]
            if found_uid:
                lines.append(f"🆔 <code>{found_uid}</code>")
            if pr_: lines.append(pr_)
            if bot_: lines.append(bot_)
            if result["phones"]:
                lines.append(f"📱 <b>Телефон:</b> {', '.join(f'<code>{p}</code>' for p in result['phones'])}")
            else:
                lines.append(f"📱 <b>Телефон:</b> —")
            if u_res and u_res["language_code"]:
                lines.append(f"🌍 Язык: <code>{u_res['language_code']}</code>")
            lines.append(f"\n<code>──────────────────────────</code>")
            lines.append(f"<i>💬 Сообщения, 📂 Чаты — доступны за ⭐</i>")

            new_stars = _get_user_stars(uid)

            # Кнопки для платных деталей
            kb_r = types.InlineKeyboardMarkup(row_width=2)
            if found_uid:
                total_msgs = result["msg_count"] + result["basa_count"]
                chats_cnt  = len(result["chats"])
                kb_r.row(
                    types.InlineKeyboardButton(
                        f"💬 Сообщения ({total_msgs}) — {USER_PROBIV_COST}⭐",
                        callback_data=f"u_probiv_show:msgs:{found_uid}"
                    ),
                )
                kb_r.row(
                    types.InlineKeyboardButton(
                        f"📂 Чаты ({chats_cnt}) — {USER_PROBIV_COST}⭐",
                        callback_data=f"u_probiv_show:chats:{found_uid}"
                    ),
                )
            kb_r.row(
                types.InlineKeyboardButton("🔍 Ещё пробив", callback_data="u_probiv_prompt"),
                types.InlineKeyboardButton("◀️ Меню",        callback_data="u_menu"),
            )
            bot.edit_message_text(
                "\n".join(lines) + f"\n\n⭐ Остаток: <b>{new_stars}</b>",
                msg.chat.id, tmp.message_id, parse_mode="HTML", reply_markup=kb_r
            )
            return

        if state_all and state_all.startswith("p_submit_chat:"):
            pb_id = int(state_all.split(":")[1])
            user_states.pop(msg.from_user.id, None)
            link_raw = (msg.text or "").strip()
            chat_un = link_raw.replace("https://t.me/", "").replace("@", "").strip().split("/")[0]
            if not chat_un:
                bot.send_message(msg.chat.id, "❌ Неверная ссылка.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
                    ))
                return
            with get_conn() as conn:
                ch = conn.execute(
                    "SELECT * FROM chats WHERE lower(username)=lower(?)", (chat_un,)
                ).fetchone()
            if not ch:
                bot.send_message(msg.chat.id,
                    f"⚠️ Чат <code>@{chat_un}</code> не найден в нашей базе.\n"
                    f"<i>Добавь бота в этот чат чтобы он начал собирать данные, затем пришли ссылку снова.</i>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
                    ))
                return
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                already = conn.execute(
                    "SELECT id FROM partner_chats WHERE bot_id=? AND chat_id=?", (pb_id, ch["id"])
                ).fetchone()
                if not already:
                    conn.execute("""
                        INSERT OR IGNORE INTO partner_chats
                        (bot_id, chat_id, chat_title, chat_username, chat_type, member_count, joined_at)
                        VALUES (?,?,?,?,?,?,?)
                    """, (pb_id, ch["id"], ch["title"], ch["username"], ch["chat_type"],
                          (ch["member_count"] if ch["member_count"] is not None else 0), now_s))
                    conn.execute("""
                        INSERT INTO stars_log (user_id, bot_id, amount, reason, ts)
                        VALUES (?,?,?,?,?)
                    """, (msg.from_user.id, pb_id, STARS_REWARD["link_chat"], "link_chat", now_s))
                    conn.execute("UPDATE partner_bots SET stars=stars+? WHERE id=?",
                                 (STARS_REWARD["link_chat"], pb_id))
                    reward = STARS_REWARD["link_chat"]
                    already_added = False
                else:
                    reward = 0
                    already_added = True
            if already_added:
                bot.send_message(msg.chat.id,
                    f"⚠️ Чат <b>{ch['title']}</b> уже добавлен — звёзды не начисляются дважды.",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
                    ))
            else:
                bot.send_message(msg.chat.id,
                    f"✅ <b>Чат добавлен!</b>\n📂 {ch['title']}\n⭐ Начислено: <b>+{reward} звёзд</b>",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("⭐ Заработать ещё", callback_data=f"p_earn:{pb_id}"),
                        types.InlineKeyboardButton("◀️ Меню",           callback_data=f"p_menu:{pb_id}"),
                    ))
            return

    # ──────────────────────────────────────────────────────────────

    if msg.from_user and msg.from_user.id in ADMIN_IDS:

        # ── ПЕРЕСЛАННОЕ СООБЩЕНИЕ ─────────────────────────────────
        if msg.forward_from or msg.forward_sender_name or getattr(msg, "forward_origin", None):
            fwd_user = msg.forward_from
            if fwd_user:
                upsert_user(fwd_user)
                tmp = bot.send_message(msg.chat.id, "🔍 <b>Пробиваю...</b>")
                result = _probiv_user(target_id=fwd_user.id)
                _send_probiv_card(msg.chat.id, tmp.message_id, result,
                                  str(fwd_user.id), edit=True)
            else:
                name = getattr(msg, "forward_sender_name", None) or "Скрытый пользователь"
                bot.send_message(
                    msg.chat.id,
                    f"<code>┌──────────────────────────┐</code>\n"
                    f"<code>│  🔁  ПЕРЕСЛАННОЕ         │</code>\n"
                    f"<code>└──────────────────────────┘</code>\n\n"
                    f"👤 <b>{name}</b>\n"
                    f"🔒 <b>ID скрыт</b> — приватность включена\n\n"
                    f"<i>Попроси отключить «Кто видит аккаунт при пересылке» в настройках TG</i>",
                    reply_markup=kb_back_menu()
                )
            return

        state = user_states.get(msg.from_user.id)
        if state == "probiv":
            user_states.pop(msg.from_user.id, None)
            raw = (msg.text or "").strip().lstrip("@").strip()
            if not raw:
                bot.send_message(msg.chat.id, "❌ Введи username, ID или телефон.", reply_markup=kb_back_menu())
                return
            tmp = bot.send_message(msg.chat.id, "🔍 <b>Пробиваю по всем базам...</b>")
            target_id = int(raw) if raw.lstrip("+-").isdigit() else None
            target_un = None if target_id else raw
            # Если похоже на телефон — нормализованный поиск
            digits_only = _re.sub(r"\D", "", raw)
            is_phone = (raw.startswith("+") or (digits_only and len(digits_only) >= 10))
            if is_phone and not target_id:
                ph_variants = _udb_normalize_phone(raw)
                found_id = None
                with get_conn() as conn:
                    for pv in ph_variants:
                        row = conn.execute("SELECT id FROM users WHERE phone LIKE ?", (f"%{pv}%",)).fetchone()
                        if row: found_id = row["id"]; break
                    if not found_id:
                        for pv in ph_variants:
                            row = conn.execute("SELECT user_id as id FROM chat_members WHERE phone LIKE ?", (f"%{pv}%",)).fetchone()
                            if row: found_id = row["id"]; break
                if found_id:
                    target_id = found_id; target_un = None
                # Если в users/chat_members не нашли — всё равно запускаем пробив
                # он найдёт в udb базах по телефону
            result = _probiv_user(target_id, target_un)
            # Если ничего не нашли но был телефон — добавим udb hits отдельно
            if not result.get("udb_hits") and is_phone:
                dbs = _udb_list()
                hits_d = {}
                ph_variants = _udb_normalize_phone(raw)
                for db in dbs:
                    for pv in ph_variants:
                        rows = _udb_search(db["id"], pv, "all", limit=50)
                        for r in rows:
                            key = (db["id"], r["id"])
                            if key not in hits_d:
                                hits_d[key] = {"db_name": db["name"], "db_id": db["id"], "entry": r, "matched_term": pv}
                if hits_d:
                    result["udb_hits"] = list(hits_d.values())
                    result["udb_searched_dbs"] = len(dbs)
            _send_probiv_card(msg.chat.id, tmp.message_id, result, raw, edit=True)
            return

        if state == "search":
            user_states.pop(msg.from_user.id, None)
            do_search(msg, (msg.text or "").strip())
            return
        if state == "basa_wait_link":
            user_states.pop(msg.from_user.id, None)
            _process_basa_link(msg, (msg.text or "").strip())
            return
        # ── ПОИСК ПО БАЗЕ ДАННЫХ ─────────────────────────────────
        if state and state.startswith("udb_search:") and state != "udb_search_all":
            parts_udb = state.split(":")
            db_id = int(parts_udb[1])
            field = parts_udb[2] if len(parts_udb) > 2 else "all"
            user_states.pop(msg.from_user.id, None)
            query = (msg.text or "").strip()
            if not query:
                bot.send_message(msg.chat.id, "❌ Пустой запрос.", reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("◀️ Назад", callback_data=f"udb_open:{db_id}")
                ))
                return
            results = _udb_search(db_id, query, field, limit=50)
            st = _udb_stats(db_id)
            db = st["db"]
            field_labels = {
                "all": "Везде", "name": "Имена", "phone": "Телефоны",
                "username": "Usernames", "tg_id": "Telegram ID", "email": "Email",
            }
            field_label = field_labels.get(field, field)
            text = (
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  🔍  РЕЗУЛЬТАТЫ ПОИСКА   │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"📂 <b>{escape_html(db['name']) if db else '?'}</b>\n"
                f"🎯 Поле: <b>{field_label}</b>\n"
                f"🔎 Запрос: <code>{escape_html(query)}</code>\n"
                f"📊 Найдено: <b>{len(results)}</b>\n\n"
            )
            if results:
                for r in results[:30]:
                    text += _fmt_udb_entry(r) + "\n"
                if len(results) > 30:
                    text += f"\n<i>...показаны первые 30 из {len(results)}</i>"
            else:
                text += (
                    f"<i>Ничего не найдено по полю «{field_label}».\n"
                    f"Попробуй поиск везде или другое написание.</i>"
                )
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton("🔍 Везде",  callback_data=f"udb_search:{db_id}:all"),
                types.InlineKeyboardButton("📛 Имена",  callback_data=f"udb_search:{db_id}:name"),
            )
            kb.row(
                types.InlineKeyboardButton("📱 Телефон", callback_data=f"udb_search:{db_id}:phone"),
                types.InlineKeyboardButton("👤 Username", callback_data=f"udb_search:{db_id}:username"),
            )
            kb.row(
                types.InlineKeyboardButton("◀️ База",   callback_data=f"udb_open:{db_id}"),
                types.InlineKeyboardButton("🗄 Все базы", callback_data="udb_list"),
            )
            bot.send_message(msg.chat.id, text, reply_markup=kb)
            return
        if state == "udb_search_all":
            user_states.pop(msg.from_user.id, None)
            query = (msg.text or "").strip()
            if not query:
                bot.send_message(msg.chat.id, "❌ Пустой запрос.")
                return
            dbs = _udb_list()
            if not dbs:
                bot.send_message(msg.chat.id, "❌ Нет загруженных баз.", reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("◀️ Назад", callback_data="udb_list")
                ))
                return
            tmp = bot.send_message(msg.chat.id, f"🔍 <b>Ищу</b> <code>{escape_html(query)}</code> по {len(dbs)} базам...")
            # Нормализованный поиск по всем базам
            ph_variants = _udb_normalize_phone(query)
            hits_dedup = {}
            for db in dbs:
                for term in set([query] + ph_variants):
                    if len(str(term)) < 2: continue
                    rr = _udb_search(db["id"], str(term), "all", limit=50)
                    for r in rr:
                        key = (db["id"], r["id"])
                        if key not in hits_dedup:
                            hits_dedup[key] = (db["name"], r)
            all_results = list(hits_dedup.values())
            lines = [
                f"<code>╔══════════════════════════════╗</code>",
                f"<code>║  🔍  ПОИСК ПО ВСЕМ БАЗАМ    ║</code>",
                f"<code>╚══════════════════════════════╝</code>",
                f"",
                f"🔎 Запрос: <code>{escape_html(query)}</code>",
                f"📊 Найдено: <b>{len(all_results)}</b> записей в <b>{len(dbs)}</b> базах",
                f"",
            ]
            if all_results:
                by_db = {}
                for db_name, r in all_results:
                    by_db.setdefault(db_name, []).append(r)
                for db_name, entries in by_db.items():
                    lines.append(f"📂 <b>{escape_html(db_name)}</b>  ({len(entries)} записей)")
                    for e in entries[:10]:
                        lines.append(_fmt_udb_entry(e))
                        lines.append("")
                    if len(entries) > 10:
                        lines.append(f"  <i>...ещё {len(entries)-10} записей</i>")
                    lines.append("")
            else:
                lines.append("<i>Ничего не найдено ни в одной базе</i>")
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton("🔍 Новый поиск", callback_data="udb_search_all"),
                types.InlineKeyboardButton("🗄 Базы",        callback_data="udb_list"),
            )
            kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
            safe_edit("\n".join(lines), msg.chat.id, tmp.message_id, reply_markup=kb)
            return
        if state and state.startswith("note:"):
            target_uid = int(state.split(":")[1])
            user_states.pop(msg.from_user.id, None)
            with db_lock, get_conn() as conn:
                conn.execute("UPDATE users SET notes=? WHERE id=?", (msg.text, target_uid))
            bot.send_message(msg.chat.id, "✅ Заметка сохранена!", reply_markup=kb_back_menu())
            return
        if state and state.startswith("ban:"):
            target_uid = int(state.split(":")[1])
            user_states.pop(msg.from_user.id, None)
            if target_uid in ADMIN_IDS:
                bot.send_message(msg.chat.id, "⛔ Нельзя забанить администратора!", reply_markup=kb_back_menu())
                return
            reason = (msg.text or "").strip() or "без причины"
            if reason == "-": reason = "без причины"
            ok = ban_user(target_uid, reason, msg.from_user.id)
            if ok:
                log_event("ban", user_id=target_uid, description=f"Забанен: {reason}")
                u2 = get_user_by_id(target_uid)
                n2 = (u2["first_name"] or str(target_uid)) if u2 else str(target_uid)
                bot.send_message(msg.chat.id,
                    f"🚫 Забанен: <b>{n2}</b> | <code>{target_uid}</code>\n📌 {reason}",
                    reply_markup=kb_back_menu())
            else:
                bot.send_message(msg.chat.id, "⛔ Нельзя забанить администратора!", reply_markup=kb_back_menu())
            return

    if msg.chat.type in ("group","supergroup"):
        if get_setting("anonymous_mode") == "1" and get_setting("forward_to_admin") == "1":
            if msg.from_user and uid not in ADMIN_IDS:
                chat_uname = getattr(msg.chat,"username",None)
                msg_url, _ = make_msg_link_safe(msg.chat.id, msg.message_id, chat_uname)
                for aid in ADMIN_IDS:
                    try:
                        u = get_user_by_id(uid)
                        un_str = f"@{u['username']}" if u and u["username"] else "—"
                        bot.send_message(aid,
                            f"📨 <b>Анон. режим</b>\n"
                            f"👤 {msg.from_user.first_name} | {un_str} | <code>{uid}</code>\n"
                            f"💬 {msg.chat.title}\n"
                            f'<a href="{msg_url}">🔗 Перейти</a>\n\n{text}')
                    except Exception:
                        pass

    # конец handle_all_messages


def do_search(msg: types.Message, query: str):
    """Поиск пользователей с красивым выводом и навигацией."""
    q = query.strip().lstrip("@").strip()
    if not q:
        bot.send_message(msg.chat.id, "❌ Введи запрос для поиска.", reply_markup=kb_back_menu())
        return

    results = search_users(q)

    if not results:
        # Покажем также результаты из загружаемых баз
        dbs = _udb_list()
        udb_results = []
        for db in dbs:
            rows = _udb_search(db["id"], q, "all", limit=5)
            for r in rows:
                udb_results.append({"db_name": db["name"], "entry": r})
        if udb_results:
            lines = [
                f"<code>╔══════════════════════════════╗</code>",
                f"<code>║  🔍  ПОИСК: нет в TG базе   ║</code>",
                f"<code>╚══════════════════════════════╝</code>",
                f"",
                f"По запросу <code>{escape_html(q)}</code> в Telegram базе — <b>ничего</b>.",
                f"",
                f"🗄 <b>Но найдено в загружаемых базах ({len(udb_results)}):</b>",
            ]
            seen = {}
            for hit in udb_results:
                seen.setdefault(hit["db_name"], []).append(hit["entry"])
            for dn, entries in seen.items():
                lines.append(f"")
                lines.append(f"  📂 <b>{escape_html(dn)}</b>:")
                for e in entries[:3]:
                    lines.append(_fmt_udb_entry(e))
            kb_nf = types.InlineKeyboardMarkup(row_width=1)
            kb_nf.add(types.InlineKeyboardButton("🔍 Новый поиск",  callback_data="search_prompt"))
            kb_nf.add(types.InlineKeyboardButton("🗄 Все базы",      callback_data="udb_list"))
            kb_nf.add(types.InlineKeyboardButton("🏠 Главное меню",  callback_data="main_menu"))
            bot.send_message(msg.chat.id, "\n".join(lines), reply_markup=kb_nf)
        else:
            bot.send_message(
                msg.chat.id,
                f"<code>╔══════════════════════════════╗</code>\n"
                f"<code>║  🔍  ПОИСК  ║</code>\n"
                f"<code>╚══════════════════════════════╝</code>\n\n"
                f"❌ По запросу <code>{escape_html(q)}</code> <b>ничего не найдено</b>\n\n"
                f"<i>Попробуй:\n"
                f"  • @username или username без @\n"
                f"  • Числовой Telegram ID\n"
                f"  • Номер телефона: +79001234567\n"
                f"  • Имя или фамилию (кириллица работает)</i>",
                reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                    types.InlineKeyboardButton("🔍 Попробовать снова", callback_data="search_prompt"),
                    types.InlineKeyboardButton("🕵️ Пробив", callback_data="probiv_prompt"),
                    types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"),
                )
            )
        return

    total = len(results)

    def _render_search_result(user_row, idx: int, tot: int) -> tuple:
        """Возвращает (text, keyboard) для одного результата поиска."""
        uid_r = user_row["id"] if hasattr(user_row, "__getitem__") else user_row.get("id")

        card = fmt_user_card_short(user_row, idx + 1, tot)
        header = (
            f"<code>╔══════════════════════════════╗</code>\n"
            f"<code>║  🔍  РЕЗУЛЬТАТЫ ПОИСКА      ║</code>\n"
            f"<code>╚══════════════════════════════╝</code>\n\n"
            f"Запрос: <code>{escape_html(q)}</code>  Найдено: <b>{tot}</b>\n\n"
        )
        text = header + card

        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            types.InlineKeyboardButton("👤 Профиль",  callback_data=f"uprofile:{uid_r}:info:0:0"),
            types.InlineKeyboardButton("🕵️ Пробив",   callback_data=f"probiv_show:{uid_r}"),
        )
        # Навигация
        nav = []
        if idx > 0:
            nav.append(types.InlineKeyboardButton(f"◀️ {idx}/{tot}", callback_data=f"search_nav:{q}:{idx-1}"))
        if idx < tot - 1:
            nav.append(types.InlineKeyboardButton(f"{idx+2}/{tot} ▶️", callback_data=f"search_nav:{q}:{idx+1}"))
        if nav:
            kb.row(*nav)
        kb.row(
            types.InlineKeyboardButton("🔍 Новый поиск", callback_data="search_prompt"),
            types.InlineKeyboardButton("🏠 Меню",         callback_data="main_menu"),
        )
        return text, kb

    text, kb = _render_search_result(results[0], 0, total)
    bot.send_message(msg.chat.id, text, reply_markup=kb)



@bot.callback_query_handler(func=lambda c: c.data.startswith("search_nav:"))
def handle_search_nav(call):
    uid = call.from_user.id
    if uid not in ADMIN_IDS and not is_staff(uid):
        bot.answer_callback_query(call.id, "⛔"); return
    parts = call.data.split(":", 2)
    q     = parts[1]
    index = int(parts[2])
    results = search_users(q)
    if not results or index >= len(results):
        bot.answer_callback_query(call.id, "Нет результатов"); return
    total      = len(results)
    user_row   = results[index]
    target_uid = user_row["id"]
    header = (
        f"<code>╔══════════════════════════════╗</code>\n"
        f"<code>║  🔍  РЕЗУЛЬТАТЫ ПОИСКА      ║</code>\n"
        f"<code>╚══════════════════════════════╝</code>\n\n"
        f"Запрос: <code>{escape_html(q)}</code>  Найдено: <b>{total}</b>\n\n"
    )
    text = header + fmt_user_card_short(user_row, index+1, total)
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("👤 Профиль",  callback_data=f"uprofile:{target_uid}:info:0:0"),
        types.InlineKeyboardButton("🕵️ Пробив",   callback_data=f"probiv_show:{target_uid}"),
    )
    nav = []
    if index > 0:
        nav.append(types.InlineKeyboardButton(f"◀️ {index}/{total}", callback_data=f"search_nav:{q}:{index-1}"))
    if index < total - 1:
        nav.append(types.InlineKeyboardButton(f"{index+2}/{total} ▶️", callback_data=f"search_nav:{q}:{index+1}"))
    if nav:
        kb.row(*nav)
    kb.row(
        types.InlineKeyboardButton("🔍 Новый поиск", callback_data="search_prompt"),
        types.InlineKeyboardButton("🏠 Меню",         callback_data="main_menu"),
    )
    safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb)
    bot.answer_callback_query(call.id)


# ─────────────────────────────────────────────
#  CALLBACK HANDLER ДЛЯ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data in (
    "u_probiv_prompt", "u_daily_stars", "u_my_stats", "u_connect_bot", "u_menu",
    "u_referral", "u_support",
) or c.data.startswith("u_probiv_show:") or c.data.startswith("support_"))
def handle_user_callbacks(call: types.CallbackQuery):
    uid  = call.from_user.id
    data = call.data

    # ── ADMIN support callbacks ──────────────────────────────────
    if data.startswith("support_") and uid in ADMIN_IDS:
        _handle_support_admin_callback(call)
        return

    if uid in ADMIN_IDS:
        bot.answer_callback_query(call.id)
        return  # Передаём в основной обработчик

    if data == "u_menu":
        _send_user_menu(call.message.chat.id, uid, call.message.message_id, edit=True)
        bot.answer_callback_query(call.id)

    elif data == "u_daily_stars":
        ok, awarded, balance = _claim_daily_stars(uid)
        if ok:
            bot.answer_callback_query(call.id, f"🎁 Получено +{awarded} ⭐! Баланс: {balance}", show_alert=True)
        else:
            bot.answer_callback_query(call.id, f"✅ Уже получено сегодня. Возвращайся завтра! Баланс: {balance}", show_alert=True)
        _send_user_menu(call.message.chat.id, uid, call.message.message_id, edit=True)

    elif data == "u_referral":
        # Показываем реферальную ссылку
        bot.answer_callback_query(call.id)
        try:
            bot_info = bot.get_me()
            bot_username = bot_info.username
        except Exception:
            bot_username = None
        ref_link = f"https://t.me/{bot_username}?start=ref_{uid}" if bot_username else f"(нет username бота)"
        with get_conn() as conn:
            ref_count = conn.execute(
                "SELECT COUNT(*) FROM user_referrals WHERE referrer_id=?", (uid,)
            ).fetchone()[0]
            total_earned = conn.execute(
                "SELECT COALESCE(SUM(stars_awarded),0) FROM user_referrals WHERE referrer_id=?", (uid,)
            ).fetchone()[0]
        reward = int(get_setting("referral_stars") or REFERRAL_STARS)
        promo_text = (
            f"🤖 Привет! Я нашёл крутой бот для поиска людей по Telegram.\n"
            f"Можно найти кого угодно по @username, ID или номеру телефона!\n"
            f"Заходи и получай ежедневные бонусы ⭐\n\n"
            f"👉 {ref_link}"
        )
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔗  РЕФЕРАЛЬНАЯ ссылка  │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"📨 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
            f"⭐ За каждого нового пользователя — <b>+{reward} ⭐</b>\n"
            f"👥 Приглашено: <b>{ref_count}</b> | Заработано: <b>{total_earned} ⭐</b>\n\n"
            f"<b>Скопируй текст ниже и отправь в группы:</b>\n\n"
            f"<i>{escape_html(promo_text)}</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                types.InlineKeyboardButton("◀️ Назад", callback_data="u_menu")
            )
        )

    elif data == "u_support":
        bot.answer_callback_query(call.id)
        user_states[uid] = "support_wait_msg"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  💬  ТЕХПОДДЕРЖКА        │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Напиши своё сообщение — оно будет передано в поддержку.\n"
            f"Мы ответим тебе здесь в боте.\n\n"
            f"<i>Можно отправить текст, фото, видео или документ.</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data="u_menu")
            )
        )

    elif data == "u_probiv_prompt":
        stars = _get_user_stars(uid)
        if stars < USER_PROBIV_COST:
            bot.answer_callback_query(call.id,
                f"⛔ Недостаточно звёзд ({stars}/{USER_PROBIV_COST}). Получи ежедневные звёзды!",
                show_alert=True)
            return
        user_states[uid] = "u_probiv"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔍  П Р О Б И В  ⭐{USER_PROBIV_COST}   │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Введи что хочешь найти:\n"
            f"  • <code>@username</code> — ник\n"
            f"  • <code>123456789</code> — Telegram ID\n"
            f"  • <code>+79001234567</code> — телефон\n\n"
            f"⭐ Баланс: <b>{stars}</b>  |  Стоимость: <b>{USER_PROBIV_COST} ⭐</b>\n\n"
            f"<i>⚠️ Данные администраторов недоступны</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data="u_menu")
            )
        )
        bot.answer_callback_query(call.id)

    elif data == "u_my_stats":
        stars = _get_user_stars(uid)
        today = datetime.date.today().isoformat()
        with get_conn() as conn:
            total_probiv = conn.execute(
                "SELECT COUNT(*) FROM user_probiv_log WHERE user_id=?", (uid,)
            ).fetchone()[0]
            today_probiv = conn.execute(
                "SELECT COUNT(*) FROM user_probiv_log WHERE user_id=? AND ts LIKE ?",
                (uid, f"{today}%")
            ).fetchone()[0]
            ref_count = conn.execute(
                "SELECT COUNT(*) FROM user_referrals WHERE referrer_id=?", (uid,)
            ).fetchone()[0]
            ref_earned = conn.execute(
                "SELECT COALESCE(SUM(stars_awarded),0) FROM user_referrals WHERE referrer_id=?", (uid,)
            ).fetchone()[0]
        safe_edit(
            f"<code>┌─────────────────────────┐</code>\n"
            f"<code>│  📊  М О Я  С Т А Т.   │</code>\n"
            f"<code>└─────────────────────────┘</code>\n\n"
            f"⭐ Баланс звёзд: <b>{stars}</b>\n"
            f"🔍 Пробивов сегодня: <b>{today_probiv}</b>\n"
            f"📊 Всего пробивов: <b>{total_probiv}</b>\n"
            f"👥 Рефералов: <b>{ref_count}</b>  |  Заработано: <b>{ref_earned} ⭐</b>\n\n"
            f"<i>Каждый день +{DAILY_STARS_AMOUNT} ⭐ бесплатно</i>\n"
            f"<i>1 пробив = {USER_PROBIV_COST} ⭐  |  1 реферал = {REFERRAL_STARS} ⭐</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("◀️ Назад", callback_data="u_menu")
            )
        )
        bot.answer_callback_query(call.id)

    elif data.startswith("u_probiv_show:"):
        # u_probiv_show:msgs:{uid} или u_probiv_show:chats:{uid}
        bot.answer_callback_query(call.id)
        parts    = data.split(":")
        detail   = parts[1]   # msgs / chats
        tgt_uid  = int(parts[2])
        stars    = _get_user_stars(uid)
        if stars < USER_PROBIV_COST:
            bot.answer_callback_query(call.id,
                f"⛔ Нужно {USER_PROBIV_COST}⭐. У тебя: {stars}⭐", show_alert=True)
            return
        if not _spend_user_stars(uid, USER_PROBIV_COST):
            bot.answer_callback_query(call.id, "⛔ Недостаточно звёзд", show_alert=True)
            return
        now_s = datetime.datetime.now().isoformat(timespec="seconds")
        with db_lock, get_conn() as conn:
            conn.execute("INSERT INTO user_probiv_log (user_id, ts) VALUES (?,?)", (uid, now_s))
        result = _probiv_user(target_id=tgt_uid)
        new_stars = _get_user_stars(uid)
        kb_back = types.InlineKeyboardMarkup(row_width=2).add(
            types.InlineKeyboardButton("🔍 Ещё пробив", callback_data="u_probiv_prompt"),
            types.InlineKeyboardButton("◀️ Меню",        callback_data="u_menu"),
        )
        if detail == "msgs":
            # Берём из обеих таблиц и сортируем по дате — свежие первые
            with get_conn() as conn:
                rows_main = conn.execute("""
                    SELECT text, timestamp, chat_title,
                           COALESCE((SELECT username FROM chats WHERE id=messages.chat_id), '') as chat_un,
                           content_type
                    FROM messages
                    WHERE user_id=? AND text IS NOT NULL AND text != '' AND text NOT LIKE '[%]'
                    ORDER BY timestamp DESC LIMIT 30
                """, (tgt_uid,)).fetchall()
                rows_basa = conn.execute("""
                    SELECT text, timestamp, chat_title, chat_username as chat_un, content_type
                    FROM basa_messages
                    WHERE user_id=? AND text IS NOT NULL AND text != '' AND text NOT LIKE '[%]'
                    ORDER BY timestamp DESC LIMIT 30
                """, (tgt_uid,)).fetchall()

            # Объединяем и сортируем по timestamp свежие первые
            all_msgs = []
            for m in list(rows_main) + list(rows_basa):
                ts  = m["timestamp"] or ""
                txt = (m["text"] or "").strip()
                cht = (m["chat_title"] or "").strip()
                un  = (m["chat_un"] or "").strip()
                if txt:
                    all_msgs.append({"ts": ts, "txt": txt, "cht": cht, "un": un})

            # Убираем дубли (одинаковый текст+чат) и сортируем
            seen = set()
            deduped = []
            for m in all_msgs:
                key = (m["txt"][:50], m["cht"])
                if key not in seen:
                    seen.add(key)
                    deduped.append(m)
            deduped.sort(key=lambda x: x["ts"], reverse=True)
            deduped = deduped[:15]

            lines = [
                f"Сообщения пользователя <b>{tgt_uid}</b>",
                f"Показаны {len(deduped)} свежих (от новых к старым)\n",
            ]
            for m in deduped:
                # Дата: берём только ГГГГ-ММ-ДД ЧЧ:ММ
                ts_fmt = m["ts"][:16].replace("T", " ") if m["ts"] else "—"
                # Чат
                if m["un"]:
                    chat_str = f"@{m['un']}"
                elif m["cht"]:
                    chat_str = m["cht"][:30]
                else:
                    chat_str = "неизвестный чат"
                txt_short = m["txt"][:80] + ("..." if len(m["txt"]) > 80 else "")
                lines.append(
                    f"{ts_fmt}  {chat_str}\n"
                    f"{escape_html(txt_short)}\n"
                )

            if not deduped:
                lines.append("Сообщения не найдены.")

            lines.append(f"Остаток: {new_stars} звёзд")
            bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=kb_back)
        elif detail == "chats":
            chats = result.get("chats", [])
            lines = [f"<code>┌──────────────────────────┐</code>",
                     f"<code>│  📂  ЧАТЫ                │</code>",
                     f"<code>└──────────────────────────┘</code>\n"]
            if chats:
                for c in chats[:15]:
                    un_c = f"@{c['chat_username']}" if c.get("chat_username") else ""
                    lines.append(f"📁 <b>{escape_html(c['chat_title'] or '?')}</b> {un_c}  — {c['cnt']} сообщ.")
            else:
                groups = result.get("groups_in", [])
                for g in groups[:15]:
                    role = " 👑" if g.get("is_admin") else ""
                    lines.append(f"👥 {escape_html(g['title'] or str(g['chat_id']))}{role}")
            if not chats and not result.get("groups_in"):
                lines.append("<i>Чаты не найдены в базе</i>")
            lines.append(f"\n⭐ Остаток: <b>{new_stars}</b>")
            bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=kb_back)
        user_states[uid] = "partner_add"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔌  ПОДКЛЮЧЕНИЕ         │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Отправь <b>токен своего бота</b> от @BotFather\n\n"
            f"📋 Формат: <code>1234567890:ABCdef...</code>\n\n"
            f"✅ После подключения получишь <b>100 ⭐</b> и расширенный доступ!\n\n"
            f"<i>Токен можно получить у @BotFather</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data="u_menu")
            )
        )
        bot.answer_callback_query(call.id)


# ─────────────────────────────────────────────
#  ТЕХПОДДЕРЖКА — ADMIN CALLBACKS
# ─────────────────────────────────────────────
def _handle_support_admin_callback(call):
    uid  = call.from_user.id
    data = call.data

    if data.startswith("support_reply:"):
        # support_reply:{ticket_id}:{user_id}
        parts     = data.split(":")
        ticket_id = int(parts[1])
        user_id   = int(parts[2])
        _support_admin_states[uid] = {"ticket_id": ticket_id, "user_id": user_id}
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id,
            f"✉️ <b>Ответ на тикет #{ticket_id}</b>\nНапиши ответ пользователю:")
        user_states[uid] = f"support_admin_reply:{ticket_id}:{user_id}"

    elif data.startswith("support_ban:"):
        parts   = data.split(":")
        user_id = int(parts[1])
        ticket_id = int(parts[2]) if len(parts) > 2 else 0
        if user_id not in ADMIN_IDS:
            ban_user(user_id, "Нарушение в поддержке", uid)
            bot.answer_callback_query(call.id, f"🚫 Пользователь {user_id} забанен", show_alert=True)
            try:
                bot.send_message(user_id, "🚫 <b>Вы заблокированы.</b>")
            except Exception:
                pass
        else:
            bot.answer_callback_query(call.id, "⛔ Нельзя забанить администратора", show_alert=True)

    elif data.startswith("support_close:"):
        ticket_id = int(data.split(":")[1])
        now_s = datetime.datetime.now().isoformat(timespec="seconds")
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE support_tickets SET status='closed', updated_at=? WHERE id=?",
                         (now_s, ticket_id))
        bot.answer_callback_query(call.id, f"✅ Тикет #{ticket_id} закрыт")
        try:
            safe_edit(call.message.text + f"\n\n<i>✅ Закрыт</i>",
                      call.message.chat.id, call.message.message_id)
        except Exception:
            pass

    elif data.startswith("support_list:"):
        page = int(data.split(":")[1]) if len(data.split(":")) > 1 else 0
        per = 5
        with get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'").fetchone()[0]
            tickets = conn.execute("""
                SELECT st.*, u.first_name, u.username
                FROM support_tickets st
                LEFT JOIN users u ON u.id=st.user_id
                WHERE st.status='open'
                ORDER BY st.created_at DESC
                LIMIT ? OFFSET ?
            """, (per, page * per)).fetchall()
        total_pages = max(1, (total + per - 1) // per)
        lines = [f"<b>💬 Тикеты поддержки (открытые: {total})</b>\nСтр. {page+1}/{total_pages}\n"]
        kb = types.InlineKeyboardMarkup(row_width=1)
        for t in tickets:
            name = (t["first_name"] or str(t["user_id"]))
            un   = f"@{t['username']}" if t["username"] else ""
            txt  = (t["text"] or "[медиа]")[:40]
            lines.append(f"#{t['id']} · {name} {un}\n  <i>{escape_html(txt)}</i>")
            kb.add(types.InlineKeyboardButton(
                f"#{t['id']} {name[:18]} — {(t['text'] or '[медиа]')[:20]}",
                callback_data=f"support_reply:{t['id']}:{t['user_id']}"
            ))
        nav = []
        if page > 0:
            nav.append(types.InlineKeyboardButton("◀️", callback_data=f"support_list:{page-1}"))
        if page < total_pages - 1:
            nav.append(types.InlineKeyboardButton("▶️", callback_data=f"support_list:{page+1}"))
        if nav:
            kb.row(*nav)
        kb.add(types.InlineKeyboardButton("⚙️ Авто-ответы", callback_data="support_auto_list"))
        kb.add(types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"))
        bot.answer_callback_query(call.id)
        safe_edit("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data == "support_auto_list":
        with get_conn() as conn:
            autos = conn.execute("SELECT * FROM support_auto_replies ORDER BY id DESC").fetchall()
        lines = [f"<b>⚙️ Авто-ответы поддержки ({len(autos)})</b>\n"]
        kb = types.InlineKeyboardMarkup(row_width=1)
        for a in autos:
            status = "✅" if a["is_active"] else "❌"
            lines.append(f"{status} <code>{a['keyword']}</code>\n  → <i>{a['reply'][:50]}</i>")
            kb.add(types.InlineKeyboardButton(
                f"{status} {a['keyword'][:25]}",
                callback_data=f"support_auto_toggle:{a['id']}"
            ))
        kb.add(types.InlineKeyboardButton("➕ Добавить авто-ответ", callback_data="support_auto_add"))
        kb.add(types.InlineKeyboardButton("◀️ Тикеты", callback_data="support_list:0"))
        bot.answer_callback_query(call.id)
        safe_edit("\n".join(lines) if autos else "<b>⚙️ Авто-ответы</b>\n\nПока нет авто-ответов.",
                  call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data.startswith("support_auto_toggle:"):
        ar_id = int(data.split(":")[1])
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE support_auto_replies SET is_active=1-is_active WHERE id=?", (ar_id,))
        bot.answer_callback_query(call.id, "✅ Переключено")
        call.data = "support_auto_list"
        _handle_support_admin_callback(call)

    elif data == "support_auto_add":
        user_states[uid] = "support_auto_add"
        bot.answer_callback_query(call.id)
        safe_edit(
            "⚙️ <b>Новый авто-ответ</b>\n\nОтправь в формате:\n<code>ключевое слово|текст ответа</code>\n\n"
            "Пример: <code>цена|Тарифы: free/basic/pro. Пишите в поддержку для деталей.</code>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("◀️ Отмена", callback_data="support_auto_list")
            )
        )
    else:
        bot.answer_callback_query(call.id)


# ─────────────────────────────────────────────
#  ГЛАВНЫЙ CALLBACK HANDLER
# ─────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call: types.CallbackQuery):
    uid  = call.from_user.id
    data = call.data

    # Разрешаем account_* только для нужного юзера — обрабатывается выше
    if data.startswith("account_"):
        bot.answer_callback_query(call.id)
        return

    if uid not in ADMIN_IDS:
        # Партнёрские callbacks обрабатываются ниже в этом же handler'е
        # u_* callbacks — в handle_user_callbacks выше (уже обработаны)
        if not (data.startswith("p_") or data.startswith("partner_") or
                data.startswith("u_") or data == "noop"):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return

    # ── ПОДДЕРЖКА (только для админов) ───────────────────────────
    if uid in ADMIN_IDS and (data.startswith("support_")):
        _handle_support_admin_callback(call)
        return

    # ── СОТРУДНИКИ ────────────────────────────────────────────────
    if data == "staff_panel":
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Только суперадмины"); return
        _show_staff_panel(call.message.chat.id, call.message.message_id, edit=True)
        bot.answer_callback_query(call.id)
        return

    if data.startswith("staff_add:"):
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Только суперадмины"); return
        role = data.split(":")[1]
        user_states[uid] = f"staff_add:{role}"
        bot.answer_callback_query(call.id)
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  👮  ДОБАВИТЬ {'ADMIN' if role=='admin' else 'МОДЕРАТОР'}      │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Отправь <b>@username</b> или <b>ID</b> пользователя:\n\n"
            f"{'🔑 admin — полный доступ (кроме управления суперадминами)' if role=='admin' else '🛡 moderator — просмотр базы, пробив, поиск'}\n\n"
            f"<i>Пользователь должен хотя бы раз написать боту.</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("◀️ Отмена", callback_data="staff_panel")
            )
        )
        return

    if data.startswith("staff_remove:"):
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Только суперадмины"); return
        target = int(data.split(":")[1])
        with db_lock, get_conn() as conn:
            conn.execute("DELETE FROM staff_roles WHERE user_id=?", (target,))
        with _staff_lock:
            _staff_cache.pop(target, None)
        bot.answer_callback_query(call.id, f"✅ Убран из сотрудников")
        try:
            bot.send_message(target, "ℹ️ Твоя роль в боте была отозвана.")
        except Exception: pass
        _show_staff_panel(call.message.chat.id, call.message.message_id, edit=True)
        return

    if data.startswith("staff_role:"):
        # staff_role:{user_id}:{new_role}
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔"); return
        parts_sr = data.split(":")
        target   = int(parts_sr[1])
        new_role = parts_sr[2]
        now_s    = datetime.datetime.now().isoformat(timespec="seconds")
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE staff_roles SET role=?, added_at=? WHERE user_id=?",
                         (new_role, now_s, target))
        with _staff_lock:
            _staff_cache[target] = new_role
        bot.answer_callback_query(call.id, f"✅ Роль изменена на {new_role}")
        _show_staff_panel(call.message.chat.id, call.message.message_id, edit=True)
        return

    # ── ЧАТ АДМИНОВ ──────────────────────────────────────────────
    if data == "adminchat_open":
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён"); return
        _adminchat_active[uid] = True
        names   = _adminchat_names()
        online  = [names[a] for a in ADMIN_IDS if _adminchat_active.get(a)]
        offline = [names[a] for a in ADMIN_IDS if not _adminchat_active.get(a)]
        user_states[uid] = "adminchat"
        kb_ac = types.InlineKeyboardMarkup(row_width=1)
        kb_ac.add(
            types.InlineKeyboardButton("🔴 Выйти из чата", callback_data="adminchat_leave"),
            types.InlineKeyboardButton("🏠 Главное меню",  callback_data="main_menu"),
        )
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  💬  ЧАТ АДМИНОВ         │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"🟢 Онлайн: {', '.join(online) if online else '—'}\n"
            f"⚫ Офлайн: {', '.join(offline) if offline else '—'}\n\n"
            f"<i>Просто пиши — сообщения видят все активные админы.</i>\n"
            f"<i>Поддерживается текст, фото, видео, голос, стикеры, документы.</i>",
            call.message.chat.id, call.message.message_id, reply_markup=kb_ac
        )
        # Уведомляем остальных что этот админ вошёл
        for aid in ADMIN_IDS:
            if aid != uid and _adminchat_active.get(aid):
                try:
                    bot.send_message(aid,
                        f"🟢 <b>{names.get(uid, str(uid))}</b> вошёл в чат",
                        reply_markup=types.InlineKeyboardMarkup().add(
                            types.InlineKeyboardButton("🔴 Выйти", callback_data="adminchat_leave")
                        ))
                except Exception: pass
        bot.answer_callback_query(call.id)
        return

    if data == "adminchat_leave":
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён"); return
        _adminchat_active[uid] = False
        user_states.pop(uid, None)
        names = _adminchat_names()
        # Уведомляем остальных
        for aid in ADMIN_IDS:
            if aid != uid and _adminchat_active.get(aid):
                try:
                    bot.send_message(aid, f"🔴 <b>{names.get(uid, str(uid))}</b> покинул чат")
                except Exception: pass
        bot.answer_callback_query(call.id, "Вышел из чата")
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  💬  ЧАТ АДМИНОВ         │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"🔴 Ты вышел из чата.\n\n"
            f"<i>Новые сообщения ты не получаешь.</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                types.InlineKeyboardButton("🟢 Войти снова",  callback_data="adminchat_open"),
                types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"),
            )
        )
        return

    if data == "adminchat_status":
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔"); return
        names   = _adminchat_names()
        online  = [names[a] for a in ADMIN_IDS if _adminchat_active.get(a)]
        offline = [names[a] for a in ADMIN_IDS if not _adminchat_active.get(a)]
        bot.answer_callback_query(call.id,
            f"🟢 {', '.join(online) or '—'}\n⚫ {', '.join(offline) or '—'}",
            show_alert=True)
        return

    # ── РАССЫЛКА ─────────────────────────────────────────────────
    if data == "broadcast_menu":
        with get_conn() as conn:
            total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        kb_bc = types.InlineKeyboardMarkup(row_width=1)
        kb_bc.add(
            types.InlineKeyboardButton("📢 Новая рассылка",         callback_data="broadcast_start"),
            types.InlineKeyboardButton("📋 История рассылок",       callback_data="broadcast_history:0"),
            types.InlineKeyboardButton("🏠 Главное меню",            callback_data="main_menu"),
        )
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  📢  РАССЫЛКА            │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"👥 Пользователей в базе: <b>{total_users:,}</b>\n\n"
            f"Рассылка отправляет сообщение всем пользователям бота.\n"
            f"Поддерживает: текст, фото, видео, документы.\n\n"
            f"<i>⚠️ Нельзя отменить после запуска</i>",
            call.message.chat.id, call.message.message_id, reply_markup=kb_bc
        )
        bot.answer_callback_query(call.id)
        return

    if data == "broadcast_start":
        user_states[uid] = "broadcast_wait_msg"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  📢  НОВАЯ РАССЫЛКА      │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Отправь сообщение для рассылки.\n\n"
            f"• Текст, фото, видео, документ — всё поддерживается\n"
            f"• Сообщение будет отправлено <b>всем пользователям</b>\n\n"
            f"<i>Отправь /cancel для отмены</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data="broadcast_menu")
            )
        )
        bot.answer_callback_query(call.id)
        return

    if data.startswith("broadcast_confirm:"):
        bc_key = data.split(":",1)[1]
        # Получаем сохранённое сообщение из состояния
        bc_data = _broadcast_pending.get(bc_key)
        if not bc_data:
            bot.answer_callback_query(call.id, "❌ Сессия устарела", show_alert=True)
            return
        bot.answer_callback_query(call.id)
        _broadcast_pending.pop(bc_key, None)
        threading.Thread(target=_do_broadcast, args=(bc_data, uid, call.message.chat.id), daemon=True).start()
        safe_edit(
            "📢 <b>Рассылка запущена!</b>\nОтчёт придёт по завершении.",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            )
        )
        return

    if data.startswith("broadcast_cancel:"):
        bc_key = data.split(":",1)[1]
        _broadcast_pending.pop(bc_key, None)
        bot.answer_callback_query(call.id, "❌ Рассылка отменена")
        safe_edit(_hacker_banner(), call.message.chat.id, call.message.message_id, reply_markup=kb_main_menu())
        return

    if data.startswith("broadcast_history:"):
        page = int(data.split(":")[1])
        per  = 5
        with get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM broadcast_log").fetchone()[0]
            rows  = conn.execute(
                "SELECT * FROM broadcast_log ORDER BY started_at DESC LIMIT ? OFFSET ?",
                (per, page * per)
            ).fetchall()
        total_pages = max(1, (total + per - 1) // per)
        lines = [f"<b>📋 История рассылок</b> (стр. {page+1}/{total_pages})\n"]
        for r in rows:
            lines.append(
                f"📅 {(r['started_at'] or '')[:16]}\n"
                f"  ✅ {r['sent']} отправлено  ❌ {r['failed']} ошибок\n"
                f"  <i>{escape_html((r['preview'] or '')[:50])}</i>\n"
            )
        kb_h = types.InlineKeyboardMarkup(row_width=2)
        nav = []
        if page > 0:
            nav.append(types.InlineKeyboardButton("◀️", callback_data=f"broadcast_history:{page-1}"))
        if page < total_pages - 1:
            nav.append(types.InlineKeyboardButton("▶️", callback_data=f"broadcast_history:{page+1}"))
        if nav: kb_h.row(*nav)
        kb_h.add(types.InlineKeyboardButton("◀️ Рассылка", callback_data="broadcast_menu"))
        bot.answer_callback_query(call.id)
        safe_edit("\n".join(lines) if rows else "<b>📋 История рассылок пуста</b>",
                  call.message.chat.id, call.message.message_id, reply_markup=kb_h)
        return

    # ── ГЛАВНОЕ МЕНЮ ─────────────────────────────────────────────
    if data == "main_menu":
        safe_edit(_hacker_banner(),
                  call.message.chat.id, call.message.message_id,
                  reply_markup=kb_main_menu())

    elif data == "account_info":
        if userbot_is_connected():
            _i = _userbot_info
            un_txt = ("  @" + str(_i.get("username",""))) if _i.get("username") else ""
            lines_acc = [
                "<code>┌─────────────────────────┐</code>",
                "<code>│  👤  А К К А У Н Т       │</code>",
                "<code>└─────────────────────────┘</code>",
                "",
                "📱 <b>+" + str(_i.get("phone","?")) + "</b>" + un_txt,
                "👤 " + str(_i.get("name","?")),
                "🆔 <code>" + str(_i.get("id","?")) + "</code>",
                "🟢 <b>Подключён и работает</b>",
            ]
            txt_a = "\n".join(lines_acc)
            kb_a  = types.InlineKeyboardMarkup()
            kb_a.add(types.InlineKeyboardButton("🔌 Отключить", callback_data="account_disconnect"))
        else:
            lines_acc = [
                "<code>┌─────────────────────────┐</code>",
                "<code>│  👤  А К К А У Н Т       │</code>",
                "<code>└─────────────────────────┘</code>",
                "",
                "🔴 <b>Userbot не подключён</b>",
                "",
                "<i>Без него недоступны:",
                "• Копирование истории",
                "• Парсинг участников",
                "• Live мониторинг</i>",
                "",
                "Войди через /account",
            ]
            txt_a = "\n".join(lines_acc)
            kb_a  = types.InlineKeyboardMarkup()
            kb_a.add(types.InlineKeyboardButton("▶️ Подключить (/account)", callback_data="account_start_auth"))
        kb_a.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
        safe_edit(txt_a, call.message.chat.id, call.message.message_id, reply_markup=kb_a)

    elif data == "account_info_hint":
        bot.answer_callback_query(call.id, "⚠️ Подключи userbot через /account!", show_alert=True)
        return

    # ── СПИСОК ПОЛЬЗОВАТЕЛЕЙ ─────────────────────────────────────
    elif data.startswith("users_list:"):
        index = int(data.split(":")[1])
        users = get_all_users()
        if not users:
            safe_edit("👥 Пользователей пока нет.",
                      call.message.chat.id, call.message.message_id, reply_markup=kb_back_menu())
            return
        index = max(0, min(index, len(users)-1))
        u_row = users[index]
        text  = fmt_user_card_short(u_row, index+1, len(users))
        safe_edit(text, call.message.chat.id, call.message.message_id,
                  reply_markup=kb_users_nav(index, len(users), u_row["id"]))

    # ── ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ (вкладки) ───────────────────────────
    elif data.startswith("uprofile:"):
        parts  = data.split(":")
        uid_p  = int(parts[1])
        tab    = parts[2]
        page   = int(parts[3]) if len(parts) > 3 else 0
        li     = int(parts[4]) if len(parts) > 4 else 0
        sub    = parts[5] if len(parts) > 5 else "all"
        per_p  = 6

        u      = get_user_by_id(uid_p)
        header = _user_header(uid_p, u)

        # ── INFO ──────────────────────────────────────────────────
        if tab == "info":
            if not u:
                bot.answer_callback_query(call.id, "Пользователь не найден"); return
            keys        = u.keys()
            phone_raw   = u["phone"]         if "phone"         in keys else None
            city        = u["city"]          if "city"          in keys else None
            country     = u["country"]       if "country"       in keys else None
            lang        = u["language_code"] if "language_code" in keys else None
            notes       = u["notes"]         if "notes"         in keys else None
            ban_reason  = u["ban_reason"]    if "ban_reason"    in keys else None
            first_seen  = u["first_seen"] or "—"
            last_seen   = u["last_seen"]  or "—"
            premium     = "⭐ Telegram Premium" if (u["is_premium"] if "is_premium" in keys else 0) else "—"
            banned_str  = f"\n🚫 <b>ЗАБАНЕН:</b> {ban_reason or 'без причины'}" \
                          if (u["is_banned"] if "is_banned" in keys else 0) else ""
            mc_m, mc_b  = get_user_message_count_split(uid_p)
            mc_str      = f"{mc_m+mc_b} (бот:{mc_m} basa:{mc_b})" if mc_b else str(mc_m+mc_b)
            lang_flag   = _LANG_FLAGS.get(lang or "", "")
            lang_str    = f"{lang_flag} {lang}" if lang_flag else lang or "—"

            if phone_raw:
                phone_line = f"📱 Телефон: <code>{phone_raw}</code>\n"
            else:
                ph_found = get_user_phones(uid_p)
                ph_in_msgs = [p for p in ph_found if p["source"] != "account"]
                if ph_in_msgs:
                    phone_line = f"📱 Телефон: скрыт · найден в сообщениях ({len(ph_in_msgs)}) → 📱\n"
                else:
                    phone_line = "📱 Телефон: <i>скрыт / не найден</i>\n"

            text = (
                header +
                f"📋 <b>Информация</b>\n\n"
                f"{phone_line}"
                f"🌆 Город: {city or '—'}  |  🌍 Страна: {country or '—'}\n"
                f"🗣 Язык: {lang_str}  |  ⭐ Premium: {premium}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 Первый контакт: <i>{fmt_date(first_seen)}</i>\n"
                f"🕐 Последняя активность: <i>{fmt_date(last_seen)}</i>\n"
                f"💬 Сообщений всего: <b>{mc_str}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📝 Заметка: {notes or '—'}"
                f"{banned_str}"
            )
            safe_edit(text, call.message.chat.id, call.message.message_id,
                      reply_markup=kb_profile_tabs(uid_p, "info", li))

        # ── PHONES ───────────────────────────────────────────────
        elif tab == "phones":
            ph_list = get_user_phones(uid_p)
            if not ph_list:
                text = header + "📱 <b>Номера</b>\n\n<i>Не найдено — телефон скрыт, в сообщениях номеров нет.</i>"
                safe_edit(text, call.message.chat.id, call.message.message_id,
                          reply_markup=kb_profile_tabs(uid_p, "phones", li))
                return
            total_ph    = len(ph_list)
            total_pg_ph = max(1, (total_ph + per_p - 1) // per_p)
            page        = max(0, min(page, total_pg_ph - 1))
            slice_ph    = ph_list[page * per_p:(page + 1) * per_p]
            src_icons   = {"account":"👤","message":"💬","basa":"📥"}
            src_labels  = {"account":"аккаунт (открытый)","message":"написал в чате","basa":"написал в basa"}
            lines = []
            for p in slice_ph:
                si   = src_icons.get(p["source"], "📌")
                sl   = src_labels.get(p["source"], p["source"])
                line = f"{si} <code>{p['phone']}</code>  <i>{sl}</i>"
                if p["message_id"] and p["chat_id"]:
                    url, is_pub = make_msg_link_safe(p["chat_id"], p["message_id"], p["chat_username"])
                    cname       = (p["chat_title"] or "?")[:20]
                    dstr        = fmt_date(p["timestamp"] or "")
                    go = f'<a href="{url}">перейти</a>' if is_pub else "🔒 приватная"
                    snippet     = (p["text"] or "")[:60].replace("<","&lt;").replace(">","&gt;")
                    line += f"\n   📍 <b>{cname}</b> · {go} · <i>{dstr}</i>"
                    if snippet:
                        line += f"\n   └ <i>{snippet}</i>"
                lines.append(line)
            pager = f"  стр. {page+1}/{total_pg_ph}" if total_pg_ph > 1 else ""
            text  = header + f"📱 <b>Номера</b> ({total_ph}){pager}\n\n" + "\n\n".join(lines)
            kb_ph = kb_profile_tabs(uid_p, "phones", li)
            if total_pg_ph > 1:
                nav = []
                if page > 0:
                    nav.append(types.InlineKeyboardButton("◀️", callback_data=f"uprofile:{uid_p}:phones:{page-1}:{li}"))
                if page < total_pg_ph - 1:
                    nav.append(types.InlineKeyboardButton("▶️", callback_data=f"uprofile:{uid_p}:phones:{page+1}:{li}"))
                if nav: kb_ph.row(*nav)
            safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb_ph)

        # ── CHATS ────────────────────────────────────────────────
        elif tab == "chats":
            chats        = get_user_chats(uid_p)
            mc_m, mc_b   = get_user_message_count_split(uid_p)
            if not chats:
                text = header + "💬 <b>Чаты</b>\n\n<i>Нет данных.</i>"
                safe_edit(text, call.message.chat.id, call.message.message_id,
                          reply_markup=kb_profile_tabs(uid_p, "chats", li))
                return
            lines = []
            for c in chats:
                cu     = c["chat_username"] if "chat_username" in c.keys() else None
                ct     = c["chat_title"] or "ЛС"
                src    = c["source"]    if "source"    in c.keys() else "main"
                icon   = "📥" if src == "basa" else "💬"
                with get_conn() as conn:
                    cnt_m = conn.execute("SELECT COUNT(*) as n FROM messages WHERE user_id=? AND chat_id=?",
                                         (uid_p, c["chat_id"])).fetchone()["n"]
                    cnt_b = conn.execute("SELECT COUNT(*) as n FROM basa_messages WHERE user_id=? AND chat_id=?",
                                         (uid_p, c["chat_id"])).fetchone()["n"]
                cnt_str = f"{cnt_m+cnt_b}"
                if cnt_b and cnt_m:
                    cnt_str = f"{cnt_m+cnt_b} (б:{cnt_m} / basa:{cnt_b})"
                if cu:
                    lines.append(f'{icon} <a href="https://t.me/{cu}">{ct}</a> · 💬 {cnt_str}')
                else:
                    lines.append(f"{icon} {ct} 🔒 · 💬 {cnt_str}")
            text = (
                header +
                f"💬 <b>Чаты</b> ({len(chats)})\n"
                f"бот: <b>{mc_m}</b>  basa: <b>{mc_b}</b>\n\n"
                + "\n".join(lines)
            )
            safe_edit(text, call.message.chat.id, call.message.message_id,
                      reply_markup=kb_profile_tabs(uid_p, "chats", li))

        # ── MSGS ─────────────────────────────────────────────────
        elif tab == "msgs":
            pp   = 6
            pg   = page
            with get_conn() as conn:
                if sub == "basa":
                    all_m = conn.execute("""
                        SELECT user_id, chat_id, chat_title, chat_username AS chat_uname,
                               text, content_type, message_id, timestamp, 'basa' AS src
                        FROM basa_messages WHERE user_id=? ORDER BY timestamp DESC
                    """, (uid_p,)).fetchall()
                elif sub == "main":
                    all_m = conn.execute("""
                        SELECT m.user_id, m.chat_id, m.chat_title,
                               c.username AS chat_uname,
                               m.text, m.content_type, m.message_id, m.timestamp, 'main' AS src
                        FROM messages m LEFT JOIN chats c ON c.id=m.chat_id
                        WHERE m.user_id=? ORDER BY m.timestamp DESC
                    """, (uid_p,)).fetchall()
                else:
                    mm = conn.execute("""
                        SELECT m.user_id, m.chat_id, m.chat_title,
                               c.username AS chat_uname,
                               m.text, m.content_type, m.message_id, m.timestamp, 'main' AS src
                        FROM messages m LEFT JOIN chats c ON c.id=m.chat_id
                        WHERE m.user_id=? ORDER BY m.timestamp DESC
                    """, (uid_p,)).fetchall()
                    bm = conn.execute("""
                        SELECT user_id, chat_id, chat_title, chat_username AS chat_uname,
                               text, content_type, message_id, timestamp, 'basa' AS src
                        FROM basa_messages WHERE user_id=? ORDER BY timestamp DESC
                    """, (uid_p,)).fetchall()
                    combined = list(mm) + list(bm)
                    combined.sort(key=lambda r: r["timestamp"] or "", reverse=True)
                    all_m = combined

            total_m  = len(all_m)
            total_pg = max(1, (total_m + pp - 1) // pp)
            pg       = max(0, min(pg, total_pg - 1))
            slice_m  = all_m[pg * pp:(pg + 1) * pp]
            u_obj    = get_user_by_id(uid_p)
            uname_s  = f"@{u_obj['username']}" if u_obj and u_obj["username"] else str(uid_p)
            sub_lbl  = {"all":"Все","main":"Бот","basa":"Basa"}.get(sub, "Все")
            lines_m  = []
            for m in slice_m:
                txt     = (m["text"] or "")[:70].replace("<","&lt;").replace(">","&gt;")
                cu      = m["chat_uname"] if "chat_uname" in m.keys() else None
                url, ip = make_msg_link_safe(m["chat_id"], m["message_id"], cu)
                icon    = ct_icon(m["content_type"])
                src_tag = " <i>[basa]</i>" if m["src"] == "basa" else ""
                cname   = (m["chat_title"] or "ЛС")[:20]
                lines_m.append(
                    f"{icon} <b>{cname}</b>{src_tag}\n"
                    f"   {fmt_msg_link(url, ip, m['timestamp'])}\n"
                    f"   └ {txt or '[медиа]'}"
                )
            body = "\n\n".join(lines_m) if lines_m else "<i>Нет сообщений.</i>"
            text = (
                header +
                f"📩 <b>Сообщения {uname_s}</b> [{sub_lbl}]  {pg+1}/{total_pg}  (всего {total_m})\n\n"
                + body
            )
            kb_m = kb_profile_tabs(uid_p, "msgs", li)
            sub_btns = []
            for s, sl in [("all","Все"),("main","🤖 Бот"),("basa","📥 Basa")]:
                prefix = "›" if s == sub else ""
                sub_btns.append(types.InlineKeyboardButton(
                    f"{prefix}{sl}", callback_data=f"uprofile:{uid_p}:msgs:0:{li}:{s}"))
            kb_m.row(*sub_btns)
            nav = []
            if pg > 0:
                nav.append(types.InlineKeyboardButton("◀️", callback_data=f"uprofile:{uid_p}:msgs:{pg-1}:{li}:{sub}"))
            if pg < total_pg - 1:
                nav.append(types.InlineKeyboardButton("▶️", callback_data=f"uprofile:{uid_p}:msgs:{pg+1}:{li}:{sub}"))
            if nav: kb_m.row(*nav)
            safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb_m)

        # ── ACTIVITY — новая вкладка "Активность" ────────────────
        elif tab == "activity":
            with get_conn() as conn:
                # По дням последние 14 дней
                daily = conn.execute("""
                    SELECT DATE(timestamp) as d, COUNT(*) as c
                    FROM (
                        SELECT timestamp FROM messages WHERE user_id=?
                        UNION ALL
                        SELECT timestamp FROM basa_messages WHERE user_id=?
                    ) AS _sub_daily
                    GROUP BY d ORDER BY d DESC LIMIT 14
                """, (uid_p, uid_p)).fetchall()

                # По чатам топ-5
                by_chat = conn.execute("""
                    SELECT chat_title, COUNT(*) as c FROM (
                        SELECT chat_title FROM messages WHERE user_id=?
                        UNION ALL
                        SELECT chat_title FROM basa_messages WHERE user_id=?
                    ) AS _sub_chat
                    GROUP BY chat_title ORDER BY c DESC LIMIT 5
                """, (uid_p, uid_p)).fetchall()

                # По типу контента
                by_type = conn.execute("""
                    SELECT content_type, COUNT(*) as c FROM (
                        SELECT content_type FROM messages WHERE user_id=?
                        UNION ALL
                        SELECT content_type FROM basa_messages WHERE user_id=?
                    ) AS _sub_type
                    GROUP BY content_type ORDER BY c DESC LIMIT 7
                """, (uid_p, uid_p)).fetchall()

            mc_m, mc_b = get_user_message_count_split(uid_p)
            total_act  = mc_m + mc_b

            # Мини-бар активности
            def bar(count, max_count):
                if not max_count:
                    return ""
                filled = round(count / max_count * 10)
                return "█" * filled + "░" * (10 - filled)

            max_daily = max((r["c"] for r in daily), default=1)

            daily_lines = ""
            for r in daily:
                d_str = fmt_date(r["d"] + " 00:00") if r["d"] else "?"
                b     = bar(r["c"], max_daily)
                daily_lines += f"  <code>{b}</code> <i>{d_str}</i> — {r['c']}\n"
            if not daily_lines:
                daily_lines = "  <i>Нет данных</i>\n"

            chat_lines = ""
            for r in by_chat:
                ct_name = (r["chat_title"] or "ЛС")[:22]
                b       = bar(r["c"], max((x["c"] for x in by_chat), default=1))
                chat_lines += f"  <code>{b}</code> {ct_name} — {r['c']}\n"
            if not chat_lines:
                chat_lines = "  <i>Нет данных</i>\n"

            type_lines = ""
            for r in by_type:
                icon = ct_icon(r["content_type"])
                type_lines += f"  {icon} {r['content_type'] or '?'}: <b>{r['c']}</b>\n"
            if not type_lines:
                type_lines = "  <i>Нет данных</i>\n"

            text = (
                header +
                f"📊 <b>Активность</b> · всего {total_act} сообщений\n\n"
                f"📅 <b>По дням (14 дн.):</b>\n{daily_lines}\n"
                f"💬 <b>Топ чатов:</b>\n{chat_lines}\n"
                f"🗂 <b>По типу:</b>\n{type_lines}"
            )
            safe_edit(text, call.message.chat.id, call.message.message_id,
                      reply_markup=kb_profile_tabs(uid_p, "activity", li))

    # ── СООБЩЕНИЯ ─────────────────────────────────────────────────
    elif data.startswith("user_msgs:"):
        _, target_id, page = data.split(":")
        target_id  = int(target_id)
        page       = int(page)
        per_page   = 5
        with get_conn() as conn:
            all_msgs = conn.execute(
                "SELECT * FROM messages WHERE user_id=? ORDER BY timestamp DESC",
                (target_id,)
            ).fetchall()
        total       = len(all_msgs)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page        = max(0, min(page, total_pages - 1))
        slice_      = all_msgs[page * per_page:(page + 1) * per_page]
        u_o         = get_user_by_id(target_id)
        uname       = f"@{u_o['username']}" if u_o and u_o["username"] else str(target_id)
        header      = f"📝 <b>Сообщения {uname}</b>\nСтр. {page+1}/{total_pages} (всего {total})\n\n"
        lines = []
        for m in slice_:
            txt = (m["text"] or "")[:80].replace("<","&lt;").replace(">","&gt;")
            lines.append(
                f"🕐 <i>{fmt_date(m['timestamp'])}</i>\n"
                f"📍 {m['chat_title'] or 'ЛС'}\n✉️ {txt or '[медиа]'}\n"
            )
        text = header + "\n".join(lines) if lines else header + "Нет сообщений"
        kb   = types.InlineKeyboardMarkup(row_width=3)
        prev_b = types.InlineKeyboardButton("◀️", callback_data=f"user_msgs:{target_id}:{page-1}") \
                 if page > 0 else types.InlineKeyboardButton("·", callback_data="noop")
        next_b = types.InlineKeyboardButton("▶️", callback_data=f"user_msgs:{target_id}:{page+1}") \
                 if page < total_pages-1 else types.InlineKeyboardButton("·", callback_data="noop")
        kb.add(prev_b, types.InlineKeyboardButton("👤 Профиль", callback_data=f"uprofile:{target_id}:info:0:0"), next_b)
        kb.add(types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"))
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

    # ── СТАТИСТИКА ────────────────────────────────────────────────
    elif data == "stats":
        s    = get_stats()
        ub   = "🟢 ONLINE" if userbot_is_connected() else "🔴 OFFLINE"
        live = s.get("live", 0)
        pb_a = len([t for t in _active_partner_bots.values() if t.is_alive()])
        # UDB info
        with get_conn() as _sc:
            udb_cnt  = _sc.execute("SELECT COUNT(*) FROM uploaded_databases").fetchone()[0]
            udb_rows = _sc.execute("SELECT COALESCE(SUM(total_rows),0) FROM uploaded_databases").fetchone()[0]
        top = ""
        for i, row in enumerate(s.get("top_users",[]), 1):
            row  = dict(row)
            un_  = f"@{row['username']}" if row.get("username") else ""
            nm_  = escape_html((row.get("first_name") or "—")[:16])
            top += f"  {i}. {nm_} {un_} — <b>{row.get('cnt',0):,}</b>\n"
        import datetime as _dt_s
        now_str = _dt_s.datetime.now().strftime("%d.%m.%Y %H:%M")
        parts = [
            "<code>╔══════════════════════════════╗</code>",
            "<code>║   📊  С Т А Т И С Т И К А   ║</code>",
            "<code>╚══════════════════════════════╝</code>",
            f"<i>Обновлено: {now_str}</i>",
            "",
            "<b>👥 ЛЮДИ</b>",
            f"  Всего пользователей:  <b>{s.get('users',0):,}</b>",
            f"  С телефоном:          <b>{s.get('phones',0):,}</b>",
            f"  Участников чатов:     <b>{s.get('members',0):,}</b>",
            f"  Забанено:             <b>{s.get('banned',0):,}</b>",
            f"  Новых сегодня:        <b>{s.get('today_users',0):,}</b>",
            "",
            "<b>💬 ДАННЫЕ</b>",
            f"  Сообщений (бот):      <b>{s.get('msgs',0):,}</b>",
            f"  Basa messages:        <b>{s.get('basa_msgs',0):,}</b>",
            f"  Сообщений сегодня:    <b>{s.get('today_msgs',0):,}</b>",
            "",
            "<b>🗂 ЧАТЫ И СЕССИИ</b>",
            f"  Чатов:                <b>{s.get('chats',0):,}</b>",
            f"  Basa-сессий:          <b>{s.get('basa_sess',0):,}</b>",
            f"  Live активных:        <b>{live:,}</b>",
            "",
            "<b>🗄 ЗАГРУЖЕННЫЕ БАЗЫ</b>",
            f"  Баз загружено:        <b>{udb_cnt:,}</b>",
            f"  Записей в базах:      <b>{udb_rows:,}</b>",
            "",
            "<b>🤖 СИСТЕМА</b>",
            f"  Userbot:              {ub}",
            f"  Партнёрских ботов:    <b>{pb_a}/{s.get('pbots',0)}</b>",
            "",
        ]
        if top:
            parts += ["<b>🏆 ТОП ПО СООБЩЕНИЯМ</b>", top]
        text = "\n".join(parts)
        kb_s = types.InlineKeyboardMarkup(row_width=2)
        kb_s.row(
            types.InlineKeyboardButton("🔄 Обновить", callback_data="stats"),
            types.InlineKeyboardButton("🏠 Меню",     callback_data="main_menu"),
        )
        kb_s.row(
            types.InlineKeyboardButton("🗄 Базы данных", callback_data="udb_list"),
            types.InlineKeyboardButton("👥 Пользователи", callback_data="users_list:0"),
        )
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb_s)

    # ── НАСТРОЙКИ ─────────────────────────────────────────────────
    elif data == "settings":
        anon = get_setting("anonymous_mode") == "1"
        log  = get_setting("logging_enabled") == "1"
        fwd  = get_setting("forward_to_admin") == "1"
        ban  = get_setting("ban_block") == "1"
        def _sw(v): return "✅ ВКЛ" if v else "❌ ВЫКЛ"
        text = (
            f"<code>╔══════════════════════════════╗</code>\n"
            f"<code>║   ⚙️  Н А С Т Р О Й К И     ║</code>\n"
            f"<code>╚══════════════════════════════╝</code>\n\n"
            f"🕵️ <b>Анонимный режим</b>: {_sw(anon)}\n"
            f"  <i>Скрывает ID бота при пересылке сообщений</i>\n\n"
            f"📝 <b>Логирование</b>: {_sw(log)}\n"
            f"  <i>Сохранять все сообщения в базу</i>\n\n"
            f"📨 <b>Пересылка adminу</b>: {_sw(fwd)}\n"
            f"  <i>Копировать входящие сообщения администратору</i>\n\n"
            f"🚫 <b>Блокировка банов</b>: {_sw(ban)}\n"
            f"  <i>Удалять сообщения от забаненных пользователей</i>"
        )
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb_settings())

    elif data == "restart_all_bots":
        bot.answer_callback_query(call.id, "🔄 Перезапускаю всех ботов...")
        safe_edit(
            "<code>┌──────────────────────────┐</code>\n"
            "<code>│  🔄  ПЕРЕЗАПУСК БОТОВ    │</code>\n"
            "<code>└──────────────────────────┘</code>\n\n"
            "⏳ <b>Останавливаю и перезапускаю всех партнёрских ботов...</b>\n"
            "<i>Подожди 3–5 секунд</i>",
            call.message.chat.id, call.message.message_id
        )
        def _do_restart():
            stats = _restart_all_partner_bots()
            names_str = "\n".join(f"  ✅ {n}" for n in stats["names"]) if stats["names"] else "  <i>нет</i>"
            err_str   = f"\n⚠️ Ошибок: <b>{stats['errors']}</b>" if stats["errors"] else ""
            try:
                safe_edit(
                    "<code>┌──────────────────────────┐</code>\n"
                    "<code>│  🔄  ПЕРЕЗАПУСК БОТОВ    │</code>\n"
                    "<code>└──────────────────────────┘</code>\n\n"
                    f"🛑 Остановлено: <b>{stats['stopped']}</b>\n"
                    f"🚀 Запущено: <b>{stats['launched']}</b>\n"
                    f"{err_str}\n\n"
                    f"<b>Боты:</b>\n{names_str}",
                    call.message.chat.id, call.message.message_id,
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
                    )
                )
            except Exception:
                pass
        threading.Thread(target=_do_restart, daemon=True).start()

    elif data.startswith("toggle:"):
        key     = data.split(":")[1]
        new_val = "0" if get_setting(key) == "1" else "1"
        set_setting(key, new_val)
        labels  = {"anonymous_mode":"Анон. режим","logging_enabled":"Логирование",
                   "forward_to_admin":"Пересылка","ban_block":"Блок банов"}
        bot.answer_callback_query(call.id, f"{'✅ Вкл' if new_val=='1' else '❌ Выкл'}: {labels.get(key,key)}")
        # Перерисовываем
        call.data = "settings"
        handle_callback(call)
        return

    # ── ПОИСК ────────────────────────────────────────────────────
    elif data == "search_prompt":
        user_states[uid] = "search"
        with get_conn() as conn:
            u_cnt  = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            cm_cnt = conn.execute("SELECT COUNT(*) FROM chat_members").fetchone()[0]
            bm_cnt = conn.execute("SELECT COUNT(DISTINCT user_id) FROM basa_messages WHERE user_id IS NOT NULL AND user_id!=0").fetchone()[0]
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔍  П О И С К           │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"📊 Доступно в базе:\n"
            f"  👥 Пользователей: <b>{u_cnt:,}</b>\n"
            f"  👤 Участников: <b>{cm_cnt:,}</b>\n"
            f"  📥 Basa: <b>{bm_cnt:,}</b>\n\n"
            f"Введи запрос:\n"
            f"  • Имя или фамилию (напр. <code>Адольф</code>)\n"
            f"  • <code>@username</code>\n"
            f"  • <code>123456789</code> — Telegram ID\n"
            f"  • <code>+79001234567</code> — телефон\n\n"
            f"<i>Поиск по всем источникам: users, chat_members, basa</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
            )
        )

    # ── ЧАТЫ ─────────────────────────────────────────────────────
    elif data == "chats_list":
        with get_conn() as conn:
            chats = conn.execute("SELECT * FROM chats ORDER BY last_activity DESC").fetchall()
        if not chats:
            safe_edit("🗂 Бот ещё не добавлен ни в одну группу.",
                      call.message.chat.id, call.message.message_id, reply_markup=kb_back_menu())
            return
        kb  = types.InlineKeyboardMarkup(row_width=1)
        lines_t = []
        for c in chats:
            un   = c["username"] or ""
            link = f'<a href="https://t.me/{un}">{c["title"]}</a>' if un else f"<b>{c['title']}</b>"
            mem  = f" · 👥{c['member_count']}" if c["member_count"] else ""
            lines_t.append(f"• {link}\n  🆔 <code>{c['id']}</code> | {c['chat_type']}{mem}")
            kb.add(types.InlineKeyboardButton(f"📂 {(c['title'] or '')[:30]}", callback_data=f"chat_view:{c['id']}"))
        kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
        safe_edit(
            f"🗂 <b>Группы</b> ({len(chats)}):\n\n" + "\n\n".join(lines_t),
            call.message.chat.id, call.message.message_id, reply_markup=kb
        )

    elif data == "notif_chats_list":
        # Поддержка пагинации: notif_chats_list или notif_chats_list:page
        page_n = 0
        if ":" in data:
            try: page_n = int(data.split(":")[1])
            except: pass

        PER_PAGE = 8
        with get_conn() as conn:
            chats = conn.execute("""
                SELECT c.id, c.title, c.username, c.chat_type, c.member_count,
                    ns.muted, ns.muted_until,
                    (SELECT COUNT(*) FROM messages     WHERE chat_id=c.id) as msg_cnt,
                    (SELECT COUNT(*) FROM basa_messages WHERE chat_id=c.id) as basa_cnt,
                    (SELECT COUNT(*) FROM chat_members  WHERE chat_id=c.id) as cm_cnt,
                    (SELECT session_id FROM basa_sessions WHERE chat_id=c.id AND is_live=1 LIMIT 1) as live_sid
                FROM chats c
                LEFT JOIN chat_notif_settings ns ON ns.chat_id=c.id
                ORDER BY c.last_activity DESC
            """).fetchall()
        if not chats:
            safe_edit("🔔 Нет чатов.", call.message.chat.id, call.message.message_id,
                      reply_markup=kb_back_menu())
            return

        total   = len(chats)
        pages   = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page_n  = max(0, min(page_n, pages - 1))
        slice_c = chats[page_n * PER_PAGE : (page_n + 1) * PER_PAGE]

        now_str  = datetime.datetime.now().isoformat(timespec="seconds")
        live_cnt = sum(1 for c in chats if c["live_sid"])
        muted_cnt= sum(1 for c in chats if c["muted"])

        kb_n = types.InlineKeyboardMarkup(row_width=1)
        for c in slice_c:
            title = (c["title"] or str(c["id"]))[:28]
            muted = c["muted"]
            mut_until = c["muted_until"] or ""
            if muted:
                if mut_until and mut_until > now_str:
                    ico = "🔇"
                else:
                    ico = "🔕"
            else:
                ico = "🔔"
            live_ico = " 🔴" if c["live_sid"] else ""
            kb_n.add(types.InlineKeyboardButton(
                f"{ico}{live_ico} {title}  💬{c['msg_cnt']} 📥{c['basa_cnt']}",
                callback_data=f"chat_view:{c['id']}"
            ))

        # Пагинация
        nav = []
        if page_n > 0:
            nav.append(types.InlineKeyboardButton("◀️", callback_data=f"notif_chats_list:{page_n-1}"))
        nav.append(types.InlineKeyboardButton(f"{page_n+1}/{pages}", callback_data="noop"))
        if page_n < pages - 1:
            nav.append(types.InlineKeyboardButton("▶️", callback_data=f"notif_chats_list:{page_n+1}"))
        if nav:
            kb_n.row(*nav)
        kb_n.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        safe_edit(
            f"<code>┌──────────────────────────────┐</code>\n"
            f"<code>│  🔔  ЧАТЫ                    │</code>\n"
            f"<code>└──────────────────────────────┘</code>\n"
            f"Всего: <b>{total}</b>  |  🔴 Live: <b>{live_cnt}</b>  |  🔕 Мут: <b>{muted_cnt}</b>\n"
            f"<i>Нажми на чат чтобы управлять уведомлениями</i>",
            call.message.chat.id, call.message.message_id, reply_markup=kb_n
        )

    elif data.startswith("chat_view:"):
        chat_id = int(data.split(":")[1])
        with get_conn() as conn:
            c        = conn.execute("SELECT * FROM chats WHERE id=?", (chat_id,)).fetchone()
            mc       = conn.execute("SELECT COUNT(*) as n FROM messages WHERE chat_id=?", (chat_id,)).fetchone()["n"]
            # Участников из таблицы chat_members (реальный парсинг)
            cm_cnt   = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=?", (chat_id,)).fetchone()["n"]
            adm_cnt  = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=? AND is_admin=1", (chat_id,)).fetchone()["n"]
            bot_cnt  = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=? AND is_bot=1", (chat_id,)).fetchone()["n"]
            ph_cnt   = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=? AND phone IS NOT NULL", (chat_id,)).fetchone()["n"]
            top_u    = conn.execute("""
                SELECT u.first_name, u.username, u.id, COUNT(m.id) as c
                FROM messages m JOIN users u ON u.id=m.user_id WHERE m.chat_id=?
                GROUP BY m.user_id, u.first_name, u.username, u.id ORDER BY c DESC LIMIT 5
            """, (chat_id,)).fetchall()
            task     = conn.execute("SELECT * FROM scrape_tasks WHERE chat_id=?", (chat_id,)).fetchone()
        if not c:
            bot.answer_callback_query(call.id, "Чат не найден"); return
        un   = c["username"] or ""
        link = f'<a href="https://t.me/{un}">{c["title"]}</a>' if un else f"<b>{c['title']}</b>"
        top_t = ""
        for i, r in enumerate(top_u, 1):
            tl  = f'<a href="tg://user?id={r["id"]}">{r["first_name"] or "?"}</a>'
            un2 = f"@{r['username']}" if r["username"] else ""
            top_t += f"  {i}. {tl} {un2} — {r['c']} сообщ.\n"
        # Статус парсинга
        if task and task["status"] == "running":
            scrape_info = f"\n⚙️ <i>Парсинг идёт... собрано {task['done']}</i>"
            scrape_btn  = "🔄 Обновить статус"
            scrape_cb   = f"chat_view:{chat_id}"
        elif task and task["status"] == "done":
            fa = (task["finished_at"] or "")[:16]
            scrape_info = f"\n✅ <i>Парсинг завершён: {fa}</i>"
            scrape_btn  = "🔄 Перепарсить"
            scrape_cb   = f"chat_scrape:{chat_id}"
        elif task and task["status"] == "error":
            scrape_info = f"\n❌ <i>Ошибка парсинга: {(task['error_msg'] or '')[:60]}</i>"
            scrape_btn  = "🔄 Повторить парсинг"
            scrape_cb   = f"chat_scrape:{chat_id}"
        else:
            scrape_info = ""
            scrape_btn  = "⚙️ Спарсить участников"
            scrape_cb   = f"chat_scrape:{chat_id}"

        members_line = f"👥 Спарсено: <b>{cm_cnt}</b>"
        if cm_cnt > 0:
            members_line += f" (👑 {adm_cnt} админ · 🤖 {bot_cnt} бот · 📱 {ph_cnt} с телефоном)"

        # Статус уведомлений
        now_s2 = datetime.datetime.now().isoformat(timespec="seconds")
        with get_conn() as conn:
            ns2      = conn.execute("SELECT * FROM chat_notif_settings WHERE chat_id=?", (chat_id,)).fetchone()
            basa_cnt = conn.execute("SELECT COUNT(*) FROM basa_messages WHERE chat_id=?", (chat_id,)).fetchone()[0]
            sess_cnt = conn.execute("SELECT COUNT(*) FROM basa_sessions WHERE chat_id=?", (chat_id,)).fetchone()[0]
            live_sid = conn.execute("SELECT session_id FROM basa_sessions WHERE chat_id=? AND is_live=1 LIMIT 1", (chat_id,)).fetchone()
        is_muted = ns2 and ns2["muted"]
        if is_muted:
            mut_until = (ns2["muted_until"] or "") if ns2 else ""
            if mut_until and mut_until > now_s2:
                notif_label = f"🔇 Мут до {mut_until[:16]}"
            else:
                notif_label = "🔕 Уведомления отключены"
        else:
            notif_label = "🔔 Уведомления включены"

        kb2 = types.InlineKeyboardMarkup(row_width=2)
        kb2.add(
            types.InlineKeyboardButton("📩 Сообщения",  callback_data=f"chat_msgs:{chat_id}:0"),
            types.InlineKeyboardButton("👥 Участники",  callback_data=f"chat_members:{chat_id}:0:all"),
        )
        ub_ok = userbot_is_connected()
        kb2.add(types.InlineKeyboardButton(
            scrape_btn if ub_ok else "⚠️ Userbot не подключён",
            callback_data=scrape_cb if ub_ok else "account_info_hint"
        ))
        if ub_ok:
            live_lbl = "⏹ Стоп Live" if live_sid else "🔴 Запустить Live"
            live_cb  = f"basa_stop:{live_sid['session_id']}:{chat_id}" if live_sid else f"chat_live_quick:{chat_id}"
            kb2.add(types.InlineKeyboardButton(live_lbl, callback_data=live_cb))
        # Кнопки уведомлений
        kb2.add(types.InlineKeyboardButton("─── 🔔 Уведомления ───", callback_data="noop"))
        if is_muted:
            kb2.row(
                types.InlineKeyboardButton("🔔 Включить уведомления", callback_data=f"notif_mute:{chat_id}:off"),
            )
        else:
            kb2.row(
                types.InlineKeyboardButton("🔕 Выкл. навсегда",  callback_data=f"notif_mute:{chat_id}:forever"),
                types.InlineKeyboardButton("🔇 Мут 1ч",          callback_data=f"notif_mute:{chat_id}:1h"),
            )
            kb2.row(
                types.InlineKeyboardButton("🔇 Мут 24ч",         callback_data=f"notif_mute:{chat_id}:24h"),
            )
        if un:
            kb2.add(types.InlineKeyboardButton(f"🌐 @{un}", url=f"https://t.me/{un}"))
        kb2.row(
            types.InlineKeyboardButton("◀️ К чатам",       callback_data="notif_chats_list"),
            types.InlineKeyboardButton("🏠 Меню",           callback_data="main_menu"),
        )
        safe_edit(
            f"<code>┌──────────────────────────────┐</code>\n"
            f"<code>│  📂  ГРУППА                  │</code>\n"
            f"<code>└──────────────────────────────┘</code>\n"
            f"\n📂 <b>{escape_html(c['title'] or '?')}</b>\n"
            f"🆔 <code>{c['id']}</code>  |  {c['chat_type']}\n"
            + (f"🔗 {link}\n" if un else "")
            + (f"📝 <i>{escape_html((c['description'] or '')[:100])}</i>\n" if c['description'] else "")
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💬 Сообщений: <b>{mc}</b>  |  📥 Basa: <b>{basa_cnt}</b>\n"
            f"{members_line}{scrape_info}\n"
            f"📊 Basa-сессий: <b>{sess_cnt}</b>  |  {'🔴 Live активен' if live_sid else '⚪ Live выключен'}\n"
            f"📅 Добавлен: {(c['joined_at'] or '')[:16]}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{notif_label}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 Топ активных:\n{top_t or '  нет данных'}",
            call.message.chat.id, call.message.message_id, reply_markup=kb2
        )

    elif data.startswith("chat_scrape:"):
        chat_id = int(data.split(":")[1])
        if not userbot_is_connected():
            bot.answer_callback_query(call.id, "⚠️ Userbot не подключён — войди в /account", show_alert=True)
            return
        with get_conn() as conn:
            task = conn.execute("SELECT status FROM scrape_tasks WHERE chat_id=?", (chat_id,)).fetchone()
        if task and task["status"] == "running":
            bot.answer_callback_query(call.id, "⚙️ Парсинг уже идёт!", show_alert=True)
            return
        bot.answer_callback_query(call.id, "⚙️ Запускаю парсинг...")
        # Ставим статус running сразу
        now_s = datetime.datetime.now().isoformat(timespec="seconds")
        with db_lock, get_conn() as conn:
            conn.execute("""
                INSERT INTO scrape_tasks (chat_id, status, done, total, started_at)
                VALUES (?,'running',0,0,?)
                ON CONFLICT(chat_id) DO UPDATE SET status='running',done=0,started_at=?,error_msg=NULL
            """, (chat_id, now_s, now_s))
        safe_edit(
            f"⚙️ <b>Запускаю сбор участников...</b>\n<i>Это может занять несколько минут.</i>\n"
            f"<i>Userbot парсит всех через Telegram API.</i>",
            call.message.chat.id, call.message.message_id
        )
        scrape_chat_members(chat_id, call.message.chat.id, call.message.message_id)

    elif data.startswith("chat_msgs:"):
        parts   = data.split(":")
        chat_id = int(parts[1])
        page    = int(parts[2])
        per_p   = 5
        with get_conn() as conn:
            all_m = conn.execute("""
                SELECT m.*, u.username, u.first_name FROM messages m
                LEFT JOIN users u ON u.id=m.user_id
                WHERE m.chat_id=? ORDER BY m.timestamp DESC
            """, (chat_id,)).fetchall()
            c     = conn.execute("SELECT * FROM chats WHERE id=?", (chat_id,)).fetchone()
        total       = len(all_m)
        total_pages = max(1, (total + per_p - 1) // per_p)
        page        = max(0, min(page, total_pages - 1))
        cu          = c["username"] if c else None
        title_c     = (c["title"] if c else "Группа") or "Группа"
        header      = f"💬 <b>{title_c}</b>\nСтр. {page+1}/{total_pages} (всего {total})\n\n"
        lines_t = []
        for m in all_m[page * per_p:(page + 1) * per_p]:
            txt    = (m["text"] or "")[:60].replace("<","&lt;").replace(">","&gt;")
            url, ip= make_msg_link_safe(chat_id, m["message_id"], cu)
            fname  = m["first_name"] or "?"
            un     = f"@{m['username']}" if m["username"] else f"id{m['user_id']}"
            u_link = f'<a href="tg://user?id={m["user_id"]}">{fname}</a>'
            icon   = ct_icon(m["content_type"])
            lines_t.append(f'{icon} {u_link} ({un})\n   {fmt_msg_link(url, ip, m["timestamp"])} — {txt or "[медиа]"}')
        text = header + "\n\n".join(lines_t) if lines_t else header + "Нет сообщений"
        kb   = types.InlineKeyboardMarkup(row_width=3)
        pb   = types.InlineKeyboardButton("◀️", callback_data=f"chat_msgs:{chat_id}:{page-1}") \
               if page > 0 else types.InlineKeyboardButton("·", callback_data="noop")
        nb   = types.InlineKeyboardButton("▶️", callback_data=f"chat_msgs:{chat_id}:{page+1}") \
               if page < total_pages-1 else types.InlineKeyboardButton("·", callback_data="noop")
        kb.add(pb, types.InlineKeyboardButton("◀️ Группа", callback_data=f"chat_view:{chat_id}"), nb)
        kb.add(types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"))
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data.startswith("chat_members:"):
        # chat_members:{chat_id}:{page}:{filter}   filter: all|admins|bots|phones
        parts    = data.split(":")
        chat_id  = int(parts[1])
        page     = int(parts[2])
        flt      = parts[3] if len(parts) > 3 else "all"
        per_p    = 10

        with get_conn() as conn:
            c = conn.execute("SELECT * FROM chats WHERE id=?", (chat_id,)).fetchone()
            # Фильтрация
            where = "WHERE cm.chat_id=?"
            params = [chat_id]
            if flt == "admins":
                where += " AND cm.is_admin=1"
            elif flt == "bots":
                where += " AND cm.is_bot=1"
            elif flt == "phones":
                where += " AND cm.phone IS NOT NULL"
            members = conn.execute(f"""
                SELECT cm.*, u.is_premium, u.is_banned as u_banned,
                       (SELECT COUNT(*) FROM messages m WHERE m.user_id=cm.user_id AND m.chat_id=cm.chat_id) as msg_cnt
                FROM chat_members cm
                LEFT JOIN users u ON u.id=cm.user_id
                {where}
                ORDER BY cm.is_admin DESC, msg_cnt DESC
            """, params).fetchall()
            total_all    = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=?", (chat_id,)).fetchone()["n"]
            total_admins = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=? AND is_admin=1", (chat_id,)).fetchone()["n"]
            total_bots   = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=? AND is_bot=1", (chat_id,)).fetchone()["n"]
            total_phones = conn.execute("SELECT COUNT(*) as n FROM chat_members WHERE chat_id=? AND phone IS NOT NULL", (chat_id,)).fetchone()["n"]

        title_c     = (c["title"] if c else "Группа") or "Группа"
        total       = len(members)
        total_pages = max(1, (total + per_p - 1) // per_p)
        page        = max(0, min(page, total_pages - 1))
        slice_      = members[page*per_p:(page+1)*per_p]

        filter_labels = {"all": f"Все ({total_all})", "admins": f"👑 Админы ({total_admins})",
                         "bots": f"🤖 Боты ({total_bots})", "phones": f"📱 С тел. ({total_phones})"}
        header = (
            f"👥 <b>{title_c}</b>\n"
            f"Фильтр: <b>{filter_labels.get(flt, flt)}</b> · Стр. {page+1}/{total_pages}\n\n"
        )
        lines_t = []
        for u in slice_:
            fn    = u["first_name"] or "?"
            un    = f"@{u['username']}" if u["username"] else "—"
            uid_m = u["user_id"]
            tl    = f'<a href="tg://user?id={uid_m}">{fn}</a>'
            badges = ""
            if u["is_admin"]:  badges += " 👑"
            if u["is_bot"]:    badges += " 🤖"
            if u["u_banned"]:  badges += " 🚫"
            if u["is_premium"]: badges += " ⭐"
            ph_str = f" · 📱<code>{u['phone']}</code>" if u["phone"] else ""
            mc_str = f" · 💬{u['msg_cnt']}" if u["msg_cnt"] else ""
            lines_t.append(f"{tl}{badges} {un}{ph_str}{mc_str}")

        text = header + "\n".join(lines_t) if lines_t else header + (
            "Нет участников.\n<i>Нажми ⚙️ Спарсить участников в карточке группы.</i>" if total_all == 0
            else "Нет совпадений по фильтру."
        )

        kb = types.InlineKeyboardMarkup(row_width=4)
        # Кнопки фильтров
        def _fb(label, f):
            mark = "▪️" if f == flt else ""
            return types.InlineKeyboardButton(f"{mark}{label}", callback_data=f"chat_members:{chat_id}:0:{f}")
        kb.row(_fb(f"Все", "all"), _fb("👑", "admins"), _fb("🤖", "bots"), _fb("📱", "phones"))
        # Навигация
        pb = types.InlineKeyboardButton("◀️", callback_data=f"chat_members:{chat_id}:{page-1}:{flt}") \
             if page > 0 else types.InlineKeyboardButton("·", callback_data="noop")
        nb = types.InlineKeyboardButton("▶️", callback_data=f"chat_members:{chat_id}:{page+1}:{flt}") \
             if page < total_pages-1 else types.InlineKeyboardButton("·", callback_data="noop")
        kb.row(pb, types.InlineKeyboardButton("◀️ Группа", callback_data=f"chat_view:{chat_id}"), nb)
        if userbot_is_connected():
            kb.add(types.InlineKeyboardButton("⚙️ Обновить парсинг", callback_data=f"chat_scrape:{chat_id}"))
        kb.add(types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"))
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

    # ── БАН / РАЗБАН ─────────────────────────────────────────────
    elif data.startswith("ban_prompt:"):
        target_id = int(data.split(":")[1])
        if target_id in ADMIN_IDS:
            bot.answer_callback_query(call.id, "⛔ Нельзя забанить администратора!", show_alert=True); return
        user_states[uid] = f"ban:{target_id}"
        u = get_user_by_id(target_id)
        n = (u["first_name"] or str(target_id)) if u else str(target_id)
        un= f"@{u['username']}" if u and u["username"] else ""
        safe_edit(
            f"🚫 <b>Бан</b>\n👤 {n} {un} | <code>{target_id}</code>\n\nВведи причину (или «-»):",
            call.message.chat.id, call.message.message_id, reply_markup=kb_back_menu()
        )

    elif data.startswith("unban:"):
        target_id = int(data.split(":")[1])
        unban_user(target_id, unbanned_by=uid)
        log_event("unban", user_id=target_id, description="Разбанен")
        bot.answer_callback_query(call.id, "✅ Разбанен")
        call.data = f"uprofile:{target_id}:info:0:0"
        handle_callback(call)
        return

    elif data.startswith("note_prompt:"):
        target_id = int(data.split(":")[1])
        user_states[uid] = f"note:{target_id}"
        safe_edit("📝 <b>Заметка</b>\n\nВведи текст заметки:",
                  call.message.chat.id, call.message.message_id, reply_markup=kb_back_menu())

    # ── 🕵️ ПРОБИВ ПО БАЗЕ ────────────────────────────────────────
    elif data == "probiv_prompt":
        user_states[uid] = "probiv"
        with get_conn() as conn:
            u_cnt   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            cm_cnt  = conn.execute("SELECT COUNT(*) FROM chat_members").fetchone()[0]
            bm_cnt  = conn.execute("SELECT COUNT(*) FROM basa_messages").fetchone()[0]
            udb_cnt = conn.execute("SELECT COUNT(*) FROM uploaded_databases").fetchone()[0]
            udb_r   = conn.execute("SELECT COALESCE(SUM(total_rows),0) FROM uploaded_databases").fetchone()[0]
        safe_edit(
            f"<code>╔══════════════════════════════╗</code>\n"
            f"<code>║     🕵️  П Р О Б И В         ║</code>\n"
            f"<code>╚══════════════════════════════╝</code>\n\n"
            f"<b>Базы для поиска:</b>\n"
            f"  👥 Пользователей:  <code>{u_cnt:,}</code>\n"
            f"  👤 Участников:     <code>{cm_cnt:,}</code>\n"
            f"  📥 Basa:           <code>{bm_cnt:,}</code>\n"
            f"  🗄 Загруж. баз:    <code>{udb_cnt}</code>  ({udb_r:,} записей)\n\n"
            f"<b>Введи что знаешь о цели:</b>\n"
            f"  📱 Телефон:   <code>+79001234567</code>\n"
            f"  🔗 Username:  <code>@username</code>\n"
            f"  🆔 TG ID:     <code>123456789</code>\n"
            f"  📛 Имя:       <code>Иван Петров</code>\n\n"
            f"<i>Поиск идёт по всем источникам и загруженным базам</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                types.InlineKeyboardButton("🗄 Поиск по базам", callback_data="udb_search_all"),
                types.InlineKeyboardButton("🏠 Главное меню",   callback_data="main_menu"),
            )
        )

    elif data.startswith("probiv_show:"):
        target_raw = data.split(":", 1)[1]
        target_id  = int(target_raw) if target_raw.lstrip("-").isdigit() else None
        target_un  = None if target_id else target_raw.lstrip("@")
        bot.answer_callback_query(call.id, "🔍 Пробиваю...")
        result = _probiv_user(target_id, target_un)
        _send_probiv_card(call.message.chat.id, call.message.message_id, result, target_raw, edit=True)

    # ── УВЕДОМЛЕНИЯ — мут чата ────────────────────────────────────
    elif data.startswith("notif_mute:"):
        parts   = data.split(":")
        cid_n   = int(parts[1])
        mode    = parts[2]  # "forever" | "24h" | "1h" | "off"
        now_s   = datetime.datetime.now().isoformat(timespec="seconds")
        if mode == "off":
            with db_lock, get_conn() as conn:
                conn.execute("INSERT OR REPLACE INTO chat_notif_settings (chat_id, muted, muted_until) VALUES (?,0,NULL)", (cid_n,))
            bot.answer_callback_query(call.id, "🔔 Уведомления включены")
        elif mode == "forever":
            with db_lock, get_conn() as conn:
                conn.execute("INSERT OR REPLACE INTO chat_notif_settings (chat_id, muted, muted_until) VALUES (?,1,NULL)", (cid_n,))
            bot.answer_callback_query(call.id, "🔕 Уведомления отключены навсегда")
        elif mode == "24h":
            until = (datetime.datetime.now() + datetime.timedelta(hours=24)).isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute("INSERT OR REPLACE INTO chat_notif_settings (chat_id, muted, muted_until) VALUES (?,1,?)", (cid_n, until))
            bot.answer_callback_query(call.id, "🔇 Мут на 24 часа")
        elif mode == "1h":
            until = (datetime.datetime.now() + datetime.timedelta(hours=1)).isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute("INSERT OR REPLACE INTO chat_notif_settings (chat_id, muted, muted_until) VALUES (?,1,?)", (cid_n, until))
            bot.answer_callback_query(call.id, "🔇 Мут на 1 час")
        # Обновляем — редиректим обратно в chat_view с обновлёнными данными
        call.data = f"chat_view:{cid_n}"
        handle_callback(call)

    elif data.startswith("chat_live_quick:"):
        cid_q = int(data.split(":")[1])
        if cid_q in _active_basa_sessions:
            bot.answer_callback_query(call.id, "⏹ Live уже запущен для этого чата", show_alert=True)
        else:
            # Создаём быструю сессию basa и запускаем live
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            sid   = f"live_{cid_q}_{int(datetime.datetime.now().timestamp())}"
            with db_lock, get_conn() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO basa_sessions
                    (session_id, chat_id, chat_title, started_at, total_messages, is_live)
                    SELECT ?, id, title, ?, 0, 1 FROM chats WHERE id=?
                """, (sid, now_s, cid_q))
            _live_set(cid_q, sid)
            bot.answer_callback_query(call.id, "🔴 Live запущен!", show_alert=True)

    # ── ПАРТНЁРЫ — главная панель ─────────────────────────────────
    # ── ПАРТНЁР — пользовательские callback'и ────────────────────
    elif data.startswith("p_probiv_prompt:"):
        pb_id = int(data.split(":")[1])
        ok, msg_err = _check_plan_limit(pb_id, "probiv")
        if not ok:
            bot.answer_callback_query(call.id, msg_err, show_alert=True)
            return
        user_states[uid] = f"p_probiv:{pb_id}"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔍  П Р О Б И В         │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Введи что хочешь найти:\n"
            f"  • <code>@username</code> — ник пользователя\n"
            f"  • <code>123456789</code> — Telegram ID\n"
            f"  • <code>+79001234567</code> — номер телефона\n\n"
            f"<i>Покажу: имя, группы, активность, телефон\n"
            f"Данные только по обычным пользователям</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data=f"p_menu:{pb_id}")
            )
        )

    elif data.startswith("p_menu:"):
        pb_id = int(data.split(":")[1])
        with get_conn() as conn:
            pb = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
        if pb:
            _send_partner_menu(call.message.chat.id, uid, pb)
        bot.answer_callback_query(call.id)

    elif data.startswith("p_earn:"):
        pb_id = int(data.split(":")[1])
        bot.answer_callback_query(call.id)
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  ⭐  ЗАРАБОТАТЬ ЗВЁЗДЫ   │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Как получить звёзды:\n\n"
            f"🔌 <b>+100⭐</b> — подключить нового бота\n"
            f"📥 <b>+50⭐</b> — добавить бота в группу 50+ участников\n"
            f"🔗 <b>+30⭐</b> — прислать ссылку на активный чат\n"
            f"👤 <b>+10⭐</b> — передать инфу (телефон, страну)\n\n"
            f"<b>На что тратятся:</b>\n"
            f"🔍 <b>1⭐</b> = 1 пробив сверх лимита\n"
            f"⚡ <b>500⭐</b> = план basic на 30 дней\n"
            f"💎 <b>2000⭐</b> = план pro на 30 дней\n\n"
            f"<i>Отправь ссылку на чат — я проверю и начислю звёзды</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                types.InlineKeyboardButton("🔗 Отправить ссылку на чат", callback_data=f"p_submit_chat:{pb_id}"),
                types.InlineKeyboardButton("◀️ Меню",                    callback_data=f"p_menu:{pb_id}"),
            )
        )

    elif data.startswith("p_submit_chat:"):
        pb_id = int(data.split(":")[1])
        user_states[uid] = f"p_submit_chat:{pb_id}"
        safe_edit(
            "🔗 <b>Отправь ссылку на чат</b>\n\n"
            "Формат: <code>https://t.me/chatname</code> или <code>@chatname</code>\n\n"
            "<i>Проверю количество участников и начислю звёзды</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("❌ Отмена", callback_data=f"p_menu:{pb_id}")
            )
        )

    elif data.startswith("p_chats:"):
        pb_id = int(data.split(":")[1])
        with get_conn() as conn:
            chts = conn.execute(
                "SELECT * FROM partner_chats WHERE bot_id=? ORDER BY joined_at DESC", (pb_id,)
            ).fetchall()
        if not chts:
            bot.answer_callback_query(call.id, "У тебя нет подключённых чатов", show_alert=True)
            return
        lines = [f"🗂 <b>Твои чаты ({len(chts)})</b>\n"]
        for c in chts[:15]:
            un  = c["chat_username"]
            lnk = f'<a href="https://t.me/{un}">{c["chat_title"] or c["chat_id"]}</a>' if un else (c["chat_title"] or str(c["chat_id"]))
            mc  = f" 👥{c['member_count']}" if c["member_count"] else ""
            lines.append(f"  ├ {lnk}{mc}")
        safe_edit(
            "\n".join(lines),
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
            )
        )

    elif data.startswith("p_stats:"):
        pb_id = int(data.split(":")[1])
        with get_conn() as conn:
            pb   = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
            sl   = conn.execute(
                "SELECT reason, COUNT(*) as cnt, SUM(amount) as total FROM stars_log WHERE bot_id=? GROUP BY reason",
                (pb_id,)
            ).fetchall()
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            today_probes = conn.execute(
                "SELECT COUNT(*) FROM stars_log WHERE bot_id=? AND reason='probiv' AND ts LIKE ?",
                (pb_id, f"{today}%")
            ).fetchone()[0]
        if not pb:
            bot.answer_callback_query(call.id); return
        plan  = pb["plan"] or "free"
        lim   = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
        stars = pb["stars"] or 0
        lines = [
            f"<code>┌──────────────────────────┐</code>",
            f"<code>│  📊  СТАТИСТИКА          │</code>",
            f"<code>└──────────────────────────┘</code>\n",
            f"⭐ Баланс: <b>{stars}</b>",
            f"📋 План: <b>{plan}</b>  |  Лимит: {lim['daily_probiv']}/день",
            f"🔍 Сегодня пробивов: <b>{today_probes}</b> / {lim['daily_probiv']}\n",
            f"📈 <b>История начислений:</b>",
        ]
        reason_names = {
            "first_connect": "🔌 Первое подключение",
            "add_bot_to_group": "📥 Добавил в группу",
            "link_chat": "🔗 Ссылка на чат",
            "provide_info": "👤 Передал инфу",
            "probiv": "🔍 Использовал пробив",
            "manual": "👑 Начислено вручную",
        }
        for r in sl:
            nm = reason_names.get(r["reason"], r["reason"])
            lines.append(f"  {nm}: ×{r['cnt']}  ({'+' if r['total']>0 else ''}{r['total']}⭐)")
        safe_edit(
            "\n".join(lines), call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
            )
        )

    elif data.startswith("p_help:"):
        pb_id = int(data.split(":")[1])
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  ℹ️  КАК ЭТО РАБОТАЕТ   │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"🔍 <b>Пробив</b> — найди любого пользователя:\n"
            f"  Введи @username, ID или номер телефона.\n"
            f"  Покажу: имя, телефон, в каких группах,\n"
            f"  сколько сообщений, когда был активен.\n\n"
            f"⭐ <b>Звёзды</b> — валюта системы:\n"
            f"  Зарабатывай и трать на пробивы сверх лимита.\n\n"
            f"📋 <b>Планы:</b>\n"
            f"  🆓 free  — 5 пробивов/день\n"
            f"  ⚡ basic — 50 пробивов/день\n"
            f"  💎 pro   — 500/день + экспорт\n\n"
            f"🚫 <b>Что недоступно:</b>\n"
            f"  Данные администраторов, управление ботом,\n"
            f"  настройки системы, история переписки.\n\n"
            f"/connect — подключить ещё один бот\n"
            f"/menu — открыть меню",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("◀️ Меню", callback_data=f"p_menu:{pb_id}")
            )
        )

    elif data == "partners_panel":
        _show_partners_panel(call.message.chat.id, call.message.message_id, edit=True)

    elif data.startswith("partner_view:"):
        pb_id = int(data.split(":")[1])
        _show_partner_detail(call.message.chat.id, call.message.message_id, pb_id, edit=True)

    elif data.startswith("partner_toggle:"):
        parts = data.split(":")
        pb_id = int(parts[1])
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE partner_bots SET is_active=1-is_active WHERE id=?", (pb_id,))
            pb = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
        if pb["is_active"]:
            _launch_partner_bot(pb)
            st = "🟢 Активирован и запущен"
        else:
            _stop_partner_bot(pb_id)
            st = "🔴 Деактивирован и остановлен"
        bot.answer_callback_query(call.id, st)
        _show_partner_detail(call.message.chat.id, call.message.message_id, pb_id, edit=True)

    elif data.startswith("partner_plan:"):
        parts  = data.split(":")
        pb_id  = int(parts[1])
        new_pl = parts[2]
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE partner_bots SET plan=? WHERE id=?", (new_pl, pb_id))
        bot.answer_callback_query(call.id, f"✅ План → {new_pl}")
        _show_partner_detail(call.message.chat.id, call.message.message_id, pb_id, edit=True)

    elif data.startswith("partner_stars:"):
        # partner_stars:{id}:{+/-}{amount}
        parts  = data.split(":")
        pb_id  = int(parts[1])
        amount = int(parts[2])
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE partner_bots SET stars=MAX(0,stars+?) WHERE id=?", (amount, pb_id))
        bot.answer_callback_query(call.id, f"{'➕' if amount>0 else '➖'} {abs(amount)} ⭐")
        _show_partner_detail(call.message.chat.id, call.message.message_id, pb_id, edit=True)

    elif data.startswith("partner_delete:"):
        pb_id = int(data.split(":")[1])
        with db_lock, get_conn() as conn:
            conn.execute("DELETE FROM partner_bots WHERE id=?", (pb_id,))
            conn.execute("DELETE FROM partner_chats WHERE bot_id=?", (pb_id,))
        bot.answer_callback_query(call.id, "🗑 Бот удалён")
        _show_partners_panel(call.message.chat.id, call.message.message_id, edit=True)

    elif data.startswith("partner_chats:"):
        pb_id = int(data.split(":")[1])
        with get_conn() as conn:
            pb   = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
            chts = conn.execute("SELECT * FROM partner_chats WHERE bot_id=? ORDER BY joined_at DESC", (pb_id,)).fetchall()
        if not pb:
            bot.answer_callback_query(call.id, "Не найдено"); return
        lines = [
            f"<code>┌──────────────────────────┐</code>",
            f"<code>│  🗂  ЧАТЫ БОТА           │</code>",
            f"<code>└──────────────────────────┘</code>",
            f"🤖 @{pb['bot_username'] or '?'}  ({len(chts)} чатов)\n",
        ]
        for c in chts[:20]:
            un = c["chat_username"]
            lnk = f'<a href="https://t.me/{un}">{c["chat_title"] or c["chat_id"]}</a>' if un else (c["chat_title"] or str(c["chat_id"]))
            mc  = f" 👥{c['member_count']}" if c["member_count"] else ""
            lines.append(f"  ├ {lnk}{mc} [{(c['joined_at'] or '')[:10]}]")
        kb2 = types.InlineKeyboardMarkup()
        kb2.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"partner_view:{pb_id}"))
        safe_edit("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=kb2)

    elif data == "partner_add_prompt":
        user_states[uid] = "partner_add"
        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔌  ПОДКЛЮЧИТЬ БОТА     │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"Отправь <b>токен бота</b> от @BotFather\n\n"
            f"Формат: <code>1234567890:ABCdef...</code>\n\n"
            f"<i>После подключения владелец сможет делать пробивы за ⭐ звёзды</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=kb_back_menu()
        )

    elif data == "banned_list":
        with get_conn() as conn:
            rows = conn.execute("""
                SELECT b.*, u.username, u.first_name FROM banned_users b
                LEFT JOIN users u ON u.id=b.user_id ORDER BY b.banned_at DESC
            """).fetchall()
        if not rows:
            safe_edit("🚫 <b>Список банов пуст</b>",
                      call.message.chat.id, call.message.message_id, reply_markup=kb_back_menu())
            return
        kb   = types.InlineKeyboardMarkup(row_width=1)
        lines_t = []
        for r in rows:
            tl = f'<a href="tg://user?id={r["user_id"]}">{r["first_name"] or "—"}</a>'
            un = f"@{r['username']}" if r["username"] else "—"
            lines_t.append(f"🚫 {tl} | {un} | <code>{r['user_id']}</code>\n   📌 {r['reason'] or '—'} | {(r['banned_at'] or '')[:16]}")
            kb.add(types.InlineKeyboardButton(f"✅ Разбанить: {(r['first_name'] or str(r['user_id']))[:20]}",
                                              callback_data=f"unban:{r['user_id']}"))
        kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
        safe_edit(f"🚫 <b>Забаненные ({len(rows)})</b>:\n\n" + "\n\n".join(lines_t),
                  call.message.chat.id, call.message.message_id, reply_markup=kb)

    # ── СЛИЯНИЕ БАЗ ──────────────────────────────────────────────
    elif data == "merge_db_confirm":
        # Быстрые COUNT без JOIN — не виснем на confirm
        with get_conn() as conn:
            r = conn.execute("""SELECT
                (SELECT COUNT(*) FROM basa_messages)  AS basa_cnt,
                (SELECT COUNT(*) FROM basa_sessions)  AS sess_cnt,
                (SELECT COUNT(*) FROM messages)       AS msg_cnt,
                (SELECT COUNT(*) FROM users)          AS u_cnt,
                (SELECT COUNT(DISTINCT user_id) FROM basa_messages WHERE user_id IS NOT NULL AND user_id!=0) AS basa_users
            """).fetchone()
        kb_c = types.InlineKeyboardMarkup(row_width=1)
        kb_c.add(
            types.InlineKeyboardButton("✅ Слить сейчас!", callback_data="merge_db_run"),
            types.InlineKeyboardButton("❌ Отмена",        callback_data="main_menu"),
        )
        safe_edit(
            f"<code>┌──────────────────────────────┐</code>\n"
            f"<code>│  🧹  СЛИЯНИЕ БАЗ             │</code>\n"
            f"<code>└──────────────────────────────┘</code>\n\n"
            f"<b>Что произойдёт:</b>\n"
            f"• 👥 Люди из basa → <b>users</b>  (сейчас {r['u_cnt']:,} чел. + ~{r['basa_users']:,} из basa)\n"
            f"• 💬 Сообщения basa ({r['basa_cnt']:,}) → <b>messages</b> (дубли пропустим)\n"
            f"• 👤 Участники → <b>chat_members</b>\n"
            f"• 📱 Телефоны chat_members → <b>users</b>\n"
            f"• 🗑 Пустые сессии ({r['sess_cnt']:,}) очистятся\n\n"
            f"<b>Basa НЕ удаляется</b> — остаётся как архив.\n\n"
            f"⚡ <i>Работает быстро, бот не зависает.</i>\n"
            f"⚠️ <i>Продолжить?</i>",
            call.message.chat.id, call.message.message_id, reply_markup=kb_c
        )

    elif data == "merge_db_run":
        try:
            safe_edit("🧹 <b>Слияние баз...</b>\n<i>Подожди, это займёт немного времени...</i>",
                      call.message.chat.id, call.message.message_id)
        except Exception:
            pass

        def _run_merge():
            try:
                stats = do_merge_databases(call.message.chat.id, call.message.message_id)
                kb_done = types.InlineKeyboardMarkup(row_width=1)
                kb_done.add(
                    types.InlineKeyboardButton("📊 Статистика",   callback_data="stats"),
                    types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"),
                )
                try:
                    secs = stats.get("total_seconds", 0)
                    safe_edit(
                        f"<code>┌──────────────────────────────┐</code>\n"
                        f"<code>│  ✅  СЛИЯНИЕ ЗАВЕРШЕНО!      │</code>\n"
                        f"<code>└──────────────────────────────┘</code>\n\n"
                        f"👤 Новых пользователей:   <b>+{stats['new_users']:,}</b>\n"
                        f"🔄 Обновлено пользователей: <b>{stats['updated_users']:,}</b>\n"
                        f"👥 Участников добавлено:  <b>+{stats.get('chat_members_added',0):,}</b>\n"
                        f"💬 Сообщений перенесено:  <b>+{stats['merged_msgs']:,}</b>\n"
                        f"♻️ Дублей пропущено:      <b>{stats['dup_msgs']:,}</b>\n"
                        f"🗑 Пустых сессий удалено: <b>{stats['empty_sessions']:,}</b>\n\n"
                        f"⏱ Время: <b>{secs}с</b>  |  База в порядке ✅",
                        call.message.chat.id, call.message.message_id,
                        reply_markup=kb_done
                    )
                except Exception:
                    pass
            except Exception as e:
                try:
                    safe_edit(
                        f"❌ <b>Ошибка при слиянии:</b>\n<code>{str(e)[:200]}</code>",
                        call.message.chat.id, call.message.message_id,
                        reply_markup=kb_back_menu()
                    )
                except Exception:
                    pass

        threading.Thread(target=_run_merge, daemon=True).start()

    # ── НОМЕРА В БАЗЕ ─────────────────────────────────────────────
    elif data.startswith("phones_list:"):
        parts_ph = data.split(":")
        page     = int(parts_ph[1]) if len(parts_ph) > 1 else 0
        ph_filter= parts_ph[2] if len(parts_ph) > 2 else "all"
        per_page = 5

        results = []
        with get_conn() as conn:
            # 1. Аккаунты с открытым номером
            if ph_filter in ("all", "account"):
                for a in conn.execute("""
                    SELECT id, username, first_name, last_name, phone
                    FROM users WHERE phone IS NOT NULL AND TRIM(phone)!=''
                    ORDER BY last_seen DESC
                """).fetchall():
                    if _is_valid_phone(a["phone"]):
                        results.append({
                            "phone":a["phone"],"user_id":a["id"],
                            "username":a["username"],"first_name":a["first_name"],"last_name":a["last_name"],
                            "source":"account","chat_id":None,"chat_title":None,
                            "chat_username":None,"message_id":None,"timestamp":None,"text":None,
                        })

            # 2. Основной лог
            if ph_filter in ("all", "message"):
                for m in conn.execute("""
                    SELECT m.user_id, m.chat_id, m.chat_title, m.message_id,
                           m.text, m.timestamp, u.username, u.first_name, u.last_name,
                           c.username as chat_username
                    FROM messages m
                    LEFT JOIN users u ON u.id=m.user_id
                    LEFT JOIN chats c ON c.id=m.chat_id
                    WHERE m.text IS NOT NULL AND m.text!=''
                    ORDER BY m.timestamp DESC
                """).fetchall():
                    for ph in _PHONE_RE_INTL.findall(m["text"] or ""):
                        ph = ph.strip()
                        if _is_valid_phone(ph):
                            results.append({
                                "phone":ph,"user_id":m["user_id"],
                                "username":m["username"],"first_name":m["first_name"],"last_name":m["last_name"],
                                "source":"message","chat_id":m["chat_id"],"chat_title":m["chat_title"],
                                "chat_username":m["chat_username"],"message_id":m["message_id"],
                                "timestamp":m["timestamp"],"text":m["text"],
                            })

            # 3. Basa
            if ph_filter in ("all", "basa"):
                for m in conn.execute("""
                    SELECT user_id, chat_id, chat_title, chat_username,
                           message_id, text, timestamp, username, first_name, last_name
                    FROM basa_messages WHERE text IS NOT NULL AND text!=''
                    ORDER BY timestamp DESC
                """).fetchall():
                    for ph in _PHONE_RE_INTL.findall(m["text"] or ""):
                        ph = ph.strip()
                        if _is_valid_phone(ph):
                            results.append({
                                "phone":ph,"user_id":m["user_id"],
                                "username":m["username"],"first_name":m["first_name"],"last_name":m["last_name"],
                                "source":"basa","chat_id":m["chat_id"],"chat_title":m["chat_title"],
                                "chat_username":m["chat_username"],"message_id":m["message_id"],
                                "timestamp":m["timestamp"],"text":m["text"],
                            })

        # Дедупликация: phone+user_id
        seen  = set()
        dedup = []
        for r in results:
            key = (_clean_phone_digits(r["phone"]), r["user_id"])
            if key not in seen:
                seen.add(key)
                dedup.append(r)
        results = dedup

        cnt_acc = sum(1 for r in results if r["source"] == "account")
        cnt_msg = sum(1 for r in results if r["source"] == "message")
        cnt_bas = sum(1 for r in results if r["source"] == "basa")
        total   = len(results)

        if total == 0:
            kb_e = types.InlineKeyboardMarkup(row_width=2)
            kb_e.row(
                types.InlineKeyboardButton("👤 Аккаунты",  callback_data="phones_list:0:account"),
                types.InlineKeyboardButton("💬 Сообщения", callback_data="phones_list:0:message"),
            )
            kb_e.row(
                types.InlineKeyboardButton("📥 Basa",      callback_data="phones_list:0:basa"),
                types.InlineKeyboardButton("🔍 Все",       callback_data="phones_list:0:all"),
            )
            kb_e.add(types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"))
            safe_edit(
                f"📱 <b>Номера не найдены</b>\n\n<i>Никто не публиковал номера и нет аккаунтов с открытым телефоном.</i>",
                call.message.chat.id, call.message.message_id, reply_markup=kb_e
            )
            return

        total_pages = max(1, (total + per_page - 1) // per_page)
        page        = max(0, min(page, total_pages - 1))
        slice_      = results[page * per_page:(page + 1) * per_page]

        src_icons  = {"account":"👤","message":"💬","basa":"📥"}
        src_labels = {"account":"аккаунт (открытый)","message":"написал в чате","basa":"написал в basa"}

        lines = []
        for r in slice_:
            uid_r   = r["user_id"]
            fname_r = r["first_name"] or ""
            lname_r = r["last_name"]  or ""
            fullname= f"{fname_r} {lname_r}".strip() or "?"
            un_r    = r["username"] or ""
            phone_r = r["phone"]
            src     = r["source"]

            if un_r:
                prof  = f'<a href="https://t.me/{un_r}">{fullname}</a>'
                un_sh = f"@{un_r}"
            else:
                prof  = f'<a href="tg://user?id={uid_r}">{fullname}</a>'
                un_sh = "—"

            si  = src_icons.get(src, "📌")
            sl  = src_labels.get(src, src)

            # Источник сообщения
            msg_part = ""
            if r["message_id"] and r["chat_id"]:
                url, ip = make_msg_link_safe(r["chat_id"], r["message_id"], r["chat_username"])
                dstr    = fmt_date(r["timestamp"] or "")
                cname   = (r["chat_title"] or "чат")[:22]
                go      = f'<a href="{url}">🔗 перейти</a>' if ip else "🔒 приватная"
                # Показываем сниппет с выделенным номером
                snippet = (r["text"] or "")[:100].replace("<","&lt;").replace(">","&gt;")
                msg_part = (
                    f"\n   📍 <b>{cname}</b> · {go} · <i>{dstr}</i>"
                    f"\n   💬 <i>{snippet}</i>"
                )

            block = (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{si} {prof} · {un_sh}\n"
                f"   🆔 <code>{uid_r}</code>\n"
                f"   📱 <b><code>{phone_r}</code></b>\n"
                f"   <i>{sl}</i>"
                f"{msg_part}"
            )
            lines.append(block)

        filter_name = {"all":"все","account":"👤 аккаунты","message":"💬 сообщения","basa":"📥 basa"}.get(ph_filter,"")
        header = (
            f"📱 <b>Номера в базе</b> — <b>{total}</b> [{filter_name}]\n"
            f"👤{cnt_acc}  💬{cnt_msg}  📥{cnt_bas}\n"
            f"Стр. {page+1}/{total_pages}\n"
        )
        text = header + "\n".join(lines)

        kb_ph = types.InlineKeyboardMarkup(row_width=4)
        fbtns = []
        for f_id, f_lbl in [("all","🔍"),("account","👤"),("message","💬"),("basa","📥")]:
            prefix = "›" if f_id == ph_filter else ""
            fbtns.append(types.InlineKeyboardButton(f"{prefix}{f_lbl}", callback_data=f"phones_list:0:{f_id}"))
        kb_ph.add(*fbtns)
        pb = types.InlineKeyboardButton("◀️", callback_data=f"phones_list:{page-1}:{ph_filter}") \
             if page > 0 else types.InlineKeyboardButton("·", callback_data="noop")
        nb = types.InlineKeyboardButton("▶️", callback_data=f"phones_list:{page+1}:{ph_filter}") \
             if page < total_pages-1 else types.InlineKeyboardButton("·", callback_data="noop")
        kb_ph.add(pb, types.InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"), nb)
        kb_ph.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb_ph)

    # ── LIVE ПАНЕЛЬ ──────────────────────────────────────────────
    elif data == "live_panel":
        # Показываем все сессии: активные live + неактивные с кнопками
        with get_conn() as conn:
            sessions_lp = conn.execute(
                "SELECT * FROM basa_sessions ORDER BY created_at DESC"
            ).fetchall()

        # Группируем по chat_id, берём самую актуальную сессию на группу
        lp_groups = OrderedDict()
        for s in sessions_lp:
            cid = s["chat_id"]
            if cid not in lp_groups:
                lp_groups[cid] = s
            # Если уже есть активная live — предпочитаем её
            if cid in _active_basa_sessions and _active_basa_sessions[cid] == s["session_id"]:
                lp_groups[cid] = s

        live_on  = [(cid, g) for cid, g in lp_groups.items() if cid in _active_basa_sessions]
        live_off = [(cid, g) for cid, g in lp_groups.items() if cid not in _active_basa_sessions]

        if not lp_groups:
            safe_edit(
                "🔴 <b>Live-мониторинг</b>\n\n"
                "<i>Нет ни одной сессии. Сначала добавь группу через /basa.</i>",
                call.message.chat.id, call.message.message_id,
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("📥 /basa — добавить группу", callback_data="main_menu"),
                    types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"),
                )
            )
            return

        kb_lp = types.InlineKeyboardMarkup(row_width=1)

        # Кнопки массового управления
        kb_lp.add(types.InlineKeyboardButton(
            "🔴 Live + Выжать ВСЕ чаты бота",
            callback_data="live_start_all_bot_chats"
        ))
        if live_off:
            kb_lp.add(types.InlineKeyboardButton(
                f"🔴 Запустить Live во ВСЕХ ({len(live_off)} групп)",
                callback_data="live_all_start"
            ))
        if live_on:
            kb_lp.add(types.InlineKeyboardButton(
                f"⏹ Остановить Live во ВСЕХ ({len(live_on)} групп)",
                callback_data="live_all_stop"
            ))

        lines_lp = []

        if live_on:
            lines_lp.append("🔴 <b>Активны:</b>")
            for cid, g in live_on:
                title_lp = g["chat_title"] or "Группа"
                un_lp    = f"@{g['chat_username']}" if g["chat_username"] else ""
                sid_lp   = _active_basa_sessions[cid]
                lines_lp.append(f"  🔴 <b>{title_lp}</b> {un_lp}")
                kb_lp.row(
                    types.InlineKeyboardButton(f"⏹ Стоп: {title_lp[:22]}",
                                               callback_data=f"basa_stop:{sid_lp}:{cid}:panel"),
                    types.InlineKeyboardButton("📋", callback_data=f"basa_view:{sid_lp}:0"),
                )

        if live_off:
            lines_lp.append("\n📂 <b>Не активны:</b>")
            for cid, g in live_off:
                title_lp = g["chat_title"] or "Группа"
                un_lp    = f"@{g['chat_username']}" if g["chat_username"] else ""
                sid_lp   = g["session_id"]
                mc_lp    = g["total_messages"] or 0
                lines_lp.append(f"  📂 {title_lp} {un_lp} · {mc_lp} сообщ.")
                kb_lp.row(
                    types.InlineKeyboardButton(f"🔴 Запустить: {title_lp[:20]}",
                                               callback_data=f"basa_start:{sid_lp}:{cid}:panel"),
                    types.InlineKeyboardButton("📋", callback_data=f"basa_view:{sid_lp}:0"),
                )

        kb_lp.add(types.InlineKeyboardButton("📊 Все сессии", callback_data="basa_sessions"))
        kb_lp.add(types.InlineKeyboardButton("🔄 Выжать ВСЕ чаты бота", callback_data="mass_scrape_all"))
        kb_lp.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        safe_edit(
            f"🔴 <b>Live-мониторинг</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Активно: <b>{len(live_on)}</b> · Неактивно: <b>{len(live_off)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            + "\n".join(lines_lp),
            call.message.chat.id, call.message.message_id, reply_markup=kb_lp
        )

    elif data == "live_all_start":
        # Запустить live во всех группах где ещё не запущен
        with get_conn() as conn:
            sessions_las = conn.execute(
                "SELECT * FROM basa_sessions ORDER BY created_at DESC"
            ).fetchall()
        groups_las = OrderedDict()
        for s in sessions_las:
            cid = s["chat_id"]
            if cid not in groups_las:
                groups_las[cid] = s
            if cid in _active_basa_sessions and _active_basa_sessions[cid] == s["session_id"]:
                groups_las[cid] = s

        started = []
        for cid, g in groups_las.items():
            if cid not in _active_basa_sessions:
                _live_set(cid, g["session_id"])
                started.append(g["chat_title"] or f"ID{cid}")

        bot.answer_callback_query(call.id, f"🔴 Запущено: {len(started)} групп")
        # Перерисовываем панель
        call.data = "live_panel"
        handle_callback(call)
        return

    elif data == "live_all_stop":
        # Остановить live везде
        stopped = list(_active_basa_sessions.keys())
        for _cid_clr in list(_active_basa_sessions.keys()):
            _live_unset(_cid_clr)
        bot.answer_callback_query(call.id, f"⏹ Остановлено: {len(stopped)} групп")
        call.data = "live_panel"
        handle_callback(call)
        return

    elif data == "live_start_all_bot_chats":
        # Запустить Live + скопировать историю для ВСЕХ чатов где есть бот
        if not userbot_is_connected():
            bot.answer_callback_query(call.id, "⚠️ Userbot не подключён!", show_alert=True)
            return
        bot.answer_callback_query(call.id, "🔴 Запускаю Live для всех чатов...")
        with get_conn() as conn:
            all_chats = conn.execute("SELECT * FROM chats ORDER BY last_activity DESC").fetchall()

        def _start_all_live():
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            started = 0
            for ch in all_chats:
                cid = ch["id"]
                # Получаем или создаём сессию
                with get_conn() as conn:
                    sess = conn.execute(
                        "SELECT session_id FROM basa_sessions WHERE chat_id=? ORDER BY created_at DESC LIMIT 1",
                        (cid,)
                    ).fetchone()
                if sess:
                    sid = sess["session_id"]
                else:
                    sid = f"{cid}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    with db_lock, get_conn() as conn:
                        conn.execute("""
                            INSERT OR IGNORE INTO basa_sessions
                            (session_id,chat_id,chat_title,chat_username,started_at,created_at,admin_id)
                            VALUES (?,?,?,?,?,?,?)
                        """, (sid, cid, ch["title"], ch["username"], now_s, now_s, uid))
                if cid not in _active_basa_sessions:
                    _live_set(cid, sid)
                    started += 1
            try:
                safe_edit(
                    f"🔴 <b>Live запущен для всех чатов!</b>\n"
                    f"✅ Запущено: <b>{started}</b> новых\n"
                    f"📡 Всего активных: <b>{len(_active_basa_sessions)}</b>\n\n"
                    f"<i>Все новые сообщения пишутся в базу в реальном времени.</i>",
                    call.message.chat.id, call.message.message_id,
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("🔴 Live панель", callback_data="live_panel"),
                        types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"),
                    )
                )
            except Exception:
                pass
        threading.Thread(target=_start_all_live, daemon=True).start()

    elif data == "mass_scrape_all":
        # Выжать историю из ВСЕХ чатов + включить Live
        if not userbot_is_connected():
            bot.answer_callback_query(call.id, "⚠️ Userbot не подключён!", show_alert=True)
            return
        bot.answer_callback_query(call.id, "⏳ Запускаю массовый выжим...")
        with get_conn() as conn:
            all_chats = conn.execute("SELECT * FROM chats ORDER BY last_activity DESC").fetchall()

        safe_edit(
            f"<code>┌──────────────────────────┐</code>\n"
            f"<code>│  🔄  МАССОВЫЙ ВЫЖИМ      │</code>\n"
            f"<code>└──────────────────────────┘</code>\n\n"
            f"📂 Чатов найдено: <b>{len(all_chats)}</b>\n"
            f"⏳ <b>Запускаю в фоне...</b>\n"
            f"<i>Бот будет последовательно копировать каждый чат и включать Live.\n"
            f"Прогресс — в Live панели.</i>",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("🔴 Live панель", callback_data="live_panel"),
                types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"),
            )
        )

        def _mass_scrape():
            now_s   = datetime.datetime.now().isoformat(timespec="seconds")
            done    = 0
            errors  = 0
            for ch in all_chats:
                cid   = ch["id"]
                title = ch["title"] or str(cid)
                uname = ch["username"] or ""
                try:
                    # Получаем или создаём сессию
                    with get_conn() as conn:
                        sess = conn.execute(
                            "SELECT session_id FROM basa_sessions WHERE chat_id=? ORDER BY created_at DESC LIMIT 1",
                            (cid,)
                        ).fetchone()
                    if sess:
                        sid = sess["session_id"]
                    else:
                        sid = f"{cid}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        with db_lock, get_conn() as conn:
                            conn.execute("""
                                INSERT OR IGNORE INTO basa_sessions
                                (session_id,chat_id,chat_title,chat_username,started_at,created_at,admin_id)
                                VALUES (?,?,?,?,?,?,?)
                            """, (sid, cid, title, uname, now_s, now_s, uid))

                    # Копируем историю (INSERT OR IGNORE — без дублей)
                    async def _copy_one(cid=cid, title=title, uname=uname, sid=sid):
                        client = _userbot_client
                        batch  = []
                        saved  = 0
                        now_c  = datetime.datetime.now().isoformat(timespec="seconds")
                        try:
                            entity = await client.get_entity(cid)
                        except Exception:
                            return 0
                        async for message in client.iter_messages(entity, reverse=True):
                            sender = None
                            try: sender = await message.get_sender()
                            except Exception: pass
                            ts  = message.date.isoformat() if message.date else now_c
                            txt = message.text or message.message or ""
                            if not txt and message.media:
                                txt = f"[{type(message.media).__name__}]"
                            uid_m = getattr(sender,"id",0)
                            batch.append((
                                sid, cid, title, uname, "supergroup",
                                uid_m,
                                getattr(sender,"username",None),
                                getattr(sender,"first_name",None),
                                getattr(sender,"last_name",None),
                                message.id, txt,
                                "text" if message.text else "media", ts, now_c
                            ))
                            if len(batch) >= 200:
                                with db_lock, get_conn() as conn:
                                    conn.executemany("""
                                        INSERT OR IGNORE INTO basa_messages
                                        (session_id,chat_id,chat_title,chat_username,chat_type,
                                         user_id,username,first_name,last_name,
                                         message_id,text,content_type,timestamp,saved_at)
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """, batch)
                                saved += len(batch)
                                batch.clear()
                        if batch:
                            with db_lock, get_conn() as conn:
                                conn.executemany("""
                                    INSERT OR IGNORE INTO basa_messages
                                    (session_id,chat_id,chat_title,chat_username,chat_type,
                                     user_id,username,first_name,last_name,
                                     message_id,text,content_type,timestamp,saved_at)
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                """, batch)
                            saved += len(batch)
                        with db_lock, get_conn() as conn:
                            conn.execute(
                                "UPDATE basa_sessions SET total_messages=? WHERE session_id=?",
                                (saved, sid)
                            )
                        return saved

                    saved_cnt = asyncio.run_coroutine_threadsafe(
                        _copy_one(), _userbot_loop
                    ).result(timeout=300)

                    # Включаем Live
                    _live_set(cid, sid)
                    done += 1
                    print(f"  ✅ mass_scrape: {title} — {saved_cnt} сообщ.")
                except Exception as e:
                    errors += 1
                    print(f"  ⚠️  mass_scrape: {title} — ошибка: {e}")

            print(f"🔄 Массовый выжим завершён: {done} чатов, {errors} ошибок")

        threading.Thread(target=_mass_scrape, daemon=True).start()

    # ── BASA ─────────────────────────────────────────────────────
    elif data.startswith("basa_resume:"):
        # basa_resume:{session_id}:{chat_id}
        parts      = data.split(":")
        session_id = parts[1]
        chat_id    = int(parts[2])
        with get_conn() as conn:
            sr = conn.execute(
                "SELECT * FROM basa_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
        if not sr:
            bot.answer_callback_query(call.id, "❌ Сессия не найдена", show_alert=True)
            return
        chat_title = sr["chat_title"] or f"Чат {chat_id}"
        chat_uname = sr["chat_username"] or ""
        last_mid   = sr["last_message_id"] or 0
        bot.answer_callback_query(call.id,
            f"▶️ Продолжаю с сообщения #{last_mid}" if last_mid else "▶️ Запускаю копирование")
        try:
            safe_edit(
                f"⏳ <b>Продолжаю копирование...</b>\n📂 <b>{chat_title}</b>\n"
                f"🔖 С сообщения: <b>#{last_mid}</b>\n<i>Подожди...</i>",
                call.message.chat.id, call.message.message_id
            )
        except Exception:
            pass
        def _do_resume():
            basa_copy_messages(chat_id, {"title": chat_title, "username": chat_uname},
                               session_id, uid, call.message.message_id,
                               call.message.chat.id, start_live=False)
        threading.Thread(target=_do_resume, daemon=True).start()

    elif data.startswith("basa_do:"):
        parts   = data.split(":")
        mode    = parts[1]
        chat_id = int(parts[2])
        with get_conn() as conn:
            row = conn.execute("SELECT * FROM chats WHERE id=?", (chat_id,)).fetchone()
        chat_title = (row["title"] if row else None) or f"Чат {chat_id}"
        chat_uname = (row["username"] if row else None) or ""
        now        = datetime.datetime.now().isoformat(timespec="seconds")

        # Переиспользуем существующую сессию для этого чата (не создаём дубли)
        with get_conn() as conn:
            existing_sess = conn.execute(
                "SELECT session_id FROM basa_sessions WHERE chat_id=? ORDER BY created_at DESC LIMIT 1",
                (chat_id,)
            ).fetchone()

        if existing_sess:
            session_id = existing_sess["session_id"]
        else:
            session_id = f"{chat_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            with db_lock, get_conn() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO basa_sessions
                    (session_id, chat_id, chat_title, chat_username, started_at, created_at, admin_id)
                    VALUES (?,?,?,?,?,?,?)
                """, (session_id, chat_id, chat_title, chat_uname, now, now, uid))

        user_states.pop(uid, None)
        if mode == "live":
            _live_set(chat_id, session_id)
            safe_edit(
                f"🔴 <b>Live-мониторинг запущен!</b>\n📂 <b>{chat_title}</b>\n"
                f"<i>Все новые сообщения сохраняются.</i>",
                call.message.chat.id, call.message.message_id,
                reply_markup=_kb_basa_view(session_id, chat_id)
            )
        elif mode in ("copy", "both"):
            start_live = (mode == "both")
            try:
                safe_edit(
                    f"⏳ <b>Копирую...</b>\n📂 <b>{chat_title}</b>\n<i>Подожди...</i>",
                    call.message.chat.id, call.message.message_id
                )
            except Exception:
                pass
            def _do():
                basa_copy_messages(chat_id, {"title":chat_title,"username":chat_uname},
                                   session_id, uid, call.message.message_id,
                                   call.message.chat.id, start_live=start_live)
            threading.Thread(target=_do, daemon=True).start()

    elif data.startswith("basa_quick:"):
        # Быстрый старт basa copy+live из уведомления о добавлении бота
        chat_id = int(data.split(":")[1])
        with get_conn() as conn:
            row = conn.execute("SELECT * FROM chats WHERE id=?", (chat_id,)).fetchone()
        if not row:
            bot.answer_callback_query(call.id, "❌ Чат не найден в базе", show_alert=True)
            return
        chat_title = row["title"] or f"Чат {chat_id}"
        chat_uname = row["username"] or ""
        session_id = f"{chat_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        now = datetime.datetime.now().isoformat(timespec="seconds")
        with db_lock, get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO basa_sessions
                (session_id, chat_id, chat_title, chat_username, started_at, created_at, admin_id)
                VALUES (?,?,?,?,?,?,?)
            """, (session_id, chat_id, chat_title, chat_uname, now, now, uid))
        try:
            tmp_msg = bot.send_message(call.message.chat.id,
                f"⏳ <b>Копирую + запускаю Live...</b>\n📂 <b>{chat_title}</b>")
        except Exception:
            tmp_msg = call.message
        def _quick_do():
            basa_copy_messages(chat_id, {"title": chat_title, "username": chat_uname},
                               session_id, uid, tmp_msg.message_id,
                               call.message.chat.id, start_live=True)
        threading.Thread(target=_quick_do, daemon=True).start()
        bot.answer_callback_query(call.id, "⏳ Начинаю копирование...")

    elif data.startswith("basa_sessions"):
        # basa_sessions  или  basa_sessions:page
        parts_bs = data.split(":")
        page_bs  = int(parts_bs[1]) if len(parts_bs) > 1 else 0
        per_bs   = 5   # групп на страницу

        with get_conn() as conn:
            raw = conn.execute("SELECT * FROM basa_sessions ORDER BY created_at DESC").fetchall()

        if not raw:
            safe_edit("📊 <b>Нет сохранённых сессий</b>\n\nИспользуй /basa чтобы скопировать группу.",
                      call.message.chat.id, call.message.message_id, reply_markup=kb_back_menu())
            return

        # ── Группируем сессии по chat_id ────────────────────────────
        # Для каждой группы берём: суммарное кол-во сообщений, список сессий,
        # самую последнюю дату, признак live, title/username.
        grouped = OrderedDict()   # chat_id -> dict
        for s in raw:
            cid = s["chat_id"]
            if cid not in grouped:
                grouped[cid] = {
                    "chat_id":       cid,
                    "title":         s["chat_title"] or "Группа",
                    "username":      s["chat_username"] or "",
                    "total_msgs":    0,
                    "sessions":      [],   # список (session_id, total_messages, created_at)
                    "last_date":     s["created_at"] or "",
                    "dup_count":     0,
                }
            g = grouped[cid]
            g["total_msgs"] += s["total_messages"] or 0
            g["sessions"].append({
                "session_id":     s["session_id"],
                "total_messages": s["total_messages"] or 0,
                "created_at":     s["created_at"] or "",
            })
            if (s["created_at"] or "") > g["last_date"]:
                g["last_date"] = s["created_at"] or ""
            if len(g["sessions"]) > 1:
                g["dup_count"] = len(g["sessions"]) - 1

        groups_list = list(grouped.values())
        total_groups = len(groups_list)
        total_dups   = sum(1 for g in groups_list if g["dup_count"] > 0)

        total_pages_bs = max(1, (total_groups + per_bs - 1) // per_bs)
        page_bs        = max(0, min(page_bs, total_pages_bs - 1))
        slice_bs       = groups_list[page_bs * per_bs:(page_bs + 1) * per_bs]

        lines_t = []
        kb      = types.InlineKeyboardMarkup(row_width=1)

        for g in slice_bs:
            title    = g["title"]
            un       = f"@{g['username']}" if g["username"] else ""
            is_live  = g["chat_id"] in _active_basa_sessions
            icon     = "🔴" if is_live else "📂"
            live_tag = " · 🔴 LIVE" if is_live else ""
            dstr     = fmt_date(g["last_date"])
            dup_warn = f" · <b>⚠️ {g['dup_count']+1} сессий</b>" if g["dup_count"] > 0 else ""
            sess_cnt = len(g["sessions"])

            # Проверяем прерванное копирование для последней сессии
            last_sess   = g["sessions"][0]
            copy_status = "idle"
            last_mid    = 0
            with get_conn() as conn:
                sr2 = conn.execute(
                    "SELECT copy_status, last_message_id FROM basa_sessions WHERE session_id=?",
                    (last_sess["session_id"],)
                ).fetchone()
            if sr2:
                copy_status = sr2["copy_status"] or "idle"
                last_mid    = sr2["last_message_id"] or 0
            resume_tag = " · ⏸ <b>прервано</b>" if copy_status == "copying" else ""

            lines_t.append(
                f"{icon} <b>{title}</b> {un}{live_tag}{dup_warn}{resume_tag}\n"
                f"   💬 <b>{g['total_msgs']}</b> сообщ."
                + (f" · {sess_cnt} сессий" if sess_cnt > 1 else "")
                + f" · <i>{dstr}</i>"
            )

            # Если одна сессия
            if sess_cnt == 1:
                sid = g["sessions"][0]["session_id"]
                # Если копирование прервано — показываем кнопку продолжить
                if copy_status == "copying":
                    lbl_resume = f"▶️ Продолжить (с #{last_mid})" if last_mid else f"▶️ Продолжить"
                    kb.row(
                        types.InlineKeyboardButton(f"{icon} {title[:20]} ({g['total_msgs']})",
                                                   callback_data=f"basa_view:{sid}:0"),
                        types.InlineKeyboardButton(lbl_resume,
                                                   callback_data=f"basa_resume:{sid}:{g['chat_id']}"),
                    )
                else:
                    kb.row(
                        types.InlineKeyboardButton(f"{icon} {title[:24]} ({g['total_msgs']}){' LIVE' if is_live else ''}",
                                                   callback_data=f"basa_view:{sid}:0"),
                        types.InlineKeyboardButton("🗑", callback_data=f"basa_del_confirm:{sid}:{g['chat_id']}")
                    )
            else:
                # Несколько сессий — кнопка "открыть все" + кнопка слить
                kb.row(
                    types.InlineKeyboardButton(
                        f"{icon} {title[:20]} ({g['total_msgs']}) ×{sess_cnt}",
                        callback_data=f"basa_group:{g['chat_id']}:0"
                    ),
                    types.InlineKeyboardButton("🔀 Слить", callback_data=f"basa_merge:{g['chat_id']}"),
                    types.InlineKeyboardButton("🗑", callback_data=f"basa_group_del:{g['chat_id']}"),
                )

        # Навигация
        header = (
            f"📊 <b>Базы групп</b> · {total_groups} групп\n"
            + (f"⚠️ <i>{total_dups} групп с дублирующимися сессиями — нажми 🔀 Слить</i>\n" if total_dups else "")
            + f"Стр. {page_bs+1}/{total_pages_bs}\n\n"
        )

        nav_row = []
        if page_bs > 0:
            nav_row.append(types.InlineKeyboardButton("◀️ Назад", callback_data=f"basa_sessions:{page_bs-1}"))
        if page_bs < total_pages_bs - 1:
            nav_row.append(types.InlineKeyboardButton("Далее ▶️", callback_data=f"basa_sessions:{page_bs+1}"))
        if nav_row:
            kb.row(*nav_row)
        if total_dups > 0:
            kb.add(types.InlineKeyboardButton("🔀 Слить ВСЕ дубли", callback_data="basa_merge_all"))
        kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))

        safe_edit(header + "\n\n".join(lines_t),
                  call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data.startswith("basa_del_confirm:"):
        parts      = data.split(":")
        session_id = parts[1]
        chat_id_c  = int(parts[2]) if len(parts) > 2 and parts[2] else 0
        with get_conn() as conn:
            s = conn.execute("SELECT * FROM basa_sessions WHERE session_id=?", (session_id,)).fetchone()
        if not s:
            bot.answer_callback_query(call.id, "Сессия не найдена"); return
        title_c  = s["chat_title"] or "Группа"
        un_c     = f"@{s['chat_username']}" if s["chat_username"] else ""
        live_warn= "\n⚠️ <b>Сейчас идёт Live — он тоже остановится!</b>" if chat_id_c in _active_basa_sessions else ""
        kb_c = types.InlineKeyboardMarkup(row_width=2)
        kb_c.add(
            types.InlineKeyboardButton("✅ Удалить", callback_data=f"basa_delete:{session_id}:{chat_id_c}"),
            types.InlineKeyboardButton("❌ Отмена",   callback_data="basa_sessions"),
        )
        safe_edit(
            f"🗑 <b>Удалить базу?</b>\n📂 <b>{title_c}</b> {un_c}\n"
            f"💬 {s['total_messages']} сообщений{live_warn}\n<i>Удалится безвозвратно.</i>",
            call.message.chat.id, call.message.message_id, reply_markup=kb_c
        )

    elif data.startswith("basa_delete:"):
        parts          = data.split(":")
        session_id_del = parts[1]
        chat_id_del    = int(parts[2]) if len(parts) > 2 and parts[2] else 0
        if chat_id_del and chat_id_del in _active_basa_sessions:
            _live_unset(chat_id_del)
        with db_lock, get_conn() as conn:
            conn.execute("DELETE FROM basa_messages WHERE session_id=?", (session_id_del,))
            conn.execute("DELETE FROM basa_sessions WHERE session_id=?", (session_id_del,))
        bot.answer_callback_query(call.id, "🗑 Удалено")
        safe_edit("🗑 <b>Сессия удалена.</b>", call.message.chat.id, call.message.message_id,
                  reply_markup=types.InlineKeyboardMarkup().add(
                      types.InlineKeyboardButton("📊 Сессии", callback_data="basa_sessions"),
                      types.InlineKeyboardButton("🏠 Меню",   callback_data="main_menu")
                  ))

    # ── ПРОСМОТР СЕССИЙ ОДНОЙ ГРУППЫ ────────────────────────────
    elif data.startswith("basa_group:"):
        parts_bg  = data.split(":")
        cid_bg    = int(parts_bg[1])
        page_bg   = int(parts_bg[2]) if len(parts_bg) > 2 else 0
        per_bg    = 5
        with get_conn() as conn:
            slist = conn.execute(
                "SELECT * FROM basa_sessions WHERE chat_id=? ORDER BY created_at DESC",
                (cid_bg,)
            ).fetchall()
        if not slist:
            bot.answer_callback_query(call.id, "Сессии не найдены"); return
        title_bg = slist[0]["chat_title"] or "Группа"
        un_bg    = f"@{slist[0]['chat_username']}" if slist[0]["chat_username"] else ""
        total_bg = len(slist)
        total_pg = max(1, (total_bg + per_bg - 1) // per_bg)
        page_bg  = max(0, min(page_bg, total_pg - 1))
        slice_bg = slist[page_bg * per_bg:(page_bg + 1) * per_bg]

        kb_bg = types.InlineKeyboardMarkup(row_width=1)
        lines_bg = []
        for s in slice_bg:
            is_live_bg = s["chat_id"] in _active_basa_sessions
            icon_bg    = "🔴" if is_live_bg else "📄"
            dstr_bg    = fmt_date(s["created_at"] or "")
            lines_bg.append(
                f"{icon_bg} {dstr_bg} · 💬 {s['total_messages']} сообщ."
                + (" · LIVE" if is_live_bg else "")
            )
            kb_bg.row(
                types.InlineKeyboardButton(f"{icon_bg} {dstr_bg} ({s['total_messages']})",
                                           callback_data=f"basa_view:{s['session_id']}:0"),
                types.InlineKeyboardButton("🗑", callback_data=f"basa_del_confirm:{s['session_id']}:{cid_bg}")
            )
        nav_bg = []
        if page_bg > 0:
            nav_bg.append(types.InlineKeyboardButton("◀️ Назад", callback_data=f"basa_group:{cid_bg}:{page_bg-1}"))
        if page_bg < total_pg - 1:
            nav_bg.append(types.InlineKeyboardButton("Далее ▶️", callback_data=f"basa_group:{cid_bg}:{page_bg+1}"))
        if nav_bg:
            kb_bg.row(*nav_bg)
        kb_bg.row(
            types.InlineKeyboardButton("🔀 Слить все в одну", callback_data=f"basa_merge:{cid_bg}"),
            types.InlineKeyboardButton("🗑 Удалить все",      callback_data=f"basa_group_del:{cid_bg}"),
        )
        kb_bg.add(types.InlineKeyboardButton("◀️ К списку", callback_data="basa_sessions"))

        total_msgs_bg = sum(s["total_messages"] or 0 for s in slist)
        safe_edit(
            f"📂 <b>{title_bg}</b> {un_bg}\n"
            f"Всего сессий: <b>{total_bg}</b> · Всего сообщений: <b>{total_msgs_bg}</b>\n"
            f"Стр. {page_bg+1}/{total_pg}\n\n"
            + "\n".join(lines_bg),
            call.message.chat.id, call.message.message_id, reply_markup=kb_bg
        )

    # ── СЛИТЬ СЕССИИ ОДНОЙ ГРУППЫ ────────────────────────────────
    elif data.startswith("basa_merge:"):
        cid_m = int(data.split(":")[1])

        def _do_merge_group(c_id, status_cid, status_mid):
            with get_conn() as conn:
                sessions_m = conn.execute(
                    "SELECT * FROM basa_sessions WHERE chat_id=? ORDER BY created_at ASC",
                    (c_id,)
                ).fetchall()
            if len(sessions_m) <= 1:
                try:
                    safe_edit("ℹ️ Только одна сессия — нечего сливать.",
                              status_cid, status_mid, reply_markup=kb_back_menu())
                except Exception: pass
                return

            # Главная сессия = первая (самая ранняя), в неё сливаем всё
            master    = sessions_m[0]
            master_id = master["session_id"]
            others    = sessions_m[1:]
            merged    = 0
            dups      = 0

            for s in others:
                with get_conn() as conn:
                    msgs_s = conn.execute(
                        "SELECT * FROM basa_messages WHERE session_id=?", (s["session_id"],)
                    ).fetchall()
                for m in msgs_s:
                    # Проверяем дубль внутри мастера
                    with get_conn() as conn:
                        dup = conn.execute("""
                            SELECT id FROM basa_messages
                            WHERE session_id=? AND message_id=? AND user_id=?
                        """, (master_id, m["message_id"], m["user_id"])).fetchone()
                    if dup:
                        dups += 1
                        continue
                    with db_lock, get_conn() as conn:
                        conn.execute("""
                            INSERT OR IGNORE INTO basa_messages
                            (session_id,chat_id,chat_title,chat_username,chat_type,
                             user_id,username,first_name,last_name,
                             message_id,text,content_type,timestamp,saved_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            master_id, m["chat_id"], m["chat_title"], m["chat_username"],
                            m["chat_type"], m["user_id"], m["username"],
                            m["first_name"], m["last_name"],
                            m["message_id"], m["text"], m["content_type"],
                            m["timestamp"], m["saved_at"]
                        ))
                    merged += 1
                # Удаляем поглощённую сессию
                with db_lock, get_conn() as conn:
                    conn.execute("DELETE FROM basa_messages WHERE session_id=?", (s["session_id"],))
                    conn.execute("DELETE FROM basa_sessions WHERE session_id=?", (s["session_id"],))
                    # Если эта сессия была live — переносим на мастер
                    if c_id in _active_basa_sessions and _active_basa_sessions[c_id] == s["session_id"]:
                        _live_set(c_id, master_id)

            # Обновляем счётчик мастера
            with get_conn() as conn:
                real_cnt = conn.execute(
                    "SELECT COUNT(*) as c FROM basa_messages WHERE session_id=?", (master_id,)
                ).fetchone()["c"]
            with db_lock, get_conn() as conn:
                conn.execute("UPDATE basa_sessions SET total_messages=? WHERE session_id=?",
                             (real_cnt, master_id))

            kb_done = types.InlineKeyboardMarkup(row_width=1)
            kb_done.add(
                types.InlineKeyboardButton("📋 Открыть", callback_data=f"basa_view:{master_id}:0"),
                types.InlineKeyboardButton("◀️ К списку", callback_data="basa_sessions"),
            )
            title_m = master["chat_title"] or "Группа"
            un_m    = f"@{master['chat_username']}" if master["chat_username"] else ""
            try:
                safe_edit(
                    f"✅ <b>Сессии слиты!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                    f"📂 <b>{title_m}</b> {un_m}\n"
                    f"💬 Перенесено: <b>{merged}</b> · Дублей пропущено: <b>{dups}</b>\n"
                    f"📊 Итого в сессии: <b>{real_cnt}</b> сообщений\n"
                    f"━━━━━━━━━━━━━━━━━━━━",
                    status_cid, status_mid, reply_markup=kb_done
                )
            except Exception:
                pass

        try:
            safe_edit(f"🔀 <b>Сливаю сессии...</b>\n<i>Подожди...</i>",
                      call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        threading.Thread(target=_do_merge_group,
                         args=(cid_m, call.message.chat.id, call.message.message_id),
                         daemon=True).start()

    # ── СЛИТЬ ВСЕ ДУБЛИРУЮЩИЕСЯ ГРУППЫ ──────────────────────────
    elif data == "basa_merge_all":
        def _do_merge_all(status_cid, status_mid):
            with get_conn() as conn:
                all_cids = [r[0] for r in conn.execute(
                    "SELECT chat_id FROM basa_sessions GROUP BY chat_id HAVING COUNT(*)>1"
                ).fetchall()]
            if not all_cids:
                try:
                    safe_edit("ℹ️ Дублей не найдено — всё уже в порядке!",
                              status_cid, status_mid, reply_markup=kb_back_menu())
                except Exception: pass
                return
            total_merged = 0
            total_dups_m = 0
            groups_done  = 0
            for cid_a in all_cids:
                with get_conn() as conn:
                    sessions_a = conn.execute(
                        "SELECT * FROM basa_sessions WHERE chat_id=? ORDER BY created_at ASC",
                        (cid_a,)
                    ).fetchall()
                if len(sessions_a) <= 1:
                    continue
                master_a  = sessions_a[0]
                master_id_a = master_a["session_id"]
                for s in sessions_a[1:]:
                    with get_conn() as conn:
                        msgs_a = conn.execute(
                            "SELECT * FROM basa_messages WHERE session_id=?", (s["session_id"],)
                        ).fetchall()
                    for m in msgs_a:
                        with get_conn() as conn:
                            dup = conn.execute("""
                                SELECT id FROM basa_messages
                                WHERE session_id=? AND message_id=? AND user_id=?
                            """, (master_id_a, m["message_id"], m["user_id"])).fetchone()
                        if dup:
                            total_dups_m += 1
                            continue
                        with db_lock, get_conn() as conn:
                            conn.execute("""
                                INSERT OR IGNORE INTO basa_messages
                                (session_id,chat_id,chat_title,chat_username,chat_type,
                                 user_id,username,first_name,last_name,
                                 message_id,text,content_type,timestamp,saved_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """, (
                                master_id_a, m["chat_id"], m["chat_title"], m["chat_username"],
                                m["chat_type"], m["user_id"], m["username"],
                                m["first_name"], m["last_name"],
                                m["message_id"], m["text"], m["content_type"],
                                m["timestamp"], m["saved_at"]
                            ))
                        total_merged += 1
                    with db_lock, get_conn() as conn:
                        conn.execute("DELETE FROM basa_messages WHERE session_id=?", (s["session_id"],))
                        conn.execute("DELETE FROM basa_sessions WHERE session_id=?", (s["session_id"],))
                        if cid_a in _active_basa_sessions and _active_basa_sessions[cid_a] == s["session_id"]:
                            _live_set(cid_a, master_id_a)
                # Обновляем счётчик
                with get_conn() as conn:
                    real_c = conn.execute(
                        "SELECT COUNT(*) as c FROM basa_messages WHERE session_id=?", (master_id_a,)
                    ).fetchone()["c"]
                with db_lock, get_conn() as conn:
                    conn.execute("UPDATE basa_sessions SET total_messages=? WHERE session_id=?",
                                 (real_c, master_id_a))
                groups_done += 1

            kb_done = types.InlineKeyboardMarkup()
            kb_done.add(types.InlineKeyboardButton("📊 К списку сессий", callback_data="basa_sessions"))
            try:
                safe_edit(
                    f"✅ <b>Все дубли слиты!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                    f"📂 Обработано групп: <b>{groups_done}</b>\n"
                    f"💬 Перенесено сообщений: <b>{total_merged}</b>\n"
                    f"♻️ Дублей пропущено: <b>{total_dups_m}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━",
                    status_cid, status_mid, reply_markup=kb_done
                )
            except Exception:
                pass

        try:
            safe_edit("🔀 <b>Сливаю все дублирующиеся сессии...</b>\n<i>Подожди...</i>",
                      call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        threading.Thread(target=_do_merge_all,
                         args=(call.message.chat.id, call.message.message_id),
                         daemon=True).start()

    # ── УДАЛИТЬ ВСЕ СЕССИИ ГРУППЫ ────────────────────────────────
    elif data.startswith("basa_group_del:"):
        cid_gd = int(data.split(":")[1])
        with get_conn() as conn:
            sessions_gd = conn.execute(
                "SELECT * FROM basa_sessions WHERE chat_id=?", (cid_gd,)
            ).fetchall()
        if not sessions_gd:
            bot.answer_callback_query(call.id, "Нет сессий"); return
        title_gd   = sessions_gd[0]["chat_title"] or "Группа"
        un_gd      = f"@{sessions_gd[0]['chat_username']}" if sessions_gd[0]["chat_username"] else ""
        total_gd   = sum(s["total_messages"] or 0 for s in sessions_gd)
        live_gd    = "⚠️ <b>Сейчас идёт Live — он тоже остановится!</b>\n" if cid_gd in _active_basa_sessions else ""
        kb_gd = types.InlineKeyboardMarkup(row_width=2)
        kb_gd.add(
            types.InlineKeyboardButton("✅ Удалить всё", callback_data=f"basa_group_del_confirm:{cid_gd}"),
            types.InlineKeyboardButton("❌ Отмена",       callback_data=f"basa_group:{cid_gd}:0"),
        )
        safe_edit(
            f"🗑 <b>Удалить ВСЕ сессии группы?</b>\n"
            f"📂 <b>{title_gd}</b> {un_gd}\n"
            f"💬 {total_gd} сообщений · {len(sessions_gd)} сессий\n"
            f"{live_gd}<i>Удалится безвозвратно.</i>",
            call.message.chat.id, call.message.message_id, reply_markup=kb_gd
        )

    elif data.startswith("basa_group_del_confirm:"):
        cid_gdc = int(data.split(":")[1])
        if cid_gdc in _active_basa_sessions:
            _live_unset(cid_gdc)
        with db_lock, get_conn() as conn:
            sessions_del = conn.execute(
                "SELECT session_id FROM basa_sessions WHERE chat_id=?", (cid_gdc,)
            ).fetchall()
            for s in sessions_del:
                conn.execute("DELETE FROM basa_messages WHERE session_id=?", (s["session_id"],))
                conn.execute("DELETE FROM basa_sessions WHERE session_id=?", (s["session_id"],))
        bot.answer_callback_query(call.id, "🗑 Все сессии группы удалены")
        safe_edit("🗑 <b>Все сессии группы удалены.</b>",
                  call.message.chat.id, call.message.message_id,
                  reply_markup=types.InlineKeyboardMarkup().add(
                      types.InlineKeyboardButton("📊 Сессии", callback_data="basa_sessions"),
                      types.InlineKeyboardButton("🏠 Меню",   callback_data="main_menu")
                  ))

    elif data.startswith("basa_stop:"):
        # basa_stop:{session_id}:{chat_id}:{from}   from=panel -> вернуть в live_panel
        parts        = data.split(":")
        session_id   = parts[1]
        chat_id_stop = int(parts[2]) if len(parts) > 2 and parts[2] else 0
        from_panel   = len(parts) > 3 and parts[3] == "panel"
        if chat_id_stop:
            _live_unset(chat_id_stop)
        bot.answer_callback_query(call.id, "⏹ Остановлено")
        if from_panel:
            call.data = "live_panel"
            handle_callback(call)
            return
        safe_edit("⏹ <b>Live-мониторинг остановлен.</b>",
                  call.message.chat.id, call.message.message_id,
                  reply_markup=_kb_basa_view(session_id, chat_id_stop))

    elif data.startswith("basa_start:"):
        # basa_start:{session_id}:{chat_id}:{from}   from=panel -> вернуть в live_panel
        parts         = data.split(":")
        session_id    = parts[1]
        chat_id_start = int(parts[2]) if len(parts) > 2 and parts[2] else 0
        from_panel    = len(parts) > 3 and parts[3] == "panel"
        if chat_id_start:
            _live_set(chat_id_start, session_id)
        bot.answer_callback_query(call.id, "🔴 Запущено!")
        if from_panel:
            call.data = "live_panel"
            handle_callback(call)
            return
        safe_edit("🔴 <b>Live-мониторинг запущен!</b>",
                  call.message.chat.id, call.message.message_id,
                  reply_markup=_kb_basa_view(session_id, chat_id_start))

    elif data.startswith("basa_view:"):
        parts      = data.split(":")
        session_id = parts[1]
        page       = int(parts[2])
        per_page   = 6
        with get_conn() as conn:
            all_m = conn.execute("SELECT * FROM basa_messages WHERE session_id=? ORDER BY timestamp ASC",
                                  (session_id,)).fetchall()
            sess  = conn.execute("SELECT * FROM basa_sessions WHERE session_id=?", (session_id,)).fetchone()
        total       = len(all_m)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page        = max(0, min(page, total_pages - 1))
        slice_      = all_m[page * per_page:(page + 1) * per_page]
        title       = (sess["chat_title"] if sess else "Группа") or "Группа"
        cu          = (sess["chat_username"] if sess else None)
        cid         = (sess["chat_id"] if sess else 0)
        header      = f"📋 <b>{title}</b>\nСтр. {page+1}/{total_pages} | {total} сообщ.\n\n"
        lines_t = []
        for m in slice_:
            txt    = (m["text"] or "")[:70].replace("<","&lt;").replace(">","&gt;")
            fname  = m["first_name"] or "?"
            un_s   = f"@{m['username']}" if m["username"] else f"id{m['user_id']}"
            u_link = f'<a href="tg://user?id={m["user_id"]}">{fname}</a>' if m["user_id"] else fname
            url, ip= make_msg_link_safe(cid, m["message_id"], cu)
            icon   = ct_icon(m["content_type"])
            lines_t.append(
                f"{icon} {u_link} <i>({un_s})</i>\n"
                f"   {fmt_msg_link(url, ip, m['timestamp'])} — {txt or '[медиа]'}"
            )
        text = header + "\n\n".join(lines_t) if lines_t else header + "Пусто"
        kb   = types.InlineKeyboardMarkup(row_width=3)
        pb   = types.InlineKeyboardButton("◀️", callback_data=f"basa_view:{session_id}:{page-1}") \
               if page > 0 else types.InlineKeyboardButton("·", callback_data="noop")
        nb   = types.InlineKeyboardButton("▶️", callback_data=f"basa_view:{session_id}:{page+1}") \
               if page < total_pages-1 else types.InlineKeyboardButton("·", callback_data="noop")
        kb.add(pb, types.InlineKeyboardButton("📊 Сессии", callback_data="basa_sessions"), nb)
        kb.add(types.InlineKeyboardButton("🏠 Меню", callback_data="main_menu"))
        safe_edit(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data == "noop":
        pass

    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass


# ─────────────────────────────────────────────
#  СОБЫТИЯ ГРУППЫ
# ─────────────────────────────────────────────
@bot.my_chat_member_handler()
def handle_chat_member(update: types.ChatMemberUpdated):
    chat   = update.chat
    new_st = update.new_chat_member.status
    now    = datetime.datetime.now().isoformat(timespec="seconds")

    if new_st in ("member", "administrator"):
        upsert_chat(chat)
        try:
            log_event("bot_added", chat_id=chat.id, description=f"Бот добавлен в {chat.title}")
        except Exception:
            pass
        _notify_bot_added(chat, update.from_user)

    elif new_st in ("kicked", "left"):
        try:
            log_event("bot_removed", chat_id=chat.id, description=f"Бот удалён из {chat.title}")
        except Exception:
            pass
        un   = getattr(chat, "username", None)
        link = f'<a href="https://t.me/{un}">{chat.title}</a>' if un else f"<b>{chat.title}</b>"
        for aid in ADMIN_IDS:
            try:
                bot.send_message(aid,
                    f"<code>┌────────────────────────┐</code>\n"
                    f"<code>│  ➖  БОТ УДАЛЁН        │</code>\n"
                    f"<code>└────────────────────────┘</code>\n"
                    f"📂 {link}\n"
                    f"🆔 <code>{chat.id}</code>  |  {chat.type}",
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("📊 К чатам", callback_data="chats_list")
                    ))
            except Exception:
                pass


def _notify_bot_added(chat, added_by=None):
    """Умное уведомление при добавлении бота в группу."""
    now = datetime.datetime.now().isoformat(timespec="seconds")
    un  = getattr(chat, "username", None)
    mc  = getattr(chat, "member_count", None)
    desc= getattr(chat, "description", None)

    # Проверяем настройки уведомлений
    with get_conn() as conn:
        notif = conn.execute(
            "SELECT * FROM chat_notif_settings WHERE chat_id=?", (chat.id,)
        ).fetchone()
    if notif and notif["muted"]:
        # Проверяем не истёк ли mute
        if notif["muted_until"]:
            if notif["muted_until"] > now:
                return  # Ещё muted
        else:
            return  # Бессрочно muted

    link = f'<a href="https://t.me/{un}">{chat.title}</a>' if un else f"<b>{escape_html(chat.title or '?')}</b>"
    mc_str = f"👥 {mc}" if mc else ""

    # Ищем доп. инфу из БД
    with get_conn() as conn:
        msg_cnt = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE chat_id=?", (chat.id,)
        ).fetchone()[0]
        basa_cnt = conn.execute(
            "SELECT COUNT(*) FROM basa_messages WHERE chat_id=?", (chat.id,)
        ).fetchone()[0]
        cm_cnt = conn.execute(
            "SELECT COUNT(*) FROM chat_members WHERE chat_id=?", (chat.id,)
        ).fetchone()[0]
        phones_cnt = conn.execute(
            "SELECT COUNT(*) FROM chat_members WHERE chat_id=? AND phone IS NOT NULL", (chat.id,)
        ).fetchone()[0]
        sess_cnt = conn.execute(
            "SELECT COUNT(*) FROM basa_sessions WHERE chat_id=?", (chat.id,)
        ).fetchone()[0]
        live_sid = conn.execute(
            "SELECT session_id FROM basa_sessions WHERE chat_id=? AND is_live=1 LIMIT 1", (chat.id,)
        ).fetchone()

    # Кто добавил
    adder = ""
    if added_by:
        adder_un = f"@{added_by.username}" if added_by.username else f"id{added_by.id}"
        adder = f"\n👤 Добавил: <a href=\"tg://user?id={added_by.id}\">{escape_html(added_by.first_name or '?')}</a> {adder_un}"

    known_label = ""
    if msg_cnt > 0 or basa_cnt > 0 or cm_cnt > 0:
        known_label = (
            f"\n♻️ <b>Уже в базе:</b> 💬{msg_cnt} сообщ. · 📥{basa_cnt} basa · 👤{cm_cnt} участн."
        )
        if phones_cnt:
            known_label += f" · 📱{phones_cnt} тел."
        if sess_cnt:
            known_label += f"\n📊 Сессий basa: {sess_cnt}"
        if live_sid:
            known_label += f"\n🔴 Live активен!"

    text = (
        f"<code>┌────────────────────────┐</code>\n"
        f"<code>│  ➕  БОТ ДОБАВЛЕН      │</code>\n"
        f"<code>└────────────────────────┘</code>\n"
        f"📂 {link}\n"
        f"🆔 <code>{chat.id}</code>  |  {chat.type}  {mc_str}"
        f"{adder}"
        + (f"\n📝 {escape_html(desc[:80])}" if desc else "")
        + known_label
    )

    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("📂 Открыть",        callback_data=f"chat_view:{chat.id}"),
        types.InlineKeyboardButton("🔕 Откл. увед.",    callback_data=f"notif_mute:{chat.id}:forever"),
    )
    if userbot_is_connected():
        kb.row(
            types.InlineKeyboardButton("⚙️ Спарсить",   callback_data=f"chat_scrape:{chat.id}"),
            types.InlineKeyboardButton("🔴 Запустить Live", callback_data=f"chat_live_quick:{chat.id}"),
        )
    kb.row(
        types.InlineKeyboardButton("🔇 Мут на 24ч",  callback_data=f"notif_mute:{chat.id}:24h"),
        types.InlineKeyboardButton("📥 Скопировать базу", callback_data=f"basa_quick:{chat.id}"),
    )

    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, text, reply_markup=kb)
        except Exception:
            pass


@bot.chat_member_handler()
def handle_member_change(update: types.ChatMemberUpdated):
    new_s = update.new_chat_member.status
    old_s = update.old_chat_member.status
    user  = update.new_chat_member.user
    if user.is_bot: return
    upsert_user(user)
    if old_s in ("left", "kicked") and new_s == "member":
        log_event("user_joined", user_id=user.id, chat_id=update.chat.id,
                  description=f"{user.first_name} вошёл в {update.chat.title}")
    elif new_s in ("left", "kicked") and old_s == "member":
        log_event("user_left", user_id=user.id, chat_id=update.chat.id,
                  description=f"{user.first_name} вышел из {update.chat.title}")


# ─────────────────────────────────────────────
#  ПАРТНЁРСКАЯ СИСТЕМА — API ботов
# ─────────────────────────────────────────────

STARS_REWARD = {
    "add_bot_to_group":  50,   # добавил бота в группу
    "link_chat":         30,   # прислал ссылку на чат
    "provide_info":      10,   # дал информацию (имя, телефон, страна)
    "first_connect":    100,   # первое подключение API
}

PLAN_LIMITS = {
    "free":    {"daily_probiv": 5,  "can_export": False, "can_search": True},
    "basic":   {"daily_probiv": 50, "can_export": False, "can_search": True},
    "pro":     {"daily_probiv": 500,"can_export": True,  "can_search": True},
    "admin":   {"daily_probiv": 999999, "can_export": True, "can_search": True},
}


def _get_partner_bot(token: str):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM partner_bots WHERE bot_token=?", (token,)).fetchone()

def _add_stars(user_id, bot_id, amount, reason):
    now = datetime.datetime.now().isoformat(timespec="seconds")
    with db_lock, get_conn() as conn:
        conn.execute("UPDATE partner_bots SET stars=stars+? WHERE id=?", (amount, bot_id))
        conn.execute(
            "INSERT INTO stars_log (user_id, bot_id, amount, reason, ts) VALUES (?,?,?,?,?)",
            (user_id, bot_id, amount, reason, now)
        )

def _check_plan_limit(bot_id, action):
    with get_conn() as conn:
        pb = conn.execute("SELECT * FROM partner_bots WHERE id=?", (bot_id,)).fetchone()
    if not pb:
        return False, "Бот не найден"
    plan = pb["plan"] or "free"
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
    if action == "probiv":
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with get_conn() as conn:
            used = conn.execute(
                "SELECT COUNT(*) FROM stars_log WHERE bot_id=? AND reason='probiv' AND ts LIKE ?",
                (bot_id, f"{today}%")
            ).fetchone()[0]
        if used >= limits["daily_probiv"]:
            return False, f"Лимит пробивов на сегодня исчерпан ({limits['daily_probiv']}/день). Апгрейд плана."
    return True, "ok"


# ─────────────────────────────────────────────
#  АВТО-ВОССТАНОВЛЕНИЕ USERBOT
# ─────────────────────────────────────────────
def _try_restore_userbot():
    if not TELETHON_OK or not TELE_API_ID:
        return
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    sessions = [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".session")]
    if not sessions:
        return
    sess_file = os.path.join(SESSIONS_DIR, sessions[0].replace(".session",""))
    print(f"🔄 Найдена сессия: {sessions[0]} — подключаю userbot...")

    def _run():
        global _userbot_client, _userbot_loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _userbot_loop = loop
        async def _boot():
            global _userbot_client
            nc, ok = await _async_start_userbot(TELE_API_ID, TELE_API_HASH, sess_file)
            _userbot_client = nc
            if ok:
                print(f"✅ Userbot: {_userbot_info.get('name')} ({_userbot_info.get('phone')})")
                await nc.run_until_disconnected()
            else:
                print("⚠️  Сессия истекла — нужен /account")
        loop.run_until_complete(_boot())

    threading.Thread(target=_run, daemon=True).start()


# ═══════════════════════════════════════════════════════════════
#  ПАРТНЁРСКАЯ ПАНЕЛЬ — вспомогательные функции
# ═══════════════════════════════════════════════════════════════

def _show_partners_panel(chat_id, msg_id, edit=False):
    with get_conn() as conn:
        bots = conn.execute("""
            SELECT pb.*,
                   (SELECT COUNT(*) FROM partner_chats WHERE bot_id=pb.id) as chat_cnt,
                   (SELECT COUNT(*) FROM stars_log WHERE bot_id=pb.id AND reason='probiv') as probiv_total
            FROM partner_bots pb ORDER BY pb.connected_at DESC
        """).fetchall()
    lines = [
        f"<code>┌──────────────────────────────┐</code>",
        f"<code>│  🔌  ПАРТНЁРСКИЕ БОТЫ  ({len(bots)})  │</code>",
        f"<code>└──────────────────────────────┘</code>\n",
    ]
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pb in bots:
        st    = "🟢" if pb["is_active"] else "🔴"
        uname = pb["bot_username"] or "?"
        plan  = pb["plan"] or "free"
        stars = pb["stars"] or 0
        chcnt = pb["chat_cnt"] or 0
        prtot = pb["probiv_total"] or 0
        owner = pb["owner_username"] or pb["owner_name"] or f"id{pb['owner_id']}" or "?"
        lines.append(
            f"{st} <b>@{uname}</b>  [{plan}]  ⭐{stars}\n"
            f"   👤 {owner}  |  🗂 {chcnt} чатов  |  🔍 {prtot} проб."
        )
        kb.add(types.InlineKeyboardButton(
            f"{st} @{uname} [{plan}] ⭐{stars}",
            callback_data=f"partner_view:{pb['id']}"
        ))
    kb.row(
        types.InlineKeyboardButton("🔌 Подключить бота", callback_data="partner_add_prompt"),
        types.InlineKeyboardButton("🏠 Меню",             callback_data="main_menu"),
    )
    text = "\n".join(lines) if bots else (
        "<code>┌──────────────────────────────┐</code>\n"
        "<code>│  🔌  ПАРТНЁРСКИЕ БОТЫ        │</code>\n"
        "<code>└──────────────────────────────┘</code>\n\n"
        "<i>Нет подключённых ботов.</i>\n"
        "Отправь токен через /api или кнопку ниже."
    )
    if edit:
        safe_edit(text, chat_id, msg_id, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)


def _show_partner_detail(chat_id, msg_id, pb_id, edit=False):
    with get_conn() as conn:
        pb   = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
        chcnt= conn.execute("SELECT COUNT(*) FROM partner_chats WHERE bot_id=?", (pb_id,)).fetchone()[0]
        sl   = conn.execute("""
            SELECT reason, COUNT(*) as cnt, SUM(amount) as total
            FROM stars_log WHERE bot_id=? GROUP BY reason
        """, (pb_id,)).fetchall()
    if not pb:
        return
    st      = "🟢 АКТИВЕН" if pb["is_active"] else "🔴 ВЫКЛЮЧЕН"
    plan    = pb["plan"] or "free"
    lim     = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
    stars   = pb["stars"] or 0
    uname   = pb["bot_username"] or "?"
    owner   = pb["owner_username"] or pb["owner_name"] or f"id{pb['owner_id']}" or "?"
    owner_id= pb["owner_id"] or 0
    token_masked = (pb["bot_token"] or "")[:10] + "..." if pb["bot_token"] else "—"

    sl_lines = ""
    for r in sl:
        sl_lines += f"\n  ├ {r['reason']}: ×{r['cnt']}  +{r['total']}⭐"

    lines = [
        f"<code>┌──────────────────────────────┐</code>",
        f"<code>│  🤖  ПАРТНЁР                 │</code>",
        f"<code>└──────────────────────────────┘</code>",
        f"\n🤖 @{uname}  —  {st}",
        f"👤 Владелец: <a href=\"tg://user?id={owner_id}\">{owner}</a>  <code>{owner_id}</code>",
        f"🔑 Токен: <code>{token_masked}</code>",
        f"📅 Подключён: {(pb['connected_at'] or '')[:16]}",
        f"🏓 Последний пинг: {(pb['last_ping'] or '—')[:16]}",
        f"\n<code>──────────────────────────────</code>",
        f"⭐ Звёзд: <b>{stars}</b>",
        f"📋 План: <b>{plan}</b>  |  🔍 Лимит: {lim['daily_probiv']}/день",
        f"🗂 Чатов: <b>{chcnt}</b>",
        f"📊 Использование:{sl_lines or ' нет'}",
        f"\n📝 Заметка: {pb['note'] or '—'}",
    ]
    if pb["owner_id"]:
        with get_conn() as conn:
            u_own = conn.execute("SELECT * FROM users WHERE id=?", (pb["owner_id"],)).fetchone()
        if u_own:
            ph = u_own["phone"] or "—"
            lines.append(f"📱 Телефон владельца: <code>{ph}</code>")

    text = "\n".join(lines)

    toggle_lbl = "⏸ Деактивировать" if pb["is_active"] else "▶️ Активировать"
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton(toggle_lbl,           callback_data=f"partner_toggle:{pb_id}"),
        types.InlineKeyboardButton("🗂 Чаты бота",       callback_data=f"partner_chats:{pb_id}"),
    )
    # Управление планом
    plans = ["free", "basic", "pro", "admin"]
    kb.row(*[
        types.InlineKeyboardButton(
            f"{'✅' if plan==p else ''}{p}",
            callback_data=f"partner_plan:{pb_id}:{p}"
        ) for p in plans
    ])
    # Управление звёздами
    kb.row(
        types.InlineKeyboardButton("➕ 100⭐",  callback_data=f"partner_stars:{pb_id}:100"),
        types.InlineKeyboardButton("➕ 500⭐",  callback_data=f"partner_stars:{pb_id}:500"),
        types.InlineKeyboardButton("➖ 100⭐",  callback_data=f"partner_stars:{pb_id}:-100"),
    )
    kb.row(
        types.InlineKeyboardButton("🕵️ Пробить владельца", callback_data=f"probiv_show:{pb['owner_id']}"),
        types.InlineKeyboardButton("🗑 Удалить",            callback_data=f"partner_delete:{pb_id}"),
    )
    kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data="partners_panel"))

    if edit:
        safe_edit(text, chat_id, msg_id, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)


def _connect_partner_bot(token: str, added_by_id: int) -> str:
    """Подключает партнёрский бот по токену. Возвращает строку-результат."""
    import json as _json
    token = token.strip()
    # Валидация формата
    if ":" not in token or len(token) < 20:
        return "❌ Неверный формат токена."
    # Проверяем через Telegram API (с отключённой SSL верификацией для Windows)
    try:
        import urllib.request, ssl as _ssl
        url = f"https://api.telegram.org/bot{token}/getMe"
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        try:
            resp = urllib.request.urlopen(url, timeout=10, context=ctx).read()
        except TypeError:
            # Старые версии Python без context параметра
            resp = urllib.request.urlopen(url, timeout=10).read()
        data = _json.loads(resp)
        if not data.get("ok"):
            return "❌ Токен не работает (getMe вернул ok=false)."
        bot_info = data["result"]
    except Exception as e:
        err_str = str(e)
        # Пробуем через telebot как fallback
        try:
            import telebot as _tb
            _tmp_bot = _tb.TeleBot(token)
            me = _tmp_bot.get_me()
            bot_info = {"username": me.username, "first_name": me.first_name}
        except Exception as e2:
            return f"❌ Не удалось проверить токен: {escape_html(err_str)}"

    uname = bot_info.get("username", "")
    name  = bot_info.get("first_name", "")
    now_s = datetime.datetime.now().isoformat(timespec="seconds")

    # Узнаём инфу о владельце
    owner_u = None
    with get_conn() as conn:
        owner_u = conn.execute("SELECT * FROM users WHERE id=?", (added_by_id,)).fetchone()
        # Проверяем не добавлен ли уже
        exists = conn.execute("SELECT id FROM partner_bots WHERE bot_token=?", (token,)).fetchone()
    if exists:
        return f"⚠️ Бот @{uname} уже подключён."

    owner_un   = (owner_u["username"]   if owner_u else None) or ""
    owner_name = ((owner_u["first_name"] if owner_u else None) or "") + " " + ((owner_u["last_name"] if owner_u else None) or "")
    owner_name = owner_name.strip()

    with db_lock, get_conn() as conn:
        conn.execute("""
            INSERT INTO partner_bots
            (bot_token, bot_username, bot_name, owner_id, owner_username, owner_name,
             stars, plan, is_active, connected_at, last_ping)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (token, uname, name, added_by_id, owner_un, owner_name,
              STARS_REWARD["first_connect"], "free", 1, now_s, now_s))
        pb_id = conn.execute("SELECT id FROM partner_bots WHERE bot_token=?", (token,)).fetchone()[0]
        # Начисляем звёзды за первое подключение
        conn.execute("""
            INSERT INTO stars_log (user_id, bot_id, amount, reason, ts)
            VALUES (?,?,?,?,?)
        """, (added_by_id, pb_id, STARS_REWARD["first_connect"], "first_connect", now_s))

    # Запускаем бота сразу после подключения
    try:
        with get_conn() as conn:
            pb_new = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
        if pb_new:
            _launch_partner_bot(pb_new)
    except Exception:
        pass

    return f"✅ <b>Бот @{uname} подключён и запущен!</b>\n⭐ Начислено {STARS_REWARD['first_connect']} звёзд.\n<i>Теперь напиши своему боту /start</i>"


# ── /api команда ──────────────────────────────────────────────
@bot.message_handler(commands=["api"])
def cmd_api(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    _show_partners_panel(msg.chat.id, None, edit=False)


# ── Обработка state partner_add ───────────────────────────────
# (встраивается в handle_all_messages через user_states)
# Это уже добавлено ниже через патч user_states блока



# ═══════════════════════════════════════════════════════════════
#  ДВИЖОК ПАРТНЁРСКИХ БОТОВ
#  Каждый партнёрский бот работает в своём потоке,
#  обращается к общей БД и даёт партнёру пробивы.
# ═══════════════════════════════════════════════════════════════

_active_partner_bots:  dict = {}   # pb_id -> Thread
_active_partner_pbots: dict = {}   # pb_id -> TeleBot instance (для stop_polling)
_partner_bot_stop:     dict = {}   # pb_id -> bool


def _send_partner_probiv_card_to_pbot(pbot, chat_id_send, msg_id, result, query_str):
    """Карточка пробива для конкретного pbot экземпляра."""
    u    = result["user"]
    empty = (not u and not result["groups_in"]
             and result["msg_count"] == 0 and result["basa_count"] == 0)

    kb_back = pbot.types.InlineKeyboardMarkup(row_width=2)
    kb_back.row(
        pbot.types.InlineKeyboardButton("🔍 Ещё раз", callback_data="pb_probiv"),
        pbot.types.InlineKeyboardButton("◀️ Меню",    callback_data="pb_menu"),
    )

    if empty:
        try:
            pbot.edit_message_text(
                f"<code>🔍 ПРОБИВ: {escape_html(query_str)}</code>\n\n"
                f"❌ <b>Не найден.</b>\n<i>Пользователь не в базе.</i>",
                chat_id_send, msg_id, parse_mode="HTML", reply_markup=kb_back)
        except Exception:
            pass
        return

    lines = ["<code>┌──────────────────────────┐</code>",
             "<code>│  🔍  ПРОБИВ              │</code>",
             "<code>└──────────────────────────┘</code>\n"]

    if u:
        name = escape_html((f"{u['first_name'] or ''} {u['last_name'] or ''}").strip() or "—")
        un_  = f"@{u['username']}" if u["username"] else "—"
        lines.append(f"👤 <b>{name}</b>  {un_}")
        lines.append(f"🆔 <code>{u['id']}</code>")
        if u["is_banned"]:
            lines.append("🚫 <b>Забанен</b>")
        lines.append(f"📅 {(u['first_seen'] or '')[:10]} → {(u['last_seen'] or '')[:10]}")
    else:
        lines.append(f"👤 <b>{escape_html(query_str)}</b>")

    if result["phones"]:
        lines.append(f"\n📱 {', '.join(f'<code>{p}</code>' for p in result['phones'])}")

    nh = result.get("name_history", [])
    if len(nh) > 1:
        lines.append(f"📛 {' · '.join(escape_html(n) for n in nh[:4])}")

    lines.append("\n<code>──────────────────────────</code>")
    total = result["msg_count"] + result["basa_count"]
    lines.append(f"💬 Активность: <b>{total}</b> сообщений")

    if result["chats"]:
        top = result["chats"][0]
        lines.append(f"📝 Чатов: <b>{len(result['chats'])}</b>  Топ: {escape_html((top['chat_title'] or '?')[:20])} ({top['cnt']})")

    if result["groups_in"]:
        lines.append(f"👥 Групп: <b>{len(result['groups_in'])}</b>")
        for g in result["groups_in"][:4]:
            role = " 👑" if g["is_admin"] else ""
            lines.append(f"  ├ {escape_html((g['title'] or str(g['chat_id']))[:25])}{role}")

    lines.append("\n<code>──────────────────────────</code>")

    try:
        pbot.edit_message_text("\n".join(lines), chat_id_send, msg_id,
                               parse_mode="HTML", reply_markup=kb_back)
    except Exception as e:
        err = str(e)
        if "can't parse entities" in err:
            try:
                import re as _re3
                clean = _re3.sub(r'<[^>]+>', '', "\n".join(lines))
                pbot.edit_message_text(clean, chat_id_send, msg_id, reply_markup=kb_back)
            except Exception:
                pass


def _make_partner_bot_handler(pb_token, pb_id, owner_id):
    """Создаёт telebot.TeleBot для партнёрского бота с обработчиками."""
    import telebot as _tb

    pbot    = _tb.TeleBot(pb_token, parse_mode="HTML")
    pstates = {}   # user_id -> "probiv"

    # Attach types so we can use them in _send_partner_probiv_card_to_pbot
    pbot.types = _tb.types

    def _pb_safe_edit(text, cid, mid, kb=None):
        try:
            pbot.edit_message_text(text, cid, mid, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            if "not modified" not in str(e) and "too old" not in str(e):
                pass

    def _pb_menu(cid, uid, mid=None, edit=False):
        with get_conn() as conn:
            pb   = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
            if not pb or not pb["is_active"]:
                pbot.send_message(cid, "⛔ Бот временно отключён.")
                return
            stars = pb["stars"] or 0
            plan  = pb["plan"]  or "free"
            lim   = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            used  = conn.execute(
                "SELECT COUNT(*) FROM stars_log WHERE bot_id=? AND reason='probiv' AND ts LIKE ?",
                (pb_id, f"{today}%")).fetchone()[0]
        rem = max(0, lim["daily_probiv"] - used)
        pi  = {"free":"🆓","basic":"⚡","pro":"💎","admin":"👑"}.get(plan,"📋")
        text = (
            f"<code>┌─────────────────────────┐</code>\n"
            f"<code>│  🤖  М Е Н Ю            │</code>\n"
            f"<code>└─────────────────────────┘</code>\n\n"
            f"{pi} План: <b>{plan}</b>  |  ⭐ Звёзд: <b>{stars}</b>\n"
            f"🔍 Сегодня: <b>{used}</b> / {lim['daily_probiv']} (осталось: {rem})\n\n"
            f"<i>Введи @username / ID / телефон для пробива</i>"
        )
        kb = _tb.types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            _tb.types.InlineKeyboardButton("🔍 Пробив",     callback_data="pb_probiv"),
            _tb.types.InlineKeyboardButton("📊 Статистика", callback_data="pb_stats"),
        )
        kb.add(_tb.types.InlineKeyboardButton("ℹ️ Помощь", callback_data="pb_help"))
        if edit and mid:
            _pb_safe_edit(text, cid, mid, kb)
        else:
            pbot.send_message(cid, text, reply_markup=kb)

    @pbot.message_handler(commands=["start","menu"])
    def pb_start(msg):
        upsert_user(msg.from_user)
        with db_lock, get_conn() as conn:
            conn.execute("UPDATE partner_bots SET last_ping=? WHERE id=?",
                         (datetime.datetime.now().isoformat(timespec="seconds"), pb_id))
        _pb_menu(msg.chat.id, msg.from_user.id)

    @pbot.message_handler(content_types=["text","photo","video","document","audio","sticker","voice"])
    def pb_msg(msg):
        uid = msg.from_user.id if msg.from_user else 0
        if msg.from_user:
            upsert_user(msg.from_user)
        if uid and is_banned(uid):
            return

        state = pstates.get(uid)
        if state == "probiv":
            pstates.pop(uid, None)
            raw = (msg.text or "").strip().lstrip("@").strip()
            if not raw:
                pbot.send_message(msg.chat.id, "❌ Введи @username, ID или телефон.",
                    reply_markup=_tb.types.InlineKeyboardMarkup().add(
                        _tb.types.InlineKeyboardButton("◀️ Меню", callback_data="pb_menu")))
                return
            ok_lim, err_lim = _check_plan_limit(pb_id, "probiv")
            if not ok_lim:
                pbot.send_message(msg.chat.id, f"⛔ {err_lim}",
                    reply_markup=_tb.types.InlineKeyboardMarkup().add(
                        _tb.types.InlineKeyboardButton("◀️ Меню", callback_data="pb_menu")))
                return
            # Проверяем баланс звёзд
            with get_conn() as conn:
                pb_bal = conn.execute("SELECT stars FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
            if not pb_bal or pb_bal["stars"] < 1:
                pbot.send_message(msg.chat.id,
                    f"⛔ <b>Недостаточно звёзд</b>\n\nНа балансе: <b>{pb_bal['stars'] if pb_bal else 0} ⭐</b>\n\nПополни баланс у администратора для продолжения пробивов.",
                    reply_markup=_tb.types.InlineKeyboardMarkup().add(
                        _tb.types.InlineKeyboardButton("◀️ Меню", callback_data="pb_menu")))
                return
            tmp = pbot.send_message(msg.chat.id, "🔍 <b>Пробиваю...</b>")
            target_id = int(raw) if raw.lstrip("+-").isdigit() else None
            target_un = None if target_id else raw
            if raw.startswith("+") or (raw.isdigit() and len(raw) >= 10):
                with get_conn() as conn:
                    u_ph = conn.execute("SELECT id FROM users WHERE phone=?", (raw,)).fetchone()
                    if not u_ph:
                        u_ph = conn.execute("SELECT user_id as id FROM chat_members WHERE phone=?", (raw,)).fetchone()
                if u_ph:
                    target_id = u_ph["id"]; target_un = None
                else:
                    try:
                        pbot.edit_message_text("❌ Телефон не найден.", msg.chat.id, tmp.message_id,
                            parse_mode="HTML",
                            reply_markup=_tb.types.InlineKeyboardMarkup().add(
                                _tb.types.InlineKeyboardButton("◀️ Меню", callback_data="pb_menu")))
                    except Exception:
                        pass
                    return
            result = _probiv_user(target_id, target_un)
            u_res  = result["user"]
            if (u_res and u_res["id"] in ADMIN_IDS) or (target_id and target_id in ADMIN_IDS):
                try:
                    pbot.edit_message_text("🚫 <b>Доступ запрещён.</b>", msg.chat.id, tmp.message_id,
                        parse_mode="HTML",
                        reply_markup=_tb.types.InlineKeyboardMarkup().add(
                            _tb.types.InlineKeyboardButton("◀️ Меню", callback_data="pb_menu")))
                except Exception:
                    pass
                return
            now_s = datetime.datetime.now().isoformat(timespec="seconds")
            with db_lock, get_conn() as conn:
                conn.execute("INSERT INTO stars_log (user_id,bot_id,amount,reason,ts) VALUES (?,?,?,?,?)",
                             (uid, pb_id, -1, "probiv", now_s))
                conn.execute("UPDATE partner_bots SET stars=MAX(0, stars-1) WHERE id=?", (pb_id,))
            _send_partner_probiv_card_to_pbot(pbot, msg.chat.id, tmp.message_id, result, raw)
        else:
            _pb_menu(msg.chat.id, uid)

    @pbot.callback_query_handler(func=lambda c: True)
    def pb_cb(call):
        uid  = call.from_user.id
        data = call.data
        if data == "pb_menu":
            _pb_menu(call.message.chat.id, uid, call.message.message_id, edit=True)
        elif data == "pb_probiv":
            ok_lim, err_lim = _check_plan_limit(pb_id, "probiv")
            if not ok_lim:
                pbot.answer_callback_query(call.id, err_lim, show_alert=True)
                return
            with get_conn() as conn:
                pb = conn.execute("SELECT stars FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
                stars = pb["stars"] if pb else 0
            if stars < 1:
                pbot.answer_callback_query(call.id, f"⛔ Недостаточно звёзд ({stars}). Обратись к администратору.", show_alert=True)
                return
            pstates[uid] = "probiv"
            _pb_safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  🔍  П Р О Б И В         │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"Введи @username, ID или +телефон:\n\n"
                f"⭐ Звёзд: <b>{stars}</b>\n"
                f"<i>Данные администраторов скрыты</i>",
                call.message.chat.id, call.message.message_id,
                _tb.types.InlineKeyboardMarkup().add(
                    _tb.types.InlineKeyboardButton("❌ Отмена", callback_data="pb_menu")))
        elif data == "pb_stats":
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            with get_conn() as conn:
                pb     = conn.execute("SELECT * FROM partner_bots WHERE id=?", (pb_id,)).fetchone()
                total_ = conn.execute("SELECT COUNT(*) FROM stars_log WHERE bot_id=? AND reason='probiv'", (pb_id,)).fetchone()[0]
                today_ = conn.execute("SELECT COUNT(*) FROM stars_log WHERE bot_id=? AND reason='probiv' AND ts LIKE ?",
                                      (pb_id, f"{today}%")).fetchone()[0]
                stars  = pb["stars"] if pb else 0
                plan   = pb["plan"]  if pb else "free"
                lim    = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])
            _pb_safe_edit(
                f"<code>┌─────────────────────────┐</code>\n"
                f"<code>│  📊  СТАТИСТИКА         │</code>\n"
                f"<code>└─────────────────────────┘</code>\n\n"
                f"⭐ Звёзд: <b>{stars}</b>  |  📋 План: <b>{plan}</b>\n"
                f"Лимит: {lim['daily_probiv']}/день\n"
                f"🔍 Сегодня: <b>{today_}</b>\n"
                f"📊 Всего: <b>{total_}</b>",
                call.message.chat.id, call.message.message_id,
                _tb.types.InlineKeyboardMarkup().add(
                    _tb.types.InlineKeyboardButton("◀️ Назад", callback_data="pb_menu")))
        elif data == "pb_help":
            _pb_safe_edit(
                f"<code>┌──────────────────────────┐</code>\n"
                f"<code>│  ℹ️  ПОМОЩЬ              │</code>\n"
                f"<code>└──────────────────────────┘</code>\n\n"
                f"🔍 <b>Пробив</b> — поиск по базе\n"
                f"  @username / ID / +телефон\n"
                f"  Показывает: имя, группы, активность, телефоны\n\n"
                f"⭐ <b>Звёзды</b> — 1⭐ за пробив\n"
                f"  Если звёзды закончились — новые пробивы недоступны.\n"
                f"  Обратись к администратору для пополнения.\n\n"
                f"📋 <b>Планы (дневной лимит пробивов):</b>\n"
                f"  🆓 free — 5/день\n"
                f"  ⚡ basic — 50/день\n"
                f"  💎 pro — 500/день\n\n"
                f"📊 <b>Статистика</b> — показывает текущий баланс звёзд,\n"
                f"  план, лимит и количество пробивов сегодня.\n\n"
                f"<i>Для пополнения звёзд / смены плана — обратись к администратору</i>",
                call.message.chat.id, call.message.message_id,
                _tb.types.InlineKeyboardMarkup().add(
                    _tb.types.InlineKeyboardButton("◀️ Назад", callback_data="pb_menu")))
        try:
            pbot.answer_callback_query(call.id)
        except Exception:
            pass

    return pbot


def _launch_partner_bot(pb_row) -> bool:
    """Запустить партнёрский бот в отдельном потоке."""
    pb_id    = pb_row["id"]
    pb_token = pb_row["bot_token"]
    owner_id = pb_row["owner_id"] or 0

    if not pb_token or pb_token == BOT_TOKEN:
        return False
    # Если поток уже жив — не запускаем второй
    if pb_id in _active_partner_bots and _active_partner_bots[pb_id].is_alive():
        return False

    # Сбрасываем флаг остановки
    _partner_bot_stop.pop(pb_id, None)

    def _run():
        import time as _t
        while True:
            if _partner_bot_stop.get(pb_id):
                break
            try:
                pbot = _make_partner_bot_handler(pb_token, pb_id, owner_id)
                _active_partner_pbots[pb_id] = pbot   # сохраняем инстанс
                print(f"  🤖 pbot @{pb_row['bot_username']} (id={pb_id}) запущен")
                pbot.infinity_polling(timeout=20, long_polling_timeout=15)
            except Exception as e:
                err_str = str(e)
                if "409" in err_str or "Conflict" in err_str:
                    print(f"  ⚠️  pbot id={pb_id}: 409 Conflict, жду 30с...")
                    _t.sleep(30)
                else:
                    print(f"  ⚠️  pbot id={pb_id} ошибка: {e}")
            _active_partner_pbots.pop(pb_id, None)
            if _partner_bot_stop.get(pb_id):
                break
            _t.sleep(5)

    t = threading.Thread(target=_run, daemon=True, name=f"pbot_{pb_id}")
    _active_partner_bots[pb_id] = t
    t.start()
    return True


def _stop_partner_bot(pb_id: int):
    """Остановить партнёрский бот — сначала stop_polling, потом убиваем поток."""
    _partner_bot_stop[pb_id] = True
    # Останавливаем polling если инстанс доступен
    pbot_instance = _active_partner_pbots.pop(pb_id, None)
    if pbot_instance:
        try:
            pbot_instance.stop_polling()
        except Exception:
            pass
    _active_partner_bots.pop(pb_id, None)


def _launch_all_partner_bots():
    """При старте — запустить все активные партнёрские боты."""
    try:
        with get_conn() as conn:
            rows = conn.execute("SELECT * FROM partner_bots WHERE is_active=1").fetchall()
        launched = 0
        for row in rows:
            if _launch_partner_bot(row):
                launched += 1
        if launched:
            print(f"🤖 Партнёрских ботов запущено: {launched}")
    except Exception as e:
        print(f"⚠️  Ошибка запуска партнёрских ботов: {e}")


def _restart_all_partner_bots() -> dict:
    """Полный перезапуск всех активных партнёрских ботов."""
    import time as _time
    stats = {"stopped": 0, "launched": 0, "errors": 0, "names": []}

    with get_conn() as conn:
        all_rows = conn.execute("SELECT * FROM partner_bots WHERE is_active=1").fetchall()

    # Останавливаем все — через stop_polling чтобы поток реально завершился
    for pb_id in list(_active_partner_bots.keys()):
        try:
            _stop_partner_bot(pb_id)
            stats["stopped"] += 1
        except Exception:
            pass

    # Ждём завершения всех потоков (макс 8 сек на каждый)
    for pb_id, t in list(_active_partner_bots.items()):
        try:
            t.join(timeout=8)
        except Exception:
            pass
    _active_partner_bots.clear()
    _partner_bot_stop.clear()

    _time.sleep(1)

    # Запускаем заново
    for row in all_rows:
        try:
            ok = _launch_partner_bot(row)
            if ok:
                stats["launched"] += 1
                stats["names"].append(f"@{row['bot_username'] or str(row['id'])}")
            else:
                stats["errors"] += 1
        except Exception:
            stats["errors"] += 1

    return stats


if __name__ == "__main__":
    print("⏳ Шаг 1/6: init_db...")
    init_db()
    print("✅ Шаг 1/6: База данных инициализирована")
    print("⏳ Шаг 2/6: _load_banned_cache...")
    _load_banned_cache()
    print(f"✅ Шаг 2/6: Кэш банов: {len(_banned_cache)} записей")
    _load_staff_cache()
    print(f"✅ Кэш ролей: {len(_staff_cache)} сотрудников")
    print("⏳ Шаг 3/6: _live_restore_from_db...")
    _live_restore_from_db()
    print(f"✅ Шаг 3/6: Live-сессии восстановлены. Админы: {ADMIN_IDS}")
    print("⏳ Шаг 4/6: _try_restore_userbot...")
    _try_restore_userbot()
    print("✅ Шаг 4/6: Userbot запущен (или не нужен)")
    print("⏳ Шаг 5/6: _launch_all_partner_bots...")
    _launch_all_partner_bots()
    print("✅ Шаг 5/6: Партнёрские боты запущены")
    print("✅ Шаг 6/6: Запускаю polling...")
    bot.infinity_polling(
        timeout=30,
        long_polling_timeout=20,
        allowed_updates=["message","callback_query","my_chat_member","chat_member"]
    )