import json
import os
import asyncio
import base64
import re
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, ConversationHandler
)
from groq import Groq
import httpx

TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "")
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
GROUP_CHAT_ID   = int(os.environ.get("GROUP_CHAT_ID", "0"))
OWNER_ID        = int(os.environ.get("OWNER_ID", "0"))
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = os.environ.get("GITHUB_REPO", "")
GITHUB_BRANCH   = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_FILE     = os.environ.get("GITHUB_FILE", "main.py")

DB_FILE    = "/tmp/office.json"
TASKS_FILE = "/tmp/tasks.json"

# ============================================================
# ХРАНИЛИЩЕ ПАТЧЕЙ — в памяти процесса, не в /tmp
# ============================================================
_PATCHES = {}  # user_id -> {"code": str, "agent": str}

def save_patch(user_id: int, code: str, agent_name: str):
    _PATCHES[user_id] = {"code": code, "agent": agent_name}

def load_patch(user_id: int):
    p = _PATCHES.get(user_id)
    if p:
        return p["code"], p["agent"]
    return None, None

def delete_patch(user_id: int):
    _PATCHES.pop(user_id, None)

# ============================================================
# БД
# ============================================================
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"agents": {}}

def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def load_tasks():
    if os.path.exists(TASKS_FILE):
        with open(TASKS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_tasks(tasks):
    with open(TASKS_FILE, "w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)

def add_task(from_name, to_name, description, status="🔄 В работе"):
    tasks = load_tasks()
    task = {
        "id": len(tasks) + 1,
        "from": from_name, "to": to_name,
        "description": description, "status": status,
        "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
    }
    tasks.append(task)
    save_tasks(tasks)
    return task

def update_task_status(task_id, status):
    tasks = load_tasks()
    for t in tasks:
        if t["id"] == task_id:
            t["status"] = status
            t["updated_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
    save_tasks(tasks)

# ============================================================
# GITHUB
# ============================================================
async def github_get_file(path=None):
    fp = path or GITHUB_FILE
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{fp}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 200:
            d = r.json()
            return {"content": base64.b64decode(d["content"]).decode("utf-8"), "sha": d["sha"]}
        return {"error": f"HTTP {r.status_code}"}

async def github_commit_file(new_content, commit_message):
    cur = await github_get_file()
    if "error" in cur:
        return cur
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    payload = {
        "message": commit_message,
        "content": base64.b64encode(new_content.encode()).decode(),
        "sha": cur["sha"], "branch": GITHUB_BRANCH
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.put(url, headers=headers, json=payload)
        if r.status_code in (200, 201):
            d = r.json()
            return {"success": True, "commit_sha": d["commit"]["sha"][:8], "url": d["commit"]["html_url"]}
        return {"error": f"HTTP {r.status_code}: {r.text[:200]}"}

async def github_get_commits(count=5):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits?per_page={count}&sha={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 200:
            return [{"sha": c["sha"][:7], "message": c["commit"]["message"][:60],
                     "author": c["commit"]["author"]["name"],
                     "date": c["commit"]["author"]["date"][:16].replace("T"," ")} for c in r.json()]
        return []

# ============================================================
# GROQ
# ============================================================
def ask_agent(agent, history, user_message, context_info=""):
    client = Groq(api_key=GROQ_API_KEY)
    t = (agent.get("name","") + agent.get("role","")).lower()
    eng = any(w in t for w in ["инженер","разработ","програм","engineer","developer"])
    eng_extra = (
        "\n\nКогда пишешь исправление кода — давай ПОЛНЫЙ файл целиком в блоке ```python. "
        "Не сокращай, не пиши '# остальной код без изменений'. Только полный файл.\n"
    ) if eng else ""
    system = (
        f'Ты — AI-агент "{agent["name"]}".\n'
        f'Роль: {agent["role"]}\n'
        f'Ответственности: {agent.get("responsibilities","")}\n'
        f'{context_info}{eng_extra}'
        f'Отвечай по-русски, чётко.'
    )
    msgs = [{"role":"system","content":system}]
    for m in history[-10:]:
        msgs.append({"role":m["role"],"content":m["content"]})
    msgs.append({"role":"user","content":user_message})
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile", messages=msgs, max_tokens=8000, temperature=0.7)
    return resp.choices[0].message.content

def extract_code(text):
    m = re.search(r"```(?:python)?\n(.*?)```", text, re.DOTALL)
    return m.group(1).strip() if m else None

def is_eng(agent):
    t = (agent.get("name","") + agent.get("role","")).lower()
    return any(w in t for w in ["инженер","разработ","програм","engineer","developer"])

def pick_emoji(name, role):
    t = (name+role).lower()
    if any(w in t for w in ["програм","код","developer","python","инженер","engineer"]): return "👨‍💻"
    if any(w in t for w in ["менеджер","управл","директор"]): return "👔"
    if any(w in t for w in ["маркет","реклам","smm","контент"]): return "📣"
    if any(w in t for w in ["дизайн","творч","художник"]): return "🎨"
    if any(w in t for w in ["аналит","данн","исследован"]): return "🔬"
    if any(w in t for w in ["юрист","право","закон"]): return "⚖️"
    if any(w in t for w in ["финанс","бухгалт"]): return "💰"
    if any(w in t for w in ["копирайт","писател","текст"]): return "✍️"
    return "🤖"

# ============================================================
# СОСТОЯНИЯ
# ============================================================
WAIT_AGENT_NAME, WAIT_AGENT_ROLE, WAIT_AGENT_RESP, WAIT_EDIT_ROLE = range(4)
MODE_CHAT="chat"; MODE_TASK="task"; MODE_BROADCAST="broadcast"
MODE_CLOSE="close"; MODE_PATCH="patch"

# ============================================================
# КЛАВИАТУРЫ
# ============================================================
def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Команда", callback_data="agents"),
         InlineKeyboardButton("➕ Агент", callback_data="add_agent")],
        [InlineKeyboardButton("📋 Задачник", callback_data="tasks"),
         InlineKeyboardButton("💬 Чат", callback_data="chat_select")],
        [InlineKeyboardButton("📢 Всей команде", callback_data="broadcast"),
         InlineKeyboardButton("⚙️ GitHub", callback_data="github_menu")],
    ])

def agents_kb(mode="view"):
    db = load_db()
    btns = [[InlineKeyboardButton(f"{a.get('emoji','🤖')} {n}", callback_data=f"{mode}:{n}")]
            for n,a in db.get("agents",{}).items()]
    btns.append([InlineKeyboardButton("🔙 Назад", callback_data="main")])
    return InlineKeyboardMarkup(btns)

def back_kb(t="main"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=t)]])

def agent_actions_kb(name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Написать", callback_data=f"chat:{name}"),
         InlineKeyboardButton("📋 Задача", callback_data=f"newtask:{name}")],
        [InlineKeyboardButton("✏️ Роль", callback_data=f"editrole:{name}"),
         InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{name}")],
        [InlineKeyboardButton("🔙 Команда", callback_data="agents")],
    ])

def patch_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Применить патч", callback_data="apply_patch"),
         InlineKeyboardButton("❌ Отмена", callback_data="cancel_patch")],
    ])

# ============================================================
# СТАРТ
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["mode"] = None
    db = load_db()
    count = len(db.get("agents",{}))
    tasks = load_tasks()
    active = sum(1 for t in tasks if "✅" not in t["status"])
    text = f"🏢 *AI-Офис v3*\n\n👥 Агентов: *{count}* | 📋 Активных задач: *{active}*\n\nУправляй командой:"
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb())
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_kb())

async def get_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"ID: `{update.effective_chat.id}`", parse_mode="Markdown")

# ============================================================
# КНОПКИ
# ============================================================
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data
    user_id = update.effective_user.id

    if d == "main":
        context.user_data["mode"] = None
        await start(update, context)

    elif d == "agents":
        db = load_db()
        agents = db.get("agents",{})
        if not agents:
            await q.edit_message_text("😕 Агентов нет.", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Добавить", callback_data="add_agent")],
                [InlineKeyboardButton("🔙 Назад", callback_data="main")]
            ])); return
        text = "👥 *Команда:*\n\n"
        for name,a in agents.items():
            text += f"{a.get('emoji','🤖')} *{name}* — {a['role'][:50]}\n"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=agents_kb("view"))

    elif d.startswith("view:"):
        name = d.split(":",1)[1]
        db = load_db()
        a = db["agents"].get(name)
        if not a:
            await q.edit_message_text("❌ Не найден.", reply_markup=back_kb()); return
        tasks = load_tasks()
        my = [t for t in tasks if t["to"]==name]
        eng = "\n🔧 _Доступ к коду на GitHub_" if is_eng(a) else ""
        text = (f"{a.get('emoji','🤖')} *{name}*{eng}\n\n"
                f"📌 *Роль:* {a['role']}\n\n"
                f"🎯 *Ответственности:*\n{a.get('responsibilities','—')}\n\n"
                f"📋 Задач: {len(my)} | Активных: {sum(1 for t in my if '✅' not in t['status'])}")
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=agent_actions_kb(name))

    elif d == "add_agent":
        context.user_data["mode"] = None
        await q.edit_message_text("➕ *Новый агент*\n\nКак его зовут?",
                                   parse_mode="Markdown", reply_markup=back_kb())
        return WAIT_AGENT_NAME

    elif d.startswith("del:"):
        name = d.split(":",1)[1]
        db = load_db()
        db["agents"].pop(name, None)
        save_db(db)
        await q.edit_message_text(f"🗑 *{name}* удалён.", parse_mode="Markdown", reply_markup=main_kb())

    elif d.startswith("editrole:"):
        name = d.split(":",1)[1]
        context.user_data["editing_agent"] = name
        context.user_data["mode"] = None
        await q.edit_message_text(f"✏️ Новая роль для *{name}*:",
                                   parse_mode="Markdown", reply_markup=back_kb())
        return WAIT_EDIT_ROLE

    elif d == "tasks":
        tasks = load_tasks()
        if not tasks:
            await q.edit_message_text("📋 Задач нет.", reply_markup=back_kb()); return
        text = "📋 *Задачник:*\n\n"
        for t in tasks[-10:][::-1]:
            text += f"{t['status']} *#{t['id']}* {t['description'][:40]}\n"
            text += f"   {t['from']} → {t['to']} · {t['created_at']}\n\n"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Закрыть задачу", callback_data="close_task")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main")]
        ]))

    elif d == "close_task":
        context.user_data["mode"] = MODE_CLOSE
        await q.edit_message_text("✅ Введи номер задачи:", reply_markup=back_kb())

    elif d.startswith("newtask:"):
        name = d.split(":",1)[1]
        context.user_data["mode"] = MODE_TASK
        context.user_data["task_to"] = name
        await q.edit_message_text(f"📋 Задача для *{name}*\n\nОпиши:",
                                   parse_mode="Markdown", reply_markup=back_kb())

    elif d == "chat_select":
        db = load_db()
        if not db.get("agents"):
            await q.edit_message_text("😕 Нет агентов!", reply_markup=back_kb()); return
        await q.edit_message_text("💬 *С кем?*", parse_mode="Markdown", reply_markup=agents_kb("chat"))

    elif d.startswith("chat:"):
        name = d.split(":",1)[1]
        db = load_db()
        agent = db["agents"].get(name)
        if not agent:
            await q.edit_message_text("❌ Не найден.", reply_markup=back_kb()); return
        context.user_data["mode"] = MODE_CHAT
        context.user_data["chat_agent"] = name
        context.user_data["chat_history"] = []
        emoji = agent.get("emoji","🤖")
        eng = "\n🔧 _Может патчить код_" if is_eng(agent) else ""
        await q.edit_message_text(
            f"{emoji} *{name}* на связи!{eng}\n\n_{agent['role'][:100]}_\n\nПиши. /start — выход.",
            parse_mode="Markdown")

    elif d == "broadcast":
        context.user_data["mode"] = MODE_BROADCAST
        await q.edit_message_text("📢 Введи сообщение — все агенты ответят:", reply_markup=back_kb())

    elif d == "github_menu":
        if not GITHUB_TOKEN or not GITHUB_REPO:
            await q.edit_message_text("⚙️ GitHub не настроен.\n\nДобавь `GITHUB_TOKEN` и `GITHUB_REPO` в Railway.",
                                       parse_mode="Markdown", reply_markup=back_kb()); return
        await q.edit_message_text("⏳ Загружаю...")
        commits = await github_get_commits(5)
        text = f"⚙️ *GitHub — {GITHUB_REPO}*\n📂 `{GITHUB_BRANCH}`\n\n"
        for c in commits:
            text += f"`{c['sha']}` {c['message']}\n_{c['author']} · {c['date']}_\n\n"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📄 Читать код", callback_data="gh_read"),
             InlineKeyboardButton("🛠 Патч вручную", callback_data="gh_patch")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main")],
        ]))

    elif d == "gh_read":
        await q.edit_message_text("⏳ Читаю...")
        result = await github_get_file()
        if "error" in result:
            await q.edit_message_text(f"❌ {result['error']}", reply_markup=back_kb("github_menu")); return
        code = result["content"]
        preview = code[:800] + ("..." if len(code)>800 else "")
        try:
            await q.edit_message_text(
                f"📄 *{GITHUB_FILE}* — {len(code.splitlines())} строк\n\n```python\n{preview}\n```",
                parse_mode="Markdown", reply_markup=back_kb("github_menu"))
        except Exception:
            await q.edit_message_text(f"Код: {len(code)} символов", reply_markup=back_kb("github_menu"))

    elif d == "gh_patch":
        context.user_data["mode"] = MODE_PATCH
        await q.edit_message_text("🛠 *Ручной патч*\n\nОпиши что исправить:",
                                   parse_mode="Markdown", reply_markup=back_kb("github_menu"))

    # ============================================================
    # ПРИМЕНИТЬ ПАТЧ
    # ============================================================
    elif d == "apply_patch":
        patch_code, agent_name = load_patch(user_id)
        if not patch_code:
            # Патч пропал (рестарт) — просим повторить
            await q.edit_message_text(
                "⚠️ Патч не найден в памяти (бот перезапустился).\n\n"
                "Пожалуйста, попроси агента снова написать код — и сразу нажми кнопку.",
                reply_markup=main_kb())
            return

        if not GITHUB_TOKEN or not GITHUB_REPO:
            await q.edit_message_text("❌ GitHub не настроен.", reply_markup=back_kb()); return

        await q.edit_message_text("⏳ Коммичу в GitHub...")
        commit_msg = f"fix: auto-patch by {agent_name} via AI Office"
        result = await github_commit_file(patch_code, commit_msg)

        if result.get("success"):
            delete_patch(user_id)
            add_task(agent_name, "Система", f"Патч: {commit_msg}", "✅ Задеплоено")
            text = (f"✅ *Патч применён!*\n\n"
                    f"📝 Коммит: `{result['commit_sha']}`\n"
                    f"🔗 [GitHub]({result['url']})\n\n"
                    f"🚀 Railway деплоит... ~1-2 мин.")
            if GROUP_CHAT_ID:
                try:
                    await context.bot.send_message(GROUP_CHAT_ID,
                        f"🚀 *Деплой!* {agent_name} закоммитил `{result['commit_sha']}`",
                        parse_mode="Markdown")
                except Exception: pass
        else:
            text = f"❌ Ошибка: {result.get('error','?')}"

        await q.edit_message_text(text, parse_mode="Markdown",
                                   reply_markup=back_kb(), disable_web_page_preview=True)

    elif d == "cancel_patch":
        delete_patch(user_id)
        context.user_data["mode"] = None
        await q.edit_message_text("❌ Патч отменён.", reply_markup=main_kb())

# ============================================================
# CONVERSATION: Добавление агента
# ============================================================
async def recv_agent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_name"] = update.message.text.strip()[:50]
    await update.message.reply_text(f"👍 *{context.user_data['new_name']}*\n\nРоль?", parse_mode="Markdown")
    return WAIT_AGENT_ROLE

async def recv_agent_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_role"] = update.message.text.strip()
    await update.message.reply_text("🎯 Ответственности _(или 'пропустить')_:", parse_mode="Markdown")
    return WAIT_AGENT_RESP

async def recv_agent_resp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resp = update.message.text.strip()
    if resp.lower() == "пропустить": resp = ""
    name = context.user_data["new_name"]
    role = context.user_data["new_role"]
    emoji = pick_emoji(name, role)
    db = load_db()
    db["agents"][name] = {"name":name,"role":role,"responsibilities":resp,"emoji":emoji}
    save_db(db)
    if GROUP_CHAT_ID:
        try:
            await context.bot.send_message(GROUP_CHAT_ID,
                f"👋 Новый агент!\n\n{emoji} *{name}*\n📌 {role}", parse_mode="Markdown")
        except Exception: pass
    await update.message.reply_text(f"✅ *{emoji} {name}* добавлен!", parse_mode="Markdown", reply_markup=main_kb())
    return ConversationHandler.END

async def recv_edit_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = context.user_data.get("editing_agent")
    new_role = update.message.text.strip()
    db = load_db()
    if name in db["agents"]:
        db["agents"][name]["role"] = new_role
        db["agents"][name]["emoji"] = pick_emoji(name, new_role)
        save_db(db)
    await update.message.reply_text(f"✅ Роль *{name}* обновлена!", parse_mode="Markdown", reply_markup=main_kb())
    return ConversationHandler.END

# ============================================================
# ЕДИНЫЙ ТЕКСТОВЫЙ ХЕНДЛЕР
# ============================================================
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.user_data.get("mode")
    user_id = update.effective_user.id
    msg = update.message.text.strip()

    if mode == MODE_CHAT:
        name = context.user_data.get("chat_agent")
        db = load_db()
        agent = db["agents"].get(name)
        if not agent:
            context.user_data["mode"] = None
            await update.message.reply_text("❌ Агент не найден.", reply_markup=main_kb()); return

        await context.bot.send_chat_action(update.effective_chat.id, "typing")
        try:
            history = context.user_data.get("chat_history", [])
            ctx = f"Коллеги: {', '.join(db['agents'].keys())}.\n"

            if is_eng(agent):
                await update.message.reply_text("📄 Читаю текущий код...")
                file_data = await github_get_file()
                if "error" not in file_data:
                    full_code = file_data['content']
                    ctx += (
                        f"\n⚠️ КРИТИЧЕСКИ ВАЖНО: Ты ОБЯЗАН вернуть ПОЛНЫЙ файл целиком в блоке ```python.\n"
                        f"ЗАПРЕЩЕНО писать '# остальной код без изменений' или сокращать файл.\n"
                        f"ЗАПРЕЩЕНО возвращать файл короче {len(full_code.splitlines()) if False else 'оригинала'} строк.\n"
                        f"Только добавляй своё изменение в нужное место, всё остальное оставляй как есть.\n\n"
                        f"ПОЛНЫЙ ТЕКУЩИЙ КОД:\n```python\n{full_code}\n```\n"
                    )

            reply = ask_agent(agent, history, msg, ctx)
            history.append({"role":"user","content":msg})
            history.append({"role":"assistant","content":reply})
            context.user_data["chat_history"] = history[-20:]

            emoji = agent.get("emoji","🤖")
            code_block = extract_code(reply)

            if is_eng(agent) and code_block and len(code_block) > 50:
                save_patch(user_id, code_block, name)
                try:
                    await update.message.reply_text(
                        f"{emoji} *{name}:*\n\n{reply[:1500]}\n\n⚠️ _Обнаружен код. Применить патч?_",
                        parse_mode="Markdown", reply_markup=patch_kb())
                except Exception:
                    await update.message.reply_text(f"{name}:\n{reply[:1000]}", reply_markup=patch_kb())
            else:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Задачу", callback_data=f"newtask:{name}"),
                     InlineKeyboardButton("🏠 Меню", callback_data="main")]
                ])
                try:
                    await update.message.reply_text(f"{emoji} *{name}:*\n\n{reply}",
                                                     parse_mode="Markdown", reply_markup=kb)
                except Exception:
                    await update.message.reply_text(f"{name}:\n{reply}", reply_markup=kb)

            if GROUP_CHAT_ID:
                try:
                    await context.bot.send_message(GROUP_CHAT_ID,
                        f"💬 *{name}:*\n_{reply[:300]}_", parse_mode="Markdown")
                except Exception: pass

        except Exception as e:
            await update.message.reply_text(f"⚠️ `{str(e)[:200]}`", parse_mode="Markdown")

    elif mode == MODE_TASK:
        context.user_data["mode"] = None
        to_name = context.user_data.get("task_to","?")
        db = load_db()
        owner = update.effective_user.first_name or "Владелец"
        task = add_task(owner, to_name, msg)
        agent = db["agents"].get(to_name)
        reply_text = "Принял, приступаю."
        if agent:
            try:
                ctx = f"Репо: {GITHUB_REPO}\n" if is_eng(agent) and GITHUB_TOKEN else ""
                reply_text = ask_agent(agent, [], f"Задача #{task['id']}: {msg}. Прими и опиши план.", ctx)
            except Exception: pass
        emoji = agent.get("emoji","🤖") if agent else "🤖"
        try:
            await update.message.reply_text(
                f"✅ *Задача #{task['id']}*\n👤 → *{to_name}*\n📝 {msg}\n\n{emoji} *{to_name}:* _{reply_text[:400]}_",
                parse_mode="Markdown", reply_markup=main_kb())
        except Exception:
            await update.message.reply_text(f"Задача #{task['id']} для {to_name}.", reply_markup=main_kb())
        if GROUP_CHAT_ID:
            try:
                await context.bot.send_message(GROUP_CHAT_ID,
                    f"📋 *Задача #{task['id']}*\n{owner} → {emoji} {to_name}\n📝 {msg}\n\n_{reply_text[:200]}_",
                    parse_mode="Markdown")
            except Exception: pass

    elif mode == MODE_CLOSE:
        context.user_data["mode"] = None
        try:
            task_id = int(msg)
            update_task_status(task_id, "✅ Выполнено")
            await update.message.reply_text(f"✅ Задача #{task_id} закрыта!", reply_markup=main_kb())
        except Exception:
            await update.message.reply_text("❌ Введи число.", reply_markup=main_kb())

    elif mode == MODE_BROADCAST:
        context.user_data["mode"] = None
        db = load_db()
        agents = db.get("agents",{})
        if not agents:
            await update.message.reply_text("Нет агентов.", reply_markup=main_kb()); return
        await update.message.reply_text("📢 Рассылаю...")
        for name, agent in agents.items():
            await context.bot.send_chat_action(update.effective_chat.id, "typing")
            try:
                reply = ask_agent(agent, [], f"Владелец всей команде: {msg}. Ответь коротко.")
                emoji = agent.get("emoji","🤖")
                try:
                    await update.message.reply_text(f"{emoji} *{name}:*\n{reply}", parse_mode="Markdown")
                except Exception:
                    await update.message.reply_text(f"{name}:\n{reply}")
                if GROUP_CHAT_ID:
                    try:
                        await context.bot.send_message(GROUP_CHAT_ID,
                            f"{emoji} *{name}:* {reply[:300]}", parse_mode="Markdown")
                    except Exception: pass
                await asyncio.sleep(0.5)
            except Exception as e:
                await update.message.reply_text(f"⚠️ {name}: {str(e)[:100]}")

    elif mode == MODE_PATCH:
        context.user_data["mode"] = None
        db = load_db()
        engineer = next(((n,a) for n,a in db["agents"].items() if is_eng(a)), None)
        if not engineer:
            await update.message.reply_text("❌ Нет агента-инженера.", reply_markup=main_kb()); return
        eng_name, eng_agent = engineer
        await update.message.reply_text(f"👨‍💻 {eng_name} анализирует...")
        await context.bot.send_chat_action(update.effective_chat.id, "typing")
        file_data = await github_get_file()
        ctx = ""
        if "error" not in file_data:
            full_code = file_data['content']
            ctx = (
                f"⚠️ КРИТИЧЕСКИ ВАЖНО: Ты ОБЯЗАН вернуть ПОЛНЫЙ файл целиком в блоке ```python.\n"
                f"ЗАПРЕЩЕНО писать '# остальной код без изменений' или сокращать файл.\n"
                f"Файл должен содержать ВСЕ строки оригинала плюс твои изменения.\n\n"
                f"ПОЛНЫЙ ТЕКУЩИЙ КОД:\n```python\n{full_code}\n```\n"
            )
        reply = ask_agent(eng_agent, [],
            f"Задача: {msg}\n\nВерни ПОЛНЫЙ исправленный файл в блоке ```python. Ни одну строку не удаляй.", ctx)
        code_block = extract_code(reply)
        if code_block and len(code_block) > 50:
            save_patch(user_id, code_block, eng_name)
            emoji = eng_agent.get("emoji","👨‍💻")
            try:
                await update.message.reply_text(
                    f"{emoji} *{eng_name}:*\n\n{reply[:1200]}\n\n_Применить патч?_",
                    parse_mode="Markdown", reply_markup=patch_kb())
            except Exception:
                await update.message.reply_text(f"{eng_name} готов патчить.", reply_markup=patch_kb())
        else:
            await update.message.reply_text(reply[:1500], reply_markup=main_kb())

    else:
        await start(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["mode"] = None
    await update.message.reply_text("Отменено.", reply_markup=main_kb())
    return ConversationHandler.END

# ============================================================
# ЗАПУСК
# ============================================================
def main():
    print("🏢 AI-Офис v3 запускается...")
    if not TELEGRAM_TOKEN: print("❌ Нет TELEGRAM_TOKEN!"); return
    if not GROQ_API_KEY: print("❌ Нет GROQ_API_KEY!"); return
    if GITHUB_TOKEN and GITHUB_REPO:
        print(f"✅ GitHub: {GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_FILE}")
    else:
        print("⚠️  GitHub не настроен")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    add_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button, pattern="^add_agent$")],
        states={
            WAIT_AGENT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_agent_name)],
            WAIT_AGENT_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_agent_role)],
            WAIT_AGENT_RESP: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_agent_resp)],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    edit_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button, pattern="^editrole:.+")],
        states={
            WAIT_EDIT_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_edit_role)],
        },
        fallbacks=[CommandHandler("start", start)],
        per_user=True
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("id", get_group_id))
    app.add_handler(add_conv)
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    print("✅ AI-Офис v3 запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
