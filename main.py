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

# ============================================================
# КОНФИГ — переменные в Railway Variables
# ============================================================
TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "")
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
GROUP_CHAT_ID   = int(os.environ.get("GROUP_CHAT_ID", "0"))
OWNER_ID        = int(os.environ.get("OWNER_ID", "0"))

# GitHub
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")        # Personal Access Token
GITHUB_REPO     = os.environ.get("GITHUB_REPO", "")         # "username/repo-name"
GITHUB_BRANCH   = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_FILE     = os.environ.get("GITHUB_FILE", "main.py")  # файл бота в репо

# Railway (опционально — для статуса деплоя)
RAILWAY_TOKEN   = os.environ.get("RAILWAY_TOKEN", "")
RAILWAY_PROJECT = os.environ.get("RAILWAY_PROJECT_ID", "")

# ============================================================
# ХРАНИЛИЩЕ
# ============================================================
DB_FILE    = "/tmp/office.json"
TASKS_FILE = "/tmp/tasks.json"

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
        "from": from_name,
        "to": to_name,
        "description": description,
        "status": status,
        "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "comments": []
    }
    tasks.append(task)
    save_tasks(tasks)
    return task

def update_task_status(task_id, status, comment=""):
    tasks = load_tasks()
    for t in tasks:
        if t["id"] == task_id:
            t["status"] = status
            t["updated_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
            if comment:
                t["comments"].append({"text": comment, "at": t["updated_at"]})
    save_tasks(tasks)

# ============================================================
# GITHUB API
# ============================================================
GITHUB_API = "https://api.github.com"

async def github_get_file(path: str = None) -> dict:
    """Получить текущий код файла из GitHub"""
    file_path = path or GITHUB_FILE
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            content = base64.b64decode(data["content"]).decode("utf-8")
            return {"content": content, "sha": data["sha"], "path": file_path}
        return {"error": f"HTTP {r.status_code}: {r.text[:200]}"}

async def github_commit_file(new_content: str, commit_message: str, path: str = None) -> dict:
    """Закоммитить изменённый файл в GitHub → Railway автодеплоит"""
    file_path = path or GITHUB_FILE
    # Сначала получаем sha текущего файла
    current = await github_get_file(file_path)
    if "error" in current:
        return current

    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    payload = {
        "message": commit_message,
        "content": base64.b64encode(new_content.encode("utf-8")).decode("utf-8"),
        "sha": current["sha"],
        "branch": GITHUB_BRANCH
    }
    async with httpx.AsyncClient() as client:
        r = await client.put(url, headers=headers, json=payload)
        if r.status_code in (200, 201):
            data = r.json()
            return {
                "success": True,
                "commit_sha": data["commit"]["sha"][:8],
                "url": data["commit"]["html_url"]
            }
        return {"error": f"HTTP {r.status_code}: {r.text[:300]}"}

async def github_get_commits(count: int = 5) -> list:
    """Последние коммиты в репо"""
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/commits?per_page={count}&sha={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 200:
            commits = r.json()
            return [
                {
                    "sha": c["sha"][:7],
                    "message": c["commit"]["message"][:60],
                    "author": c["commit"]["author"]["name"],
                    "date": c["commit"]["author"]["date"][:16].replace("T", " ")
                }
                for c in commits
            ]
        return []

# ============================================================
# GROQ — AI-агенты
# ============================================================
def ask_agent(agent: dict, history: list, user_message: str, context_info: str = "") -> str:
    client = Groq(api_key=GROQ_API_KEY)

    # Специальный системный промпт для инженера с доступом к коду
    is_engineer = any(w in (agent.get("role", "") + agent.get("name", "")).lower()
                      for w in ["инженер", "разработ", "програм", "engineer", "developer", "код"])

    engineer_extra = ""
    if is_engineer:
        engineer_extra = (
            "\n\nУ тебя есть доступ к исходному коду бота на GitHub через команды:\n"
            "- Если нужно прочитать код: скажи 'READ_CODE'\n"
            "- Если нужно закоммитить исправление: опиши изменение и скажи 'PATCH: <описание>'\n"
            "- Ты можешь предлагать конкретные правки кода в блоках ```python\n"
            "Ты — единственный агент, кто может менять код бота.\n"
        )

    system = (
        f'Ты — AI-агент по имени "{agent["name"]}".\n\n'
        f'Твоя роль: {agent["role"]}\n\n'
        f'Зоны ответственности: {agent.get("responsibilities", "")}\n\n'
        f'{context_info}'
        f'{engineer_extra}'
        f'Отвечай чётко, по делу, по-русски. Ты часть AI-офиса. '
        f'Если другой агент обращается к тебе, отвечай как коллеге.'
    )
    messages = [{"role": "system", "content": system}]
    for m in history[-10:]:
        messages.append({"role": m["role"], "content": m["content"]})
    messages.append({"role": "user", "content": user_message})

    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=messages,
        max_tokens=1000,
        temperature=0.7,
    )
    return resp.choices[0].message.content

def extract_code_block(text: str) -> str | None:
    """Извлечь блок кода из ответа агента"""
    pattern = r"```(?:python)?\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL)
    return match.group(1).strip() if match else None

# ============================================================
# СОСТОЯНИЯ
# ============================================================
(WAIT_AGENT_NAME, WAIT_AGENT_ROLE, WAIT_AGENT_RESP,
 WAIT_CHAT_MSG, WAIT_TASK_DESC,
 WAIT_EDIT_ROLE, WAIT_EDIT_RESP,
 WAIT_CODE_PATCH, WAIT_CLOSE_TASK_ID) = range(9)

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
    agents = db.get("agents", {})
    buttons = []
    for name in agents:
        emoji = agents[name].get("emoji", "🤖")
        buttons.append([InlineKeyboardButton(f"{emoji} {name}", callback_data=f"{mode}:{name}")])
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="main")])
    return InlineKeyboardMarkup(buttons)

def back_kb(target="main"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=target)]])

def agent_actions_kb(name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Написать", callback_data=f"chat:{name}"),
         InlineKeyboardButton("📋 Задача", callback_data=f"newtask:{name}")],
        [InlineKeyboardButton("✏️ Роль", callback_data=f"editrole:{name}"),
         InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{name}")],
        [InlineKeyboardButton("🔙 Команда", callback_data="agents")],
    ])

def pick_emoji(name, role):
    t = (name + role).lower()
    if any(w in t for w in ["програм", "код", "developer", "python", "инженер", "engineer"]): return "👨‍💻"
    if any(w in t for w in ["менеджер", "управл", "директор", "руководит"]): return "👔"
    if any(w in t for w in ["маркет", "реклам", "smm", "контент"]): return "📣"
    if any(w in t for w in ["дизайн", "творч", "художник"]): return "🎨"
    if any(w in t for w in ["аналит", "данн", "исследован"]): return "🔬"
    if any(w in t for w in ["юрист", "право", "закон"]): return "⚖️"
    if any(w in t for w in ["финанс", "бухгалт", "деньг"]): return "💰"
    if any(w in t for w in ["копирайт", "писател", "текст"]): return "✍️"
    if any(w in t for w in ["ассистент", "помощник", "секретар"]): return "🧑‍💼"
    return "🤖"

def is_engineer_agent(agent: dict) -> bool:
    t = (agent.get("name", "") + agent.get("role", "")).lower()
    return any(w in t for w in ["инженер", "разработ", "програм", "engineer", "developer"])

# ============================================================
# ГЛАВНОЕ МЕНЮ
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = load_db()
    count = len(db.get("agents", {}))
    tasks = load_tasks()
    active = sum(1 for t in tasks if "✅" not in t["status"])
    text = (
        f"🏢 *AI-Офис*\n\n"
        f"👥 Агентов: *{count}* | 📋 Активных задач: *{active}*\n\n"
        f"Управляй командой:"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb())
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_kb())

async def get_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"ID чата: `{chat_id}`", parse_mode="Markdown")

# ============================================================
# GITHUB МЕНЮ
# ============================================================
async def github_menu_handler(query, context):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        await query.edit_message_text(
            "⚙️ *GitHub не настроен*\n\nДобавь в Railway Variables:\n"
            "`GITHUB_TOKEN` — Personal Access Token\n"
            "`GITHUB_REPO` — username/repo-name\n"
            "`GITHUB_BRANCH` — ветка (main)\n"
            "`GITHUB_FILE` — путь к файлу бота",
            parse_mode="Markdown",
            reply_markup=back_kb()
        )
        return

    await query.edit_message_text("⏳ Загружаю данные GitHub...", parse_mode="Markdown")
    commits = await github_get_commits(5)

    text = f"⚙️ *GitHub — {GITHUB_REPO}*\n📂 Ветка: `{GITHUB_BRANCH}`\n\n"
    if commits:
        text += "*Последние коммиты:*\n"
        for c in commits:
            text += f"`{c['sha']}` {c['message']}\n_{c['author']} · {c['date']}_\n\n"
    else:
        text += "❌ Не удалось загрузить коммиты\n"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Читать код", callback_data="gh_read"),
         InlineKeyboardButton("🔄 Статус деплоя", callback_data="gh_status")],
        [InlineKeyboardButton("🛠 Патч вручную", callback_data="gh_patch")],
        [InlineKeyboardButton("🔙 Назад", callback_data="main")],
    ])
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)

async def github_read_handler(query, context):
    await query.edit_message_text("⏳ Читаю код из GitHub...")
    result = await github_get_file()
    if "error" in result:
        await query.edit_message_text(f"❌ Ошибка: {result['error']}", reply_markup=back_kb("github_menu"))
        return
    code = result["content"]
    preview = code[:800] + ("..." if len(code) > 800 else "")
    text = (
        f"📄 *{result['path']}*\n"
        f"📏 Строк: {len(code.splitlines())} | Символов: {len(code)}\n\n"
        f"```python\n{preview}\n```"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 GitHub", callback_data="github_menu")]
    ])
    try:
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        await query.edit_message_text(f"Код загружен: {len(code)} символов, {len(code.splitlines())} строк", reply_markup=kb)

# ============================================================
# КНОПКИ
# ============================================================
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data

    if d == "main":
        await start(update, context)

    elif d == "agents":
        db = load_db()
        agents = db.get("agents", {})
        if not agents:
            await q.edit_message_text(
                "😕 Агентов пока нет. Добавь первого!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="main")]
                ])
            )
            return
        text = "👥 *Команда AI-агентов:*\n\n"
        for name, a in agents.items():
            emoji = a.get("emoji", "🤖")
            eng = " 🔧" if is_engineer_agent(a) else ""
            text += f"{emoji} *{name}*{eng} — {a['role'][:50]}\n"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=agents_kb("view"))

    elif d.startswith("view:"):
        name = d.split(":", 1)[1]
        db = load_db()
        a = db["agents"].get(name)
        if not a:
            await q.edit_message_text("❌ Агент не найден.", reply_markup=back_kb())
            return
        emoji = a.get("emoji", "🤖")
        tasks = load_tasks()
        my_tasks = [t for t in tasks if t["to"] == name]
        active = sum(1 for t in my_tasks if "✅" not in t["status"])
        eng_note = "\n🔧 _Имеет доступ к коду на GitHub_" if is_engineer_agent(a) else ""
        text = (
            f"{emoji} *{name}*{eng_note}\n\n"
            f"📌 *Роль:* {a['role']}\n\n"
            f"🎯 *Ответственности:*\n{a.get('responsibilities', '—')}\n\n"
            f"📋 Задач: {len(my_tasks)} | Активных: {active}\n"
            f"🧠 Модель: llama-3.3-70b (Groq)"
        )
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=agent_actions_kb(name))

    elif d == "add_agent":
        await q.edit_message_text(
            "➕ *Новый агент*\n\nКак его зовут?\n_Пример: Алиса, Макс, Инженер_",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_AGENT_NAME

    elif d.startswith("del:"):
        name = d.split(":", 1)[1]
        db = load_db()
        if name in db["agents"]:
            del db["agents"][name]
            save_db(db)
        await q.edit_message_text(f"🗑 *{name}* удалён.", parse_mode="Markdown", reply_markup=main_kb())

    elif d.startswith("editrole:"):
        name = d.split(":", 1)[1]
        context.user_data["editing_agent"] = name
        await q.edit_message_text(
            f"✏️ Новая роль для *{name}*:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_EDIT_ROLE

    elif d == "tasks":
        tasks = load_tasks()
        if not tasks:
            await q.edit_message_text(
                "📋 Задач нет.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="main")]
                ])
            )
            return
        text = "📋 *Задачник:*\n\n"
        for t in tasks[-10:][::-1]:
            text += f"{t['status']} *#{t['id']}* {t['description'][:40]}\n"
            text += f"   {t['from']} → {t['to']} · {t['created_at']}\n"
            if t.get("comments"):
                text += f"   💬 {t['comments'][-1]['text'][:50]}\n"
            text += "\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Закрыть задачу", callback_data="close_task")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main")]
        ])
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)

    elif d.startswith("newtask:"):
        name = d.split(":", 1)[1]
        context.user_data["task_to"] = name
        await q.edit_message_text(
            f"📋 Задача для *{name}*\n\nОпиши задачу:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_TASK_DESC

    elif d == "close_task":
        await q.edit_message_text(
            "✅ Введи *номер задачи* для закрытия:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        context.user_data["action"] = "close_task"

    elif d == "chat_select":
        db = load_db()
        if not db.get("agents"):
            await q.edit_message_text("😕 Нет агентов!", reply_markup=back_kb())
            return
        await q.edit_message_text(
            "💬 *С кем поговорить?*",
            parse_mode="Markdown",
            reply_markup=agents_kb("chat")
        )

    elif d.startswith("chat:"):
        name = d.split(":", 1)[1]
        db = load_db()
        agent = db["agents"].get(name)
        if not agent:
            await q.edit_message_text("❌ Агент не найден.", reply_markup=back_kb())
            return
        context.user_data["chat_agent"] = name
        context.user_data["chat_history"] = []
        emoji = agent.get("emoji", "🤖")
        eng_note = "\n🔧 _Может читать и патчить код бота_" if is_engineer_agent(agent) else ""
        await q.edit_message_text(
            f"{emoji} *{name}* на связи!{eng_note}\n\n_{agent['role'][:100]}_\n\n"
            f"Пиши. /start — выход.",
            parse_mode="Markdown"
        )
        return WAIT_CHAT_MSG

    elif d == "broadcast":
        db = load_db()
        if not db.get("agents"):
            await q.edit_message_text("😕 Нет агентов!", reply_markup=back_kb())
            return
        await q.edit_message_text(
            "📢 *Сообщение всей команде*\n\nВведи сообщение — все агенты ответят:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        context.user_data["action"] = "broadcast"

    elif d == "github_menu":
        await github_menu_handler(q, context)

    elif d == "gh_read":
        await github_read_handler(q, context)

    elif d == "gh_patch":
        await q.edit_message_text(
            "🛠 *Ручной патч кода*\n\nОпиши что нужно изменить — инженер сделает это сам.\n\n"
            "_Или вставь готовый Python-код в блоке ```python_",
            parse_mode="Markdown", reply_markup=back_kb("github_menu")
        )
        context.user_data["action"] = "manual_patch"

    elif d == "gh_status":
        text = (
            "🚀 *Статус деплоя*\n\n"
            "Railway автоматически деплоит при каждом пуше в GitHub.\n\n"
            "Схема:\n"
            "Агент пишет код → коммит в GitHub → Railway видит пуш → автодеплой → бот перезапускается\n\n"
            f"🔗 Репо: `{GITHUB_REPO}`\n"
            f"📂 Ветка: `{GITHUB_BRANCH}`"
        )
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=back_kb("github_menu"))

# ============================================================
# CONVERSATION: Добавление агента
# ============================================================
async def recv_agent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_name"] = update.message.text.strip()[:50]
    await update.message.reply_text(
        f"👍 *{context.user_data['new_name']}*\n\nОпиши роль:",
        parse_mode="Markdown"
    )
    return WAIT_AGENT_ROLE

async def recv_agent_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_role"] = update.message.text.strip()
    await update.message.reply_text(
        "🎯 Зоны ответственности _(или 'пропустить')_:",
        parse_mode="Markdown"
    )
    return WAIT_AGENT_RESP

async def recv_agent_resp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resp = update.message.text.strip()
    if resp.lower() == "пропустить":
        resp = ""
    name = context.user_data["new_name"]
    role = context.user_data["new_role"]
    emoji = pick_emoji(name, role)
    db = load_db()
    db["agents"][name] = {"name": name, "role": role, "responsibilities": resp, "emoji": emoji}
    save_db(db)

    if GROUP_CHAT_ID:
        try:
            await context.bot.send_message(
                GROUP_CHAT_ID,
                f"👋 Новый агент в команде!\n\n{emoji} *{name}*\n📌 {role}",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    eng_note = ""
    if is_engineer_agent({"name": name, "role": role}):
        eng_note = "\n\n🔧 _Агент определён как инженер и получит доступ к коду бота на GitHub._"

    await update.message.reply_text(
        f"✅ *{emoji} {name}* добавлен!{eng_note}",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )
    return ConversationHandler.END

# ============================================================
# CONVERSATION: Чат с агентом (с поддержкой GitHub)
# ============================================================
async def chat_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = update.message.text.strip()
    name = context.user_data.get("chat_agent")
    db = load_db()
    agent = db["agents"].get(name)
    if not agent:
        await update.message.reply_text("❌ Агент не найден.", reply_markup=main_kb())
        return ConversationHandler.END

    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    try:
        history = context.user_data.get("chat_history", [])
        all_agents = ", ".join(db["agents"].keys())
        ctx = f"Ты работаешь в AI-офисе. Коллеги: {all_agents}.\n"

        # Если инженер — прочитаем код для контекста при запросах
        code_context = ""
        if is_engineer_agent(agent) and any(w in user_msg.lower() for w in
                ["код", "баг", "ошибк", "исправ", "патч", "fix", "bug", "read", "покажи"]):
            await update.message.reply_text("📄 Читаю текущий код...")
            file_data = await github_get_file()
            if "error" not in file_data:
                code_context = f"\nТекущий код бота ({len(file_data['content'])} символов):\n```\n{file_data['content'][:3000]}\n```\n"
            ctx += code_context

        reply = ask_agent(agent, history, user_msg, ctx)

        history.append({"role": "user", "content": user_msg})
        history.append({"role": "assistant", "content": reply})
        context.user_data["chat_history"] = history[-20:]

        emoji = agent.get("emoji", "🤖")

        # Проверяем: агент предлагает патч кода?
        code_block = extract_code_block(reply)
        if is_engineer_agent(agent) and code_block and len(code_block) > 100:
            context.user_data["pending_patch"] = code_block
            context.user_data["pending_patch_agent"] = name

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Применить патч", callback_data="apply_patch"),
                 InlineKeyboardButton("❌ Отмена", callback_data=f"chat:{name}")],
                [InlineKeyboardButton("🏠 Меню", callback_data="main")]
            ])
            try:
                await update.message.reply_text(
                    f"{emoji} *{name}:*\n\n{reply[:1500]}\n\n"
                    f"⚠️ _Обнаружен блок кода. Применить патч к GitHub?_",
                    parse_mode="Markdown", reply_markup=kb
                )
            except Exception:
                await update.message.reply_text(f"{name}:\n{reply[:1000]}", reply_markup=kb)
        else:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Задачу", callback_data=f"newtask:{name}"),
                 InlineKeyboardButton("🏠 Меню", callback_data="main")]
            ])
            try:
                await update.message.reply_text(
                    f"{emoji} *{name}:*\n\n{reply}", parse_mode="Markdown", reply_markup=kb
                )
            except Exception:
                await update.message.reply_text(f"{emoji} {name}:\n{reply}", reply_markup=kb)

        # В группу
        if GROUP_CHAT_ID:
            try:
                await context.bot.send_message(
                    GROUP_CHAT_ID,
                    f"💬 *{name}* ответил:\n_{reply[:300]}_",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка: `{str(e)[:200]}`", parse_mode="Markdown")

    return WAIT_CHAT_MSG

# ============================================================
# ПРИМЕНИТЬ ПАТЧ — из кнопки
# ============================================================
async def apply_patch_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    patch_code = context.user_data.get("pending_patch")
    agent_name = context.user_data.get("pending_patch_agent", "Инженер")

    if not patch_code:
        await q.edit_message_text("❌ Нет патча для применения.", reply_markup=back_kb())
        return

    if not GITHUB_TOKEN or not GITHUB_REPO:
        await q.edit_message_text(
            "❌ GitHub не настроен. Добавь GITHUB_TOKEN и GITHUB_REPO в Railway Variables.",
            reply_markup=back_kb()
        )
        return

    await q.edit_message_text("⏳ Применяю патч...")

    commit_msg = f"fix: auto-patch by {agent_name} via AI Office bot"
    result = await github_commit_file(patch_code, commit_msg)

    if result.get("success"):
        task = add_task(agent_name, "Система", f"Патч кода: {commit_msg}", "✅ Задеплоено")
        text = (
            f"✅ *Патч применён!*\n\n"
            f"📝 Коммит: `{result['commit_sha']}`\n"
            f"🔗 [Посмотреть на GitHub]({result['url']})\n\n"
            f"🚀 Railway сейчас автоматически деплоит новую версию.\n"
            f"Бот перезапустится через ~1-2 минуты.\n\n"
            f"📋 Задача #{task['id']} зафиксирована."
        )
        context.user_data.pop("pending_patch", None)

        if GROUP_CHAT_ID:
            try:
                await context.bot.send_message(
                    GROUP_CHAT_ID,
                    f"🚀 *Деплой!*\n👨‍💻 {agent_name} закоммитил патч\n`{result['commit_sha']}` — {commit_msg}",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
    else:
        text = f"❌ Ошибка GitHub: {result.get('error', 'неизвестно')}"

    await q.edit_message_text(text, parse_mode="Markdown",
                               reply_markup=back_kb(), disable_web_page_preview=True)
    return ConversationHandler.END

# ============================================================
# CONVERSATION: Задача
# ============================================================
async def recv_task_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    desc = update.message.text.strip()
    to_name = context.user_data.get("task_to", "?")
    db = load_db()
    owner_name = update.effective_user.first_name or "Владелец"
    task = add_task(owner_name, to_name, desc)

    agent = db["agents"].get(to_name)
    reply_text = ""
    if agent:
        try:
            # Если инженер — дай ему контекст кода
            ctx = ""
            if is_engineer_agent(agent) and GITHUB_TOKEN:
                ctx = f"\nРепо: {GITHUB_REPO}, ветка: {GITHUB_BRANCH}, файл: {GITHUB_FILE}.\n"
            reply_text = ask_agent(
                agent, [], f"Задача #{task['id']}: {desc}. Прими и коротко опиши план.", ctx
            )
        except Exception:
            reply_text = "Принял задачу, приступаю."

    emoji = agent.get("emoji", "🤖") if agent else "🤖"
    text = (
        f"✅ *Задача #{task['id']} создана*\n\n"
        f"👤 → *{to_name}*\n📝 {desc}\n\n"
        f"{emoji} *{to_name}:* _{reply_text[:400]}_"
    )
    try:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb())
    except Exception:
        await update.message.reply_text(f"Задача #{task['id']} для {to_name}.\n{reply_text[:300]}",
                                         reply_markup=main_kb())

    if GROUP_CHAT_ID:
        try:
            await context.bot.send_message(
                GROUP_CHAT_ID,
                f"📋 *Задача #{task['id']}*\n{owner_name} → {emoji} {to_name}\n📝 {desc}\n\n"
                f"{emoji} *{to_name}:* _{reply_text[:200]}_",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    return ConversationHandler.END

# ============================================================
# CONVERSATION: Редактирование роли
# ============================================================
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
# СВОБОДНЫЙ ТЕКСТ вне диалога
# ============================================================
async def free_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.get("action")

    if action == "close_task":
        context.user_data.pop("action", None)
        try:
            task_id = int(update.message.text.strip())
            update_task_status(task_id, "✅ Выполнено")
            await update.message.reply_text(f"✅ Задача #{task_id} закрыта!", reply_markup=main_kb())
        except Exception:
            await update.message.reply_text("❌ Введи число — номер задачи.", reply_markup=main_kb())
        return

    if action == "manual_patch":
        context.user_data.pop("action", None)
        user_msg = update.message.text.strip()
        db = load_db()

        # Ищем инженера
        engineer = None
        for name, a in db["agents"].items():
            if is_engineer_agent(a):
                engineer = (name, a)
                break

        if not engineer:
            await update.message.reply_text(
                "❌ Нет агента-инженера. Добавь агента с ролью 'инженер/разработчик'.",
                reply_markup=main_kb()
            )
            return

        eng_name, eng_agent = engineer
        await update.message.reply_text(f"👨‍💻 {eng_name} анализирует запрос...")
        await context.bot.send_chat_action(update.effective_chat.id, "typing")

        # Читаем код
        file_data = await github_get_file()
        code_ctx = ""
        if "error" not in file_data:
            code_ctx = f"\nТекущий код:\n```\n{file_data['content'][:3000]}\n```\n"

        reply = ask_agent(
            eng_agent, [],
            f"Задача по коду: {user_msg}\n\n"
            f"Напиши ПОЛНЫЙ исправленный файл в блоке ```python. Не сокращай код.",
            code_ctx
        )

        code_block = extract_code_block(reply)
        if code_block and len(code_block) > 100:
            context.user_data["pending_patch"] = code_block
            context.user_data["pending_patch_agent"] = eng_name
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Применить", callback_data="apply_patch"),
                 InlineKeyboardButton("❌ Отмена", callback_data="main")],
            ])
            emoji = eng_agent.get("emoji", "👨‍💻")
            try:
                await update.message.reply_text(
                    f"{emoji} *{eng_name}:*\n\n{reply[:1200]}\n\n"
                    f"_Применить патч?_",
                    parse_mode="Markdown", reply_markup=kb
                )
            except Exception:
                await update.message.reply_text(f"{eng_name} готов применить патч.", reply_markup=kb)
        else:
            await update.message.reply_text(reply[:1500], reply_markup=main_kb())
        return

    if action == "broadcast":
        context.user_data.pop("action", None)
        db = load_db()
        agents = db.get("agents", {})
        if not agents:
            await update.message.reply_text("Нет агентов.", reply_markup=main_kb())
            return

        msg = update.message.text.strip()
        await update.message.reply_text("📢 Рассылаю команде...")

        for name, agent in agents.items():
            await context.bot.send_chat_action(update.effective_chat.id, "typing")
            try:
                reply = ask_agent(agent, [], f"Владелец всей команде: {msg}. Ответь коротко.")
                emoji = agent.get("emoji", "🤖")
                try:
                    await update.message.reply_text(f"{emoji} *{name}:*\n{reply}", parse_mode="Markdown")
                except Exception:
                    await update.message.reply_text(f"{emoji} {name}:\n{reply}")
                if GROUP_CHAT_ID:
                    try:
                        await context.bot.send_message(
                            GROUP_CHAT_ID, f"{emoji} *{name}:* {reply[:300]}", parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                await asyncio.sleep(0.5)
            except Exception as e:
                await update.message.reply_text(f"⚠️ {name}: {str(e)[:100]}")
        return

    await start(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.", reply_markup=main_kb())
    return ConversationHandler.END

# ============================================================
# ЗАПУСК
# ============================================================
def main():
    print("🏢 AI-Офис v2 запускается...")
    if not TELEGRAM_TOKEN:
        print("❌ Нет TELEGRAM_TOKEN!"); return
    if not GROQ_API_KEY:
        print("❌ Нет GROQ_API_KEY!"); return

    if GITHUB_TOKEN and GITHUB_REPO:
        print(f"✅ GitHub: {GITHUB_REPO} / {GITHUB_BRANCH} / {GITHUB_FILE}")
    else:
        print("⚠️  GitHub не настроен — self-patch недоступен")

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

    chat_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button, pattern="^chat:.+")],
        states={
            WAIT_CHAT_MSG: [
                CallbackQueryHandler(apply_patch_handler, pattern="^apply_patch$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, chat_msg),
                CallbackQueryHandler(button, pattern="^(main|newtask:|chat_select)"),
            ],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    task_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button, pattern="^newtask:.+")],
        states={
            WAIT_TASK_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_task_desc)],
        },
        fallbacks=[CommandHandler("start", start)],
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
    app.add_handler(chat_conv)
    app.add_handler(task_conv)
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(apply_patch_handler, pattern="^apply_patch$"))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, free_text))

    print("✅ AI-Офис v2 запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
