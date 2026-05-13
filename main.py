import json
import os
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, ConversationHandler
)
from groq import Groq

# ============================================================
# КОНФИГ — заполни в Railway Variables
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
# ID группового чата (после добавления бота в группу напиши /id в группе)
GROUP_CHAT_ID  = int(os.environ.get("GROUP_CHAT_ID", "0"))
# Твой личный Telegram user_id (узнай у @userinfobot)
OWNER_ID       = int(os.environ.get("OWNER_ID", "6091955295"))

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
# GROQ
# ============================================================
def ask_agent(agent: dict, history: list, user_message: str, context_info: str = "") -> str:
    client = Groq(api_key=GROQ_API_KEY)
    system = (
        f'Ты — AI-агент по имени "{agent["name"]}".\n\n'
        f'Твоя роль: {agent["role"]}\n\n'
        f'Зоны ответственности: {agent.get("responsibilities", "")}\n\n'
        f'{context_info}'
        f'Отвечай чётко, по делу, по-русски. Ты часть AI-офиса.'
    )
    messages = [{"role": "system", "content": system}]
    for m in history[-8:]:
        messages.append({"role": m["role"], "content": m["content"]})
    messages.append({"role": "user", "content": user_message})

    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=messages,
        max_tokens=800,
        temperature=0.7,
    )
    return resp.choices[0].message.content

# ============================================================
# СОСТОЯНИЯ
# ============================================================
(WAIT_AGENT_NAME, WAIT_AGENT_ROLE, WAIT_AGENT_RESP,
 WAIT_CHAT_MSG, WAIT_TASK_DESC, WAIT_TASK_TO,
 WAIT_EDIT_ROLE, WAIT_EDIT_RESP) = range(8)

# ============================================================
# КЛАВИАТУРЫ
# ============================================================
def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Мои агенты", callback_data="agents"),
         InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")],
        [InlineKeyboardButton("📋 Задачник", callback_data="tasks"),
         InlineKeyboardButton("💬 Чат с агентом", callback_data="chat_select")],
        [InlineKeyboardButton("📢 Написать всей команде", callback_data="broadcast")],
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

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Главное меню", callback_data="main")]])

def agent_actions_kb(name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Написать", callback_data=f"chat:{name}"),
         InlineKeyboardButton("📋 Дать задачу", callback_data=f"newtask:{name}")],
        [InlineKeyboardButton("✏️ Изменить роль", callback_data=f"editrole:{name}"),
         InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{name}")],
        [InlineKeyboardButton("🔙 Все агенты", callback_data="agents")],
    ])

def pick_emoji(name, role):
    t = (name + role).lower()
    if any(w in t for w in ["програм", "код", "developer", "python", "инженер"]): return "👨‍💻"
    if any(w in t for w in ["менеджер", "управл", "директор", "руководит"]): return "👔"
    if any(w in t for w in ["маркет", "реклам", "smm", "контент"]): return "📣"
    if any(w in t for w in ["дизайн", "творч", "художник"]): return "🎨"
    if any(w in t for w in ["аналит", "данн", "исследован"]): return "🔬"
    if any(w in t for w in ["юрист", "право", "закон"]): return "⚖️"
    if any(w in t for w in ["финанс", "бухгалт", "деньг"]): return "💰"
    if any(w in t for w in ["копирайт", "писател", "текст"]): return "✍️"
    if any(w in t for w in ["ассистент", "помощник", "секретар"]): return "🧑‍💼"
    return "🤖"

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
        f"👥 Агентов в команде: *{count}*\n"
        f"📋 Активных задач: *{active}*\n\n"
        f"Управляй своей AI-командой:"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb())
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_kb())

async def get_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /id — узнать ID чата"""
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"ID этого чата: `{chat_id}`", parse_mode="Markdown")

# ============================================================
# КНОПКИ
# ============================================================
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data

    # --- ГЛАВНОЕ МЕНЮ ---
    if d == "main":
        await start(update, context)

    # --- СПИСОК АГЕНТОВ ---
    elif d == "agents":
        db = load_db()
        agents = db.get("agents", {})
        if not agents:
            await q.edit_message_text("😕 Агентов пока нет. Добавь первого!", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")],
                [InlineKeyboardButton("🔙 Назад", callback_data="main")]
            ]))
            return
        text = "👥 *Команда AI-агентов:*\n\n"
        for name, a in agents.items():
            emoji = a.get("emoji", "🤖")
            text += f"{emoji} *{name}* — {a['role'][:50]}\n"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=agents_kb("view"))

    # --- ПРОСМОТР АГЕНТА ---
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
        text = (
            f"{emoji} *{name}*\n\n"
            f"📌 *Роль:* {a['role']}\n\n"
            f"🎯 *Зоны ответственности:*\n{a.get('responsibilities', '—')}\n\n"
            f"📋 Задач всего: {len(my_tasks)} | Активных: {active}"
        )
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=agent_actions_kb(name))

    # --- ДОБАВИТЬ АГЕНТА ---
    elif d == "add_agent":
        await q.edit_message_text(
            "➕ *Новый агент*\n\nКак зовут агента?\nПример: _Алиса_, _Макс_, _Инженер_",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_AGENT_NAME

    # --- УДАЛИТЬ АГЕНТА ---
    elif d.startswith("del:"):
        name = d.split(":", 1)[1]
        db = load_db()
        if name in db["agents"]:
            del db["agents"][name]
            save_db(db)
        await q.edit_message_text(f"🗑 Агент *{name}* удалён.", parse_mode="Markdown", reply_markup=main_kb())

    # --- ИЗМЕНИТЬ РОЛЬ ---
    elif d.startswith("editrole:"):
        name = d.split(":", 1)[1]
        context.user_data["editing_agent"] = name
        await q.edit_message_text(
            f"✏️ Введи новую роль для *{name}*:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_EDIT_ROLE

    # --- ЗАДАЧНИК ---
    elif d == "tasks":
        tasks = load_tasks()
        if not tasks:
            await q.edit_message_text("📋 Задач пока нет.", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Создать задачу", callback_data="newtask")],
                [InlineKeyboardButton("🔙 Назад", callback_data="main")]
            ]))
            return
        text = "📋 *Задачник:*\n\n"
        for t in tasks[-10:][::-1]:
            text += f"{t['status']} *#{t['id']}* {t['description'][:40]}\n"
            text += f"   👤 {t['from']} → {t['to']} | {t['created_at']}\n\n"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Новая задача", callback_data="newtask"),
             InlineKeyboardButton("✅ Закрыть задачу", callback_data="close_task")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main")]
        ])
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)

    # --- НОВАЯ ЗАДАЧА (без агента) ---
    elif d == "newtask":
        db = load_db()
        if not db.get("agents"):
            await q.edit_message_text("😕 Нет агентов. Сначала добавь агента!", reply_markup=back_kb())
            return
        await q.edit_message_text(
            "📋 *Новая задача*\n\nКому назначить?",
            parse_mode="Markdown",
            reply_markup=agents_kb("assignto")
        )

    # --- НОВАЯ ЗАДАЧА (с агентом) ---
    elif d.startswith("newtask:"):
        name = d.split(":", 1)[1]
        context.user_data["task_to"] = name
        await q.edit_message_text(
            f"📋 Задача для *{name}*\n\nОпиши задачу:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_TASK_DESC

    # --- НАЗНАЧИТЬ ЗАДАЧУ (выбор агента) ---
    elif d.startswith("assignto:"):
        name = d.split(":", 1)[1]
        context.user_data["task_to"] = name
        await q.edit_message_text(
            f"📋 Задача для *{name}*\n\nОпиши задачу:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        return WAIT_TASK_DESC

    # --- ЗАКРЫТЬ ЗАДАЧУ ---
    elif d == "close_task":
        await q.edit_message_text(
            "✅ Введи *номер задачи* для закрытия (например: 3):",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        context.user_data["action"] = "close_task"

    # --- ЧАТ: выбор агента ---
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

    # --- ЧАТ: начало ---
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
        await q.edit_message_text(
            f"{emoji} *{name}* на связи!\n\n_{agent['role'][:100]}_\n\nПиши. /start — выход.",
            parse_mode="Markdown"
        )
        return WAIT_CHAT_MSG

    # --- НАПИСАТЬ ВСЕЙ КОМАНДЕ ---
    elif d == "broadcast":
        db = load_db()
        if not db.get("agents"):
            await q.edit_message_text("😕 Нет агентов!", reply_markup=back_kb())
            return
        await q.edit_message_text(
            "📢 *Сообщение всей команде*\n\nВведи сообщение — все агенты его увидят и ответят:",
            parse_mode="Markdown", reply_markup=back_kb()
        )
        context.user_data["action"] = "broadcast"

# ============================================================
# CONVERSATION: Добавление агента
# ============================================================
async def recv_agent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_name"] = update.message.text.strip()[:50]
    await update.message.reply_text(
        f"👍 Имя: *{context.user_data['new_name']}*\n\nОпиши его роль:",
        parse_mode="Markdown"
    )
    return WAIT_AGENT_ROLE

async def recv_agent_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_role"] = update.message.text.strip()
    await update.message.reply_text(
        "🎯 Опиши зоны ответственности агента\n_(или напиши 'пропустить')_:",
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

    # Уведомление в группу если настроена
    if GROUP_CHAT_ID:
        try:
            bot = context.bot
            await bot.send_message(
                GROUP_CHAT_ID,
                f"👋 В команду добавлен новый агент!\n\n{emoji} *{name}*\n📌 {role}",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    await update.message.reply_text(
        f"✅ *{emoji} {name}* добавлен в команду!\n\n📌 {role}",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )
    return ConversationHandler.END

# ============================================================
# CONVERSATION: Чат с агентом
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
        # Контекст команды
        all_agents = ", ".join(db["agents"].keys())
        ctx = f"Ты работаешь в AI-офисе вместе с: {all_agents}.\n"
        reply = ask_agent(agent, history, user_msg, ctx)

        history.append({"role": "user", "content": user_msg})
        history.append({"role": "assistant", "content": reply})
        context.user_data["chat_history"] = history[-16:]

        emoji = agent.get("emoji", "🤖")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Создать задачу", callback_data=f"newtask:{name}"),
             InlineKeyboardButton("🏠 Меню", callback_data="main")]
        ])
        try:
            await update.message.reply_text(f"{emoji} *{name}:*\n\n{reply}", parse_mode="Markdown", reply_markup=kb)
        except Exception:
            await update.message.reply_text(f"{emoji} {name}:\n\n{reply}", reply_markup=kb)

        # Дублируем в группу если настроена
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
# CONVERSATION: Задача
# ============================================================
async def recv_task_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    desc = update.message.text.strip()
    to_name = context.user_data.get("task_to", "?")
    db = load_db()
    owner_name = update.effective_user.first_name or "Владелец"
    task = add_task(owner_name, to_name, desc)

    # Агент реагирует на задачу
    agent = db["agents"].get(to_name)
    reply_text = ""
    if agent:
        try:
            reply_text = ask_agent(
                agent, [], f"Тебе назначена задача: {desc}. Прими задачу и кратко скажи как будешь её выполнять."
            )
        except Exception:
            reply_text = "Принял задачу, приступаю к выполнению."

    emoji = agent.get("emoji", "🤖") if agent else "🤖"
    text = (
        f"✅ *Задача #{task['id']} создана*\n\n"
        f"👤 Кому: *{to_name}*\n"
        f"📝 {desc}\n\n"
        f"{emoji} *{to_name}:* _{reply_text[:300]}_"
    )

    try:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb())
    except Exception:
        await update.message.reply_text(f"Задача #{task['id']} создана для {to_name}.\n{to_name}: {reply_text[:300]}", reply_markup=main_kb())

    # В группу
    if GROUP_CHAT_ID:
        try:
            await context.bot.send_message(
                GROUP_CHAT_ID,
                f"📋 *Новая задача #{task['id']}*\n"
                f"👤 {owner_name} → {emoji} {to_name}\n"
                f"📝 {desc}\n\n"
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
# ТЕКСТОВЫЕ СООБЩЕНИЯ вне диалога (broadcast / close_task)
# ============================================================
async def free_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.get("action")

    # Закрытие задачи
    if action == "close_task":
        context.user_data.pop("action", None)
        try:
            task_id = int(update.message.text.strip())
            update_task_status(task_id, "✅ Выполнено")
            await update.message.reply_text(f"✅ Задача #{task_id} закрыта!", reply_markup=main_kb())
        except Exception:
            await update.message.reply_text("❌ Введи число — номер задачи.", reply_markup=main_kb())
        return

    # Broadcast всем агентам
    if action == "broadcast":
        context.user_data.pop("action", None)
        db = load_db()
        agents = db.get("agents", {})
        if not agents:
            await update.message.reply_text("Нет агентов.", reply_markup=main_kb())
            return

        msg = update.message.text.strip()
        await update.message.reply_text(f"📢 Отправляю сообщение команде...", reply_markup=main_kb())

        # Каждый агент отвечает
        for name, agent in agents.items():
            await context.bot.send_chat_action(update.effective_chat.id, "typing")
            try:
                reply = ask_agent(agent, [], f"Владелец написал всей команде: {msg}. Ответь коротко от своего лица.")
                emoji = agent.get("emoji", "🤖")
                try:
                    await update.message.reply_text(f"{emoji} *{name}:*\n{reply}", parse_mode="Markdown")
                except Exception:
                    await update.message.reply_text(f"{emoji} {name}:\n{reply}")

                # В группу
                if GROUP_CHAT_ID:
                    try:
                        await context.bot.send_message(
                            GROUP_CHAT_ID,
                            f"{emoji} *{name}:* {reply[:300]}",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                await asyncio.sleep(0.5)
            except Exception as e:
                await update.message.reply_text(f"⚠️ {name} не ответил: {str(e)[:100]}")
        return

    # Если ничего — показать меню
    await start(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.", reply_markup=main_kb())
    return ConversationHandler.END

# ============================================================
# ЗАПУСК
# ============================================================
def main():
    print("🏢 AI-Офис запускается...")
    if not TELEGRAM_TOKEN:
        print("❌ Нет TELEGRAM_TOKEN!"); return
    if not GROQ_API_KEY:
        print("❌ Нет GROQ_API_KEY!"); return

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Добавление агента
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

    # Чат с агентом
    chat_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button, pattern="^chat:.+")],
        states={
            WAIT_CHAT_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, chat_msg),
                CallbackQueryHandler(button, pattern="^(main|newtask:|chat_select)"),
            ],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    # Задача
    task_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button, pattern="^newtask:.+"),
            CallbackQueryHandler(button, pattern="^assignto:.+"),
        ],
        states={
            WAIT_TASK_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_task_desc)],
        },
        fallbacks=[CommandHandler("start", start)],
        per_user=True
    )

    # Редактирование роли
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
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, free_text))

    print("✅ AI-Офис запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
