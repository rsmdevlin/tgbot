import json
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, ConversationHandler
)
import google.generativeai as genai

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyDpg8K2RbbVuF7aUVeiakqmJ3abVHfygNI")

genai.configure(api_key=GEMINI_API_KEY)

AGENTS_FILE = "/tmp/agents.json"

def load_agents():
    if os.path.exists(AGENTS_FILE):
        with open(AGENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_agents(agents):
    with open(AGENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(agents, f, ensure_ascii=False, indent=2)

(WAITING_AGENT_NAME, WAITING_AGENT_TASK,
 WAITING_MESSAGE, WAITING_EDIT_NAME, WAITING_EDIT_TASK) = range(5)

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить агента", callback_data="add_agent")],
        [InlineKeyboardButton("🤖 Мои агенты", callback_data="list_agents")],
        [InlineKeyboardButton("💬 Поговорить с агентом", callback_data="chat_select")],
        [InlineKeyboardButton("❌ Удалить агента", callback_data="delete_select")],
    ])

def agents_list_keyboard(agents, prefix="chat"):
    buttons = []
    for name in agents:
        emoji = agents[name].get("emoji", "🤖")
        buttons.append([InlineKeyboardButton(f"{emoji} {name}", callback_data=f"{prefix}:{name}")])
    buttons.append([InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(buttons)

def back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]])

def pick_emoji(name, task):
    text = (name + task).lower()
    if any(w in text for w in ["код", "программ", "python", "developer", "разработ"]):
        return "👨‍💻"
    elif any(w in text for w in ["бизнес", "менеджер", "стратег", "продаж", "маркет"]):
        return "💼"
    elif any(w in text for w in ["копирайт", "текст", "писател", "контент", "автор"]):
        return "✍️"
    elif any(w in text for w in ["учител", "репетитор", "обучен"]):
        return "👨‍🏫"
    elif any(w in text for w in ["психолог", "коуч", "советник"]):
        return "🧠"
    elif any(w in text for w in ["дизайн", "творч", "художник"]):
        return "🎨"
    elif any(w in text for w in ["аналит", "данн", "исследован"]):
        return "🔬"
    elif any(w in text for w in ["юрист", "право", "закон"]):
        return "⚖️"
    elif any(w in text for w in ["финанс", "инвест", "деньг"]):
        return "💰"
    else:
        return "🤖"

async def send_main_menu(update, context):
    agents = load_agents()
    count = len(agents)
    user = update.effective_user
    text = (
        f"👋 Привет, *{user.first_name}*!\n\n"
        f"🧠 *AgentHub* — твой центр ИИ-агентов\n\n"
        f"📊 Агентов: *{count}*\n\n"
        f"Выбери действие:"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_main_menu(update, context)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "main_menu":
        await send_main_menu(update, context)

    elif data == "add_agent":
        await query.edit_message_text(
            "✨ *Создание агента*\n\nКак назвать агента?\nПример: _Программист_, _Копирайтер_, _Бизнес-советник_",
            parse_mode="Markdown",
            reply_markup=back_keyboard()
        )
        return WAITING_AGENT_NAME

    elif data == "list_agents":
        agents = load_agents()
        if not agents:
            await query.edit_message_text("😕 Агентов нет. Создай первого!", reply_markup=main_menu_keyboard())
            return
        text = "🤖 *Твои агенты:*\n\n"
        for name, d in agents.items():
            emoji = d.get("emoji", "🤖")
            preview = d["task"][:60] + "..." if len(d["task"]) > 60 else d["task"]
            text += f"{emoji} *{name}*\n_{preview}_\n\n"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Редактировать агента", callback_data="edit_select")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]
        ])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)

    elif data == "chat_select":
        agents = load_agents()
        if not agents:
            await query.edit_message_text("😕 Нет агентов. Создай первого!", reply_markup=main_menu_keyboard())
            return
        await query.edit_message_text(
            "💬 *Выбери агента:*",
            parse_mode="Markdown",
            reply_markup=agents_list_keyboard(agents, "chat")
        )

    elif data.startswith("chat:"):
        agent_name = data.split(":", 1)[1]
        agents = load_agents()
        agent = agents.get(agent_name)
        if not agent:
            await query.edit_message_text("❌ Агент не найден.", reply_markup=back_keyboard())
            return
        context.user_data["active_agent"] = agent_name
        context.user_data["chat_history"] = []
        emoji = agent.get("emoji", "🤖")
        await query.edit_message_text(
            f"{emoji} *{agent_name}* готов!\n\n"
            f"_{agent['task'][:120]}_\n\n"
            f"Напиши сообщение. /start — выход в меню.",
            parse_mode="Markdown"
        )
        return WAITING_MESSAGE

    elif data == "delete_select":
        agents = load_agents()
        if not agents:
            await query.edit_message_text("😕 Нет агентов.", reply_markup=main_menu_keyboard())
            return
        await query.edit_message_text(
            "❌ *Выбери агента для удаления:*",
            parse_mode="Markdown",
            reply_markup=agents_list_keyboard(agents, "delete")
        )

    elif data.startswith("delete:"):
        agent_name = data.split(":", 1)[1]
        agents = load_agents()
        if agent_name in agents:
            del agents[agent_name]
            save_agents(agents)
            await query.edit_message_text(
                f"✅ Агент *{agent_name}* удалён.",
                parse_mode="Markdown",
                reply_markup=main_menu_keyboard()
            )

    elif data == "edit_select":
        agents = load_agents()
        if not agents:
            await query.edit_message_text("Нет агентов.", reply_markup=main_menu_keyboard())
            return
        await query.edit_message_text(
            "✏️ *Выбери агента:*",
            parse_mode="Markdown",
            reply_markup=agents_list_keyboard(agents, "edit")
        )

    elif data.startswith("edit:"):
        agent_name = data.split(":", 1)[1]
        agents = load_agents()
        agent = agents.get(agent_name)
        emoji = agent.get("emoji", "🤖")
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Изменить имя", callback_data=f"editname:{agent_name}")],
            [InlineKeyboardButton("📋 Изменить задачу", callback_data=f"edittask:{agent_name}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="list_agents")]
        ])
        await query.edit_message_text(
            f"{emoji} *{agent_name}*\n\n_{agent['task']}_",
            parse_mode="Markdown",
            reply_markup=keyboard
        )

    elif data.startswith("editname:"):
        agent_name = data.split(":", 1)[1]
        context.user_data["editing_agent"] = agent_name
        await query.edit_message_text(
            f"✏️ Введи новое имя для *{agent_name}*:",
            parse_mode="Markdown",
            reply_markup=back_keyboard()
        )
        return WAITING_EDIT_NAME

    elif data.startswith("edittask:"):
        agent_name = data.split(":", 1)[1]
        context.user_data["editing_agent"] = agent_name
        await query.edit_message_text(
            f"📋 Введи новую задачу для *{agent_name}*:",
            parse_mode="Markdown",
            reply_markup=back_keyboard()
        )
        return WAITING_EDIT_TASK

async def receive_agent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()[:50]
    context.user_data["new_agent_name"] = name
    await update.message.reply_text(
        f"👍 Имя: *{name}*\n\n"
        f"Теперь опиши его задачу и роль:\n\n"
        f"Пример: _Ты опытный Python разработчик. Помогаешь с кодом, находишь баги, пишешь чистые решения._",
        parse_mode="Markdown"
    )
    return WAITING_AGENT_TASK

async def receive_agent_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    task = update.message.text.strip()
    name = context.user_data.get("new_agent_name", "Агент")
    emoji = pick_emoji(name, task)
    agents = load_agents()
    agents[name] = {"task": task, "emoji": emoji}
    save_agents(agents)
    await update.message.reply_text(
        f"✅ *Агент создан!*\n\n{emoji} *{name}*\n_{task[:150]}_",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )
    return ConversationHandler.END

async def chat_with_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text.strip()
    agent_name = context.user_data.get("active_agent")
    if not agent_name:
        await update.message.reply_text("❌ Выбери агента!", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    agents = load_agents()
    agent = agents.get(agent_name)
    if not agent:
        await update.message.reply_text("❌ Агент не найден.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    try:
        history = context.user_data.get("chat_history", [])
        system_prompt = (
            f'Ты — ИИ-агент по имени "{agent_name}".\n\n'
            f'{agent["task"]}\n\n'
            f'Отвечай по-русски, будь конкретным и полезным.'
        )

        model = genai.GenerativeModel(model_name="gemini-2.0-flash")

        # Системный промпт передаём через первые сообщения истории
        gemini_history = [
            {"role": "user", "parts": [f"Запомни свою роль и придерживайся её всегда:\n\n{system_prompt}"]},
            {"role": "model", "parts": ["Понял, запомнил свою роль. Готов работать!"]},
        ]
        for m in history[-10:]:
            gemini_history.append({"role": m["role"], "parts": [m["content"]]})

        chat = model.start_chat(history=gemini_history)
        response = chat.send_message(user_message)
        reply_text = response.text

        history.append({"role": "user", "content": user_message})
        history.append({"role": "model", "content": reply_text})
        context.user_data["chat_history"] = history[-20:]

        emoji = agent.get("emoji", "🤖")
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Сменить агента", callback_data="chat_select"),
             InlineKeyboardButton("🏠 Меню", callback_data="main_menu")]
        ])
        await update.message.reply_text(
            f"{emoji} *{agent_name}:*\n\n{reply_text}",
            parse_mode="Markdown",
            reply_markup=keyboard
        )

    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Ошибка: `{str(e)[:300]}`",
            parse_mode="Markdown"
        )

    return WAITING_MESSAGE

async def receive_edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_name = update.message.text.strip()
    old_name = context.user_data.get("editing_agent")
    agents = load_agents()
    if old_name in agents:
        agents[new_name] = agents.pop(old_name)
        save_agents(agents)
        await update.message.reply_text(
            f"✅ Переименован: *{old_name}* → *{new_name}*",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
    return ConversationHandler.END

async def receive_edit_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_task = update.message.text.strip()
    agent_name = context.user_data.get("editing_agent")
    agents = load_agents()
    if agent_name in agents:
        agents[agent_name]["task"] = new_task
        save_agents(agents)
        await update.message.reply_text(
            f"✅ Задача *{agent_name}* обновлена!",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

def main():
    print("🚀 AgentHub Bot запускается...")
    if not TELEGRAM_TOKEN:
        print("❌ ОШИБКА: Не задан TELEGRAM_TOKEN в переменных окружения!")
        return

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    create_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^add_agent$")],
        states={
            WAITING_AGENT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_agent_name)],
            WAITING_AGENT_TASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_agent_task)],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    chat_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^chat:.+")],
        states={
            WAITING_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, chat_with_agent),
                CallbackQueryHandler(button_handler, pattern="^(chat:|chat_select|main_menu)"),
            ],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    edit_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_handler, pattern="^editname:.+"),
            CallbackQueryHandler(button_handler, pattern="^edittask:.+"),
        ],
        states={
            WAITING_EDIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_edit_name)],
            WAITING_EDIT_TASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_edit_task)],
        },
        fallbacks=[CommandHandler("start", start)],
        per_user=True
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(create_conv)
    app.add_handler(chat_conv)
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(button_handler))

    print("✅ Бот запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
