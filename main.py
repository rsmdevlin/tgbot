import asyncio
import json
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, ConversationHandler
)
import google.generativeai as genai

# =============================================
# 🔑 ТОКЕНЫ — ВСТАВЬ СВОИ ЗДЕСЬ
# =============================================
TELEGRAM_TOKEN = "8634098100:AAE6U_6LoSd3bWf91-H9c9s8cu32p4midZE"
GEMINI_API_KEY = "AIzaSyAhJSSYCwQEoB25QumQ6kdo7eZosxPaysw"

# =============================================
# Настройка Gemini
# =============================================
genai.configure(api_key=GEMINI_API_KEY)

# =============================================
# Хранилище агентов (JSON файл)
# =============================================
AGENTS_FILE = "agents.json"

def load_agents():
    if os.path.exists(AGENTS_FILE):
        with open(AGENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_agents(agents):
    with open(AGENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(agents, f, ensure_ascii=False, indent=2)

# Состояния диалога
(WAITING_AGENT_NAME, WAITING_AGENT_TASK, WAITING_AGENT_MODEL,
 WAITING_MESSAGE, WAITING_EDIT_NAME, WAITING_EDIT_TASK) = range(6)

# =============================================
# КЛАВИАТУРЫ
# =============================================
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

# =============================================
# КОМАНДЫ
# =============================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    agents = load_agents()
    count = len(agents)
    
    text = (
        f"👋 Привет, *{user.first_name}*!\n\n"
        f"🧠 *AgentHub* — твой личный центр управления ИИ-агентами.\n\n"
        f"Ты можешь создавать агентов с разными задачами:\n"
        f"• 👨‍💻 Программист\n"
        f"• 📋 Бизнес-ассистент\n"
        f"• ✍️ Копирайтер\n"
        f"• 🔬 Исследователь\n"
        f"• ... и любой другой!\n\n"
        f"📊 У тебя сейчас *{count}* агент(ов).\n\n"
        f"Выбери действие:"
    )
    
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Как пользоваться AgentHub:*\n\n"
        "1️⃣ Нажми *Добавить агента*\n"
        "2️⃣ Дай ему имя (например: Программист)\n"
        "3️⃣ Опиши его задачу (роль, стиль, специализацию)\n"
        "4️⃣ Иди в *Поговорить с агентом* и выбери нужного\n\n"
        "🔧 *Команды:*\n"
        "/start — главное меню\n"
        "/agents — список агентов\n"
        "/help — помощь\n\n"
        "💡 *Совет:* Чем точнее описание задачи агента, тем лучше он работает!"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=back_keyboard())

async def agents_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    agents = load_agents()
    if not agents:
        await update.message.reply_text(
            "😕 У тебя пока нет агентов.\nСоздай первого!",
            reply_markup=main_menu_keyboard()
        )
        return
    
    text = "🤖 *Твои агенты:*\n\n"
    for name, data in agents.items():
        emoji = data.get("emoji", "🤖")
        task_preview = data["task"][:80] + "..." if len(data["task"]) > 80 else data["task"]
        text += f"{emoji} *{name}*\n_{task_preview}_\n\n"
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

# =============================================
# CALLBACK HANDLERS
# =============================================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "main_menu":
        await start(update, context)

    elif data == "add_agent":
        await query.edit_message_text(
            "✨ *Создание нового агента*\n\n"
            "Как назвать агента?\n"
            "Примеры: _Программист_, _Бизнес-советник_, _Копирайтер_, _Личный тренер_",
            parse_mode="Markdown",
            reply_markup=back_keyboard()
        )
        return WAITING_AGENT_NAME

    elif data == "list_agents":
        agents = load_agents()
        if not agents:
            await query.edit_message_text(
                "😕 Агентов пока нет. Создай первого!",
                reply_markup=main_menu_keyboard()
            )
            return
        
        text = "🤖 *Твои агенты:*\n\n"
        for name, d in agents.items():
            emoji = d.get("emoji", "🤖")
            task_preview = d["task"][:60] + "..." if len(d["task"]) > 60 else d["task"]
            text += f"{emoji} *{name}*\n_{task_preview}_\n\n"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_select")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]
        ])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)

    elif data == "chat_select":
        agents = load_agents()
        if not agents:
            await query.edit_message_text(
                "😕 Нет агентов для чата. Сначала создай агента!",
                reply_markup=main_menu_keyboard()
            )
            return
        await query.edit_message_text(
            "💬 *Выбери агента для разговора:*",
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
            f"{emoji} *{agent_name}* готов к работе!\n\n"
            f"📋 Задача: _{agent['task'][:100]}_\n\n"
            f"Напиши своё сообщение. Для выхода: /start",
            parse_mode="Markdown"
        )
        return WAITING_MESSAGE

    elif data == "delete_select":
        agents = load_agents()
        if not agents:
            await query.edit_message_text("😕 Нет агентов для удаления.", reply_markup=main_menu_keyboard())
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
        else:
            await query.edit_message_text("❌ Агент не найден.", reply_markup=back_keyboard())

    elif data == "edit_select":
        agents = load_agents()
        if not agents:
            await query.edit_message_text("😕 Нет агентов.", reply_markup=main_menu_keyboard())
            return
        await query.edit_message_text(
            "✏️ *Выбери агента для редактирования:*",
            parse_mode="Markdown",
            reply_markup=agents_list_keyboard(agents, "edit")
        )

    elif data.startswith("edit:"):
        agent_name = data.split(":", 1)[1]
        context.user_data["editing_agent"] = agent_name
        agents = load_agents()
        agent = agents.get(agent_name)
        emoji = agent.get("emoji", "🤖")
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Изменить имя", callback_data=f"editname:{agent_name}")],
            [InlineKeyboardButton("📋 Изменить задачу", callback_data=f"edittask:{agent_name}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="list_agents")]
        ])
        await query.edit_message_text(
            f"{emoji} *{agent_name}*\n\n📋 _{agent['task']}_",
            parse_mode="Markdown",
            reply_markup=keyboard
        )

    elif data.startswith("editname:"):
        agent_name = data.split(":", 1)[1]
        context.user_data["editing_agent"] = agent_name
        await query.edit_message_text(
            f"✏️ Введи новое имя для агента *{agent_name}*:",
            parse_mode="Markdown",
            reply_markup=back_keyboard()
        )
        return WAITING_EDIT_NAME

    elif data.startswith("edittask:"):
        agent_name = data.split(":", 1)[1]
        context.user_data["editing_agent"] = agent_name
        await query.edit_message_text(
            f"📋 Введи новую задачу для агента *{agent_name}*:",
            parse_mode="Markdown",
            reply_markup=back_keyboard()
        )
        return WAITING_EDIT_TASK

# =============================================
# CONVERSATION HANDLERS — Создание агента
# =============================================
async def receive_agent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if len(name) > 50:
        await update.message.reply_text("❌ Имя слишком длинное (макс. 50 символов). Попробуй ещё раз:")
        return WAITING_AGENT_NAME
    
    context.user_data["new_agent_name"] = name
    await update.message.reply_text(
        f"👍 Отлично! Агент будет называться *{name}*.\n\n"
        f"📋 Теперь опиши его *задачу и роль*:\n\n"
        f"Примеры:\n"
        f"• _Ты опытный Python-разработчик. Помогаешь с кодом, объясняешь ошибки, пишешь чистый код._\n"
        f"• _Ты бизнес-советник. Помогаешь с стратегией, анализом рынка, планированием._\n"
        f"• _Ты копирайтер. Пишешь цепляющие тексты, посты, рекламу._",
        parse_mode="Markdown"
    )
    return WAITING_AGENT_TASK

async def receive_agent_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    task = update.message.text.strip()
    name = context.user_data.get("new_agent_name", "Агент")
    
    # Автоматически подбираем эмодзи по ключевым словам
    emoji = pick_emoji(name, task)
    
    agents = load_agents()
    agents[name] = {
        "task": task,
        "emoji": emoji,
        "created_at": str(asyncio.get_event_loop().time())
    }
    save_agents(agents)
    
    await update.message.reply_text(
        f"✅ *Агент создан!*\n\n"
        f"{emoji} *{name}*\n"
        f"📋 _{task[:150]}_\n\n"
        f"Теперь можешь поговорить с ним!",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )
    return ConversationHandler.END

def pick_emoji(name: str, task: str) -> str:
    text = (name + task).lower()
    if any(w in text for w in ["код", "программ", "python", "developer", "разработ", "debug"]):
        return "👨‍💻"
    elif any(w in text for w in ["бизнес", "менеджер", "стратег", "продаж", "маркет"]):
        return "💼"
    elif any(w in text for w in ["копирайт", "текст", "писател", "контент", "автор"]):
        return "✍️"
    elif any(w in text for w in ["учител", "репетитор", "обучен", "образован"]):
        return "👨‍🏫"
    elif any(w in text for w in ["психолог", "коуч", "советник", "помощн"]):
        return "🧠"
    elif any(w in text for w in ["дизайн", "творч", "художник", "креатив"]):
        return "🎨"
    elif any(w in text for w in ["аналит", "данн", "исследован", "наук"]):
        return "🔬"
    elif any(w in text for w in ["юрист", "право", "закон"]):
        return "⚖️"
    elif any(w in text for w in ["медицин", "врач", "здоров"]):
        return "🏥"
    elif any(w in text for w in ["финанс", "инвест", "деньг", "бухгалт"]):
        return "💰"
    else:
        return "🤖"

# =============================================
# CONVERSATION HANDLER — Чат с агентом
# =============================================
async def chat_with_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text.strip()
    agent_name = context.user_data.get("active_agent")
    
    if not agent_name:
        await update.message.reply_text("❌ Сначала выбери агента!", reply_markup=main_menu_keyboard())
        return ConversationHandler.END
    
    agents = load_agents()
    agent = agents.get(agent_name)
    
    if not agent:
        await update.message.reply_text("❌ Агент не найден.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    # Отправляем "печатает..."
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    
    try:
        # Строим историю чата
        history = context.user_data.get("chat_history", [])
        
        # Системный промпт
        system_prompt = f"""Ты — ИИ-агент по имени "{agent_name}".

Твоя роль и задача:
{agent['task']}

Отвечай строго в соответствии со своей ролью. Будь полезным, конкретным и профессиональным.
Общайся на русском языке, если пользователь пишет по-русски."""

        # Создаём модель
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            system_instruction=system_prompt
        )
        
        # Строим историю для Gemini
        gemini_history = []
        for msg in history[-10:]:  # последние 10 сообщений
            gemini_history.append({
                "role": msg["role"],
                "parts": [msg["content"]]
            })
        
        chat = model.start_chat(history=gemini_history)
        response = chat.send_message(user_message)
        reply_text = response.text
        
        # Сохраняем в историю
        history.append({"role": "user", "content": user_message})
        history.append({"role": "model", "content": reply_text})
        context.user_data["chat_history"] = history[-20:]  # храним 20 сообщений
        
        emoji = agent.get("emoji", "🤖")
        
        # Кнопки под ответом
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
            f"⚠️ Ошибка при обращении к ИИ:\n`{str(e)[:200]}`\n\nПопробуй ещё раз.",
            parse_mode="Markdown"
        )
    
    return WAITING_MESSAGE

# =============================================
# РЕДАКТИРОВАНИЕ агентов
# =============================================
async def receive_edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_name = update.message.text.strip()
    old_name = context.user_data.get("editing_agent")
    
    agents = load_agents()
    if old_name in agents:
        agents[new_name] = agents.pop(old_name)
        save_agents(agents)
        await update.message.reply_text(
            f"✅ Агент переименован: *{old_name}* → *{new_name}*",
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
            f"✅ Задача агента *{agent_name}* обновлена!",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

# =============================================
# ЗАПУСК БОТА
# =============================================
def main():
    print("🚀 AgentHub Bot запускается...")
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # ConversationHandler для создания агента
    create_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^add_agent$")],
        states={
            WAITING_AGENT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_agent_name)],
            WAITING_AGENT_TASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_agent_task)],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    # ConversationHandler для чата
    chat_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^chat:.+")],
        states={
            WAITING_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, chat_with_agent),
                CallbackQueryHandler(button_handler, pattern="^(chat:|chat_select|main_menu)")
            ],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel)],
        per_user=True
    )

    # ConversationHandler для редактирования
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

    # Регистрируем handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("agents", agents_command))
    app.add_handler(create_conv)
    app.add_handler(chat_conv)
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(button_handler))

    print("✅ Бот запущен! Нажми Ctrl+C для остановки.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
