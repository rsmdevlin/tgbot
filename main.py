import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Привет!")

async def basa(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id=update.effective_chat.id, text="База данных")

def main() -> None:
    application = ApplicationBuilder().token("ВСТАВЬТЕ_ВАШ_ТОКЕН_ЗДЕСЬ").build()

    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)

    basa_handler = CommandHandler('basa', basa)
    application.add_handler(basa_handler)

    application.run_polling()

if __name__ == '__main__':
    main()