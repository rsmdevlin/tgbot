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

# ============================================================
# УМНОЕ ПРИМЕНЕНИЕ ПАТЧЕЙ — без перезаписи всего файла
# ============================================================
def apply_smart_patch(original_code: str, patch_text: str):
    """Применяет патч точечно, не перезаписывая весь файл"""
    import re
    result = original_code
    changes = []

    # ФОРМАТ 1: ADD_FUNCTION + REGISTER
    add_match = re.search(r"===ADD_FUNCTION===\n(.*?)===END===", patch_text, re.DOTALL)
    reg_match  = re.search(r"===REGISTER===\n(.*?)===END===",    patch_text, re.DOTALL)
    if add_match:
        new_func = add_match.group(1).strip()
        if "\ndef main():" in result:
            result = result.replace("\ndef main():", f"\n{new_func}\n\ndef main():")
            changes.append("добавлена функция")
    if reg_match:
        reg_line = reg_match.group(1).strip()
        if "    app.run_polling(" in result:
            result = result.replace("    app.run_polling(", f"    {reg_line}\n    app.run_polling(")
            changes.append("зарегистрирован хендлер")

    # ФОРМАТ 2: REPLACE...WITH
    for m in re.finditer(r"===REPLACE===\n(.*?)===WITH===\n(.*?)===END===", patch_text, re.DOTALL):
        old_code = m.group(1).strip()
        new_code = m.group(2).strip()
        if old_code in result:
            result = result.replace(old_code, new_code, 1)
            changes.append("заменён блок")
        else:
            changes.append("⚠️ блок для замены не найден")

    # ФОРМАТ 3: INSERT_AFTER
    for m in re.finditer(r"===INSERT_AFTER===\n(.*?)===CODE===\n(.*?)===END===", patch_text, re.DOTALL):
        pattern = m.group(1).strip()
        code    = m.group(2).strip()
        if pattern in result:
            result = result.replace(pattern, pattern + "\n" + code, 1)
            changes.append("вставлен код")

    # ФОРМАТ 4: полный файл в