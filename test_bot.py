#!/usr/bin/env python3
# ============================================
# test_bot.py
# Simple test to check Telegram connection
# ============================================

import asyncio
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TestBot')

# تنظیمات
TELEGRAM_BOT_TOKEN = "8294766234:AAFA7tsO7IPCLligqOiY_XNo9-rPy-m1chs",
SIGNAL_CHANNEL = "-1003881031372",

try:
    from telegram import Bot
    from telegram.constants import ParseMode
    TELE = True
except ImportError:
    TELE = False
    logger.error("python-telegram-bot not installed!")

async def main():
    if not TELE:
        logger.error("Cannot run without python-telegram-bot")
        return
    
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        # تست اتصال
        me = await bot.get_me()
        logger.info(f"✅ Connected to bot: @{me.username}")
        
        # ارسال پیام تست
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = f"🧪 *پیام تستی از Render*\n⏰ زمان: {now}\n✅ اگر این پیام رو می‌بینی، یعنی همه چی کار می‌کنه!"
        
        await bot.send_message(
            chat_id=SIGNAL_CHANNEL,
            text=msg,
            parse_mode=ParseMode.MARKDOWN
        )
        
        logger.info("✅ Test message sent to channel!")
        logger.info("🎉 Everything works! You can now deploy the main bot.")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
