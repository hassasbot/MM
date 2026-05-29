import asyncio
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TestBot')

TELEGRAM_BOT_TOKEN = "8294766234:AAFA7tsO7IPCLligqOiY_XNo9-rPy-m1chs"
SIGNAL_CHANNEL = "-1003881031372"

from telegram import Bot

async def main():
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        me = await bot.get_me()
        logger.info(f"Connected to bot: @{me.username}")
        
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = f"Test message from Render at {now}"
        
        await bot.send_message(chat_id=SIGNAL_CHANNEL, text=msg)
        logger.info("Message sent successfully!")
        
    except Exception as e:
        logger.error(f"Error: {e}")

asyncio.run(main())
