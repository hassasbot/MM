#!/usr/bin/env python3
# ============================================
# test_signal_bot.py
# Simple test - sends signal every 2 minutes
# ============================================

import asyncio, random, logging
from datetime import datetime, timezone
from aiohttp import web

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TestSignal')

TELEGRAM_TOKEN = "8294766234:AAFA7tsO7IPCLligqOiY_XNo9-rPy-m1chs"
CHANNEL_ID = "-1003881031372"

from telegram import Bot
from telegram.constants import ParseMode

async def health_check(request):
    return web.Response(text="OK")

async def run_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 10000)
    await site.start()

async def send_signals():
    bot = Bot(token=TELEGRAM_TOKEN)
    
    # First startup message
    await bot.send_message(CHANNEL_ID, "🧪 *ربات تستی شروع به کار کرد*\nهر ۲ دقیقه یه سیگنال الکی میدم", parse_mode=ParseMode.MARKDOWN)
    
    while True:
        await asyncio.sleep(120)  # Wait 2 minutes
        
        direction = random.choice(["LONG 🟢", "SHORT 🔴"])
        entry = round(random.uniform(100, 200), 2)
        sl = round(entry * 0.99, 2) if "LONG" in direction else round(entry * 1.01, 2)
        tp1 = round(entry * 1.01, 2) if "LONG" in direction else round(entry * 0.99, 2)
        tp2 = round(entry * 1.02, 2) if "LONG" in direction else round(entry * 0.98, 2)
        tp3 = round(entry * 1.03, 2) if "LONG" in direction else round(entry * 0.97, 2)
        confidence = random.randint(70, 95)
        stars = "⭐" * (confidence // 20 + 1)
        
        msg = (
            f"{'🟢' if 'LONG' in direction else '🔴'} *سیگنال تستی*\n"
            f"{'─'*30}\n"
            f"⭐ *اطمینان:* {stars} ({confidence}%)\n"
            f"🎯 *ورود:* {entry}\n"
            f"🛑 *SL:* {sl}\n"
            f"✅ *TP1:* {tp1}\n"
            f"✅ *TP2:* {tp2}\n"
            f"✅ *TP3:* {tp3}\n"
            f"📝 *نوع:* سیگنال تستی"
        )
        
        await bot.send_message(CHANNEL_ID, msg, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Test signal sent: {direction}")

async def main():
    asyncio.create_task(run_web_server())
    await send_signals()

if __name__ == "__main__":
    asyncio.run(main())
