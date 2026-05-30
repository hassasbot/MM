import asyncio
import aiohttp
import ssl
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('OKXTest')

OKX_URL = "https://www.okx.com"

async def test_okx():
    ssl_ctx = ssl.create_default_context()
    
    try:
        session = aiohttp.ClientSession()
        
        # تست قیمت SOLUSDT از OKX
        url = f"{OKX_URL}/api/v5/market/ticker?instId=SOL-USDT-SWAP"
        
        async with session.get(url, ssl=ssl_ctx) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    ticker = data["data"][0]
                    price = float(ticker.get("last", 0))
                    logger.info(f"✅ SOLUSDT Price from OKX: ${price:.4f}")
                    logger.info(f"   High 24h: {ticker.get('high24h', 'N/A')}")
                    logger.info(f"   Low 24h: {ticker.get('low24h', 'N/A')}")
                    logger.info(f"   Volume 24h: {ticker.get('vol24h', 'N/A')}")
                else:
                    logger.error(f"❌ API Error: {data}")
            else:
                logger.error(f"❌ HTTP {resp.status}: {await resp.text()}")
        
        await session.close()
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")

async def main():
    logger.info("🔍 Testing OKX connection...")
    await test_okx()
    logger.info("🏁 Test complete")

if __name__ == "__main__":
    asyncio.run(main())
