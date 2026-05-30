import asyncio
import aiohttp
import ssl
import logging
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PriceTest')

BINANCE_URL = "https://api.binance.com"

async def test_price():
    # روش ۱: با SSL معمولی
    ssl_ctx = ssl.create_default_context()
    
    # روش ۲: با connector سفارشی (DNS bypass)
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,
        ttl_dns_cache=300,
        force_close=True
    )
    
    # تست با هر دو روش
    for name, conn in [("SSL Default", None), ("Custom Connector", connector)]:
        try:
            if conn:
                session = aiohttp.ClientSession(connector=conn)
            else:
                session = aiohttp.ClientSession()
            
            url = f"{BINANCE_URL}/api/v3/ticker/price?symbol=SOLUSDT"
            
            async with session.get(url, ssl=ssl_ctx) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = float(data.get("price", 0))
                    logger.info(f"✅ [{name}] SOLUSDT Price: ${price:.2f}")
                else:
                    logger.error(f"❌ [{name}] HTTP {resp.status}: {await resp.text()}")
            
            await session.close()
        except Exception as e:
            logger.error(f"❌ [{name}] Error: {e}")

async def main():
    logger.info("🔍 Testing Binance connection...")
    await test_price()
    logger.info("🏁 Test complete")

if __name__ == "__main__":
    asyncio.run(main())
