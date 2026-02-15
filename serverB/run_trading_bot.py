import asyncio
import logging

from trading_bot import TradingBot


async def _run_forever():
    bot = TradingBot()
    await bot.start()
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        await bot.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(_run_forever())
    except KeyboardInterrupt:
        pass
