import asyncio
import datetime

from .PricingData_pb2 import PricingData
from .yahoo_finance import YahooFinance


class Stock:
    def __init__(self, symbol):
        self._symbol = symbol
        self._open = None
        self._close = None
        self._high = None
        self._low = None
        self._volume = None
        self._last_updated_at = None

    async def bootstrap(self):

        # print("bootstrapping for", self._symbol)

        try:
            # use v10 because it only shows regular market OHLCV, need to be able
            # to bootstrap after hours and get regular market OHLCV

            ohlc = await YahooFinance().get_today_quote_v10(self._symbol)
            self._open = ohlc["open"]
            self._high = ohlc["high"]
            self._low = ohlc["low"]
            self._close = ohlc["close"]
            self._volume = ohlc["volume"]

        # Black formatter complains that ex is "assigned but never used" here
        # except Exception as ex:
        except Exception:
            # print("exception in bootstrap for symbol", self._symbol, ex)
            pass

    def on_transction(self, price, volume, timestamp):

        now = datetime.datetime.now()
        today4pm = now.replace(hour=16, minute=0, second=0, microsecond=0)
        today930am = now.replace(hour=9, minute=30, second=0, microsecond=0)

        # Don't update price after hours
        if today930am < now < today4pm:

            if not self._high:
                self._high = price
            if not self._low:
                self._low = price
            if not self._open:
                self._open = price
            if not self._close:
                self._close = price
            if not self._volume:
                self._volume = volume

            self._last_updated_at = timestamp
            self._close = price
            if price < self._low:
                self._low = price
            if price > self._high:
                self._high = price
            self._volume = volume

    @property
    def json(self):
        return {
            "symbol": self._symbol,
            "open": self._open,
            "close": self._close,
            "high": self._high,
            "low": self._low,
            "volume": self._volume,
            "last_updated_at": self._last_updated_at,
        }

    def clear(self):
        self._open = None
        self._close = None
        self._high = None
        self._low = None
        self._volume = None
        self._last_updated_at = None


class TickerManager:
    def __init__(self, symbols: list = None):
        self._symbols = symbols

        self._queue = asyncio.Queue()
        self._yf = YahooFinance()

    def set_symbols(self, symbols):
        # print(symbols)
        self._symbols = symbols
        self._tickers = {symbol: Stock(symbol) for symbol in self._symbols}

    async def bootstrap(self):
        tasks = []
        # 200 appears to be stable but didn't test upper limit. Took 25 seconds on local computer,
        # so presumably faster in the cloud. Can adjust later if needed.

        chunk = 200
        for i in range(0, len(self._symbols), chunk):
            chunked = self._symbols[i : i + chunk]
            for symbol in chunked:
                tasks.append(
                    asyncio.wait_for(self._tickers[symbol].bootstrap(), timeout=10.0)
                )

            await asyncio.gather(*tasks, return_exceptions=True)

            # After chunk completes, begin next chunk
            tasks = []

    async def on_quote(self, pd: PricingData):
        symbol = pd.id
        price = pd.price
        timestamp = pd.time / 1000
        volume = pd.dayVolume
        self._tickers[symbol].on_transction(price, volume, timestamp)

    async def start(self):
        await self._yf.quotes_for(self._symbols, self.on_quote)

    def get_all_ohlcv(self):
        return [stock.json for symbol, stock in self._tickers.items()]

    def get_ohlcv(self, symbol):

        # if not self._open and not self._high and not self._low and not self._close:
        #     print(
        #         "get_ohlcv, no vals for OHLC, attempting single bootstrap", self._symbol
        #     )
        # Potentially add a single symbol bootstrap here as a backup, dependent on either
        # all vals being missing, or last_updated being too old.

        today_ts = int(
            datetime.datetime.fromisoformat(
                datetime.date.today().isoformat()
            ).timestamp()
            * (10 ** 9)
        )

        ohlcv = self._tickers[symbol].json
        ohlcv["timestamp"] = today_ts

        return ohlcv

    def clear_all(self):
        for symbol in self._symbols:
            self._tickers[symbol].clear()
