import asyncio
import base64
import datetime
import enum
import json

import aiohttp

from debug.printException import PrintExceptionInfo

from .PricingData_pb2 import PricingData


class Interval(enum.Enum):
    ONE_MINUTE = "1m"
    TWO_MINUTE = "2m"
    FIVE_MINUTE = "5m"
    FIFTEEN_MINUTE = "15m"
    THIRTY_MINUTE = "30m"
    SIXTY_MINUTE = "60m"
    NINTY_MINUTE = "90m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"
    FIVE_DAY = "5d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"
    THREE_MONTH = "3mo"


class YahooFinance:
    def __init__(self):
        self._base_uri = "https://query1.finance.yahoo.com/v8/finance/chart"
        self._base_uri_v10 = (
            "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
        )
        MAX = 36500

        self._intervals = {
            Interval.ONE_MINUTE: {
                "chunk": datetime.timedelta(days=7),
                "last": 30,
            },
            Interval.TWO_MINUTE: {
                "chunk": datetime.timedelta(days=60),
                "last": 60,
            },
            Interval.FIVE_MINUTE: {
                "chunk": datetime.timedelta(days=60),
                "last": 60,
            },
            Interval.FIFTEEN_MINUTE: {
                "chunk": datetime.timedelta(days=60),
                "last": 60,
            },
            Interval.THIRTY_MINUTE: {
                "chunk": datetime.timedelta(days=60),
                "last": 60,
            },
            Interval.SIXTY_MINUTE: {"chunk": datetime.timedelta(days=730), "last": 730},
            Interval.NINTY_MINUTE: {"chunk": datetime.timedelta(days=60), "last": 60},
            Interval.ONE_HOUR: {
                "chunk": datetime.timedelta(days=730),
                "last": 730,
            },
            Interval.ONE_DAY: {"chunk": datetime.timedelta(days=MAX), "last": MAX},
            Interval.FIVE_DAY: {"chunk": datetime.timedelta(days=MAX), "last": MAX},
            Interval.ONE_WEEK: {"chunk": datetime.timedelta(days=MAX), "last": MAX},
            Interval.ONE_MONTH: {"chunk": datetime.timedelta(days=MAX), "last": MAX},
            Interval.THREE_MONTH: {"chunk": datetime.timedelta(days=MAX), "last": MAX},
        }

    def _get_epoch_time(self, date: datetime.datetime):
        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = date - epoch
        return int(delta.total_seconds())

    async def _data_request(
        self,
        symbol: str,
        start: datetime.datetime,
        end: datetime.datetime,
        interval: Interval,
    ):
        params = {
            "symbol": symbol,
            "period1": self._get_epoch_time(start),
            "period2": self._get_epoch_time(end),
            "interval": interval.value,
        }

        async with aiohttp.ClientSession() as session:
            # print(f"{self._base_uri}/{symbol} params={params}")
            async with session.get(f"{self._base_uri}/{symbol}", params=params) as resp:
                # print(resp.url)
                return await resp.json()

    async def get_all_data(self, symbol: str, interval: Interval):
        to = datetime.datetime.utcnow()
        frm = to - datetime.timedelta(days=self._intervals[interval]["last"] - 1)
        return await self.get_historical_data(symbol, frm, to, interval)

    async def get_historical_data(
        self,
        symbol: str,
        frm: datetime.datetime,
        to: datetime.datetime,
        interval: Interval,
    ):

        res = {
            "symbol": symbol,
            "timestamps": [],
            "open": [],
            "close": [],
            "high": [],
            "low": [],
            "volume": [],
            "interval": interval.value,
            "frm": str(frm),
            "to": str(to),
        }

        async for data in self.get_historical_data_chunked(symbol, frm, to, interval):
            try:
                # current_trading_period = data["chart"]["result"][0]["meta"][
                # "currentTradingPeriod"
                # ]
                # happens if scraped on days when market was on holiday, and the
                # last chunk happens to have no data
                # print(
                # self._get_epoch_time(frm), current_trading_period["regular"]["end"]
                # )
                # if self._get_epoch_time(frm) > current_trading_period["regular"]["end"]:
                # continue

                # if no trades

                if data["chart"]["result"] == None:
                    # print(frm, to)
                    if (
                        data["chart"]["error"]["description"]
                        == "No data found, symbol may be delisted"
                    ):
                        # print(f"Delisted: {symbol}")
                        continue

                result = data["chart"]["result"][0]

                if "timestamp" not in result and interval == Interval.ONE_MINUTE:
                    continue
                if (
                    "timestamp" not in result
                    and self._get_epoch_time(frm) > result["meta"]["regularMarketTime"]
                ):
                    continue
                if (
                    "timestamp" not in result
                    and result["meta"]["instrumentType"] == "MUTUALFUND"
                ):
                    continue
                if (
                    "timestamp" not in result
                    and result["meta"]["instrumentType"] == "ETF"
                ):
                    continue

                quote = result["indicators"]["quote"][0]
                res["timestamps"] += result["timestamp"]

            except TypeError as e:
                import pprint

                PrintExceptionInfo()
                pprint.pprint(data)
                print("\n\n\n\n\n")
                raise e

            except Exception as e:
                print(
                    f"Undandled Exception in get_historical_data for stock {symbol}, {e}"
                )
                import pprint

                pprint.pprint(data)
                raise e

            res["open"] += quote["open"]
            res["close"] += quote["close"]
            res["high"] += quote["high"]
            res["low"] += quote["low"]
            res["volume"] += quote["volume"]

        return res

    async def get_historical_data_chunked(
        self,
        symbol: str,
        frm: datetime.datetime,
        to: datetime.datetime,
        interval: Interval,
    ):
        curr_start = frm
        chunk_size = self._intervals[interval]["chunk"]
        curr_end = frm + chunk_size
        if curr_end > to:
            curr_end = to

        while True:
            if curr_start == to:
                break

            yield await self._data_request(
                symbol,
                curr_start,
                curr_end,
                interval,
            )
            curr_start = curr_end
            curr_end = curr_start + chunk_size
            if curr_end > to:
                curr_end = to

    async def get_last_price(self, symbol: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._base_uri}/{symbol}") as resp:
                try:
                    return (await resp.json())["chart"]["result"][0]["meta"][
                        "regularMarketPrice"
                    ]
                except TypeError as e:
                    print(e)

    async def get_today_quote_v8(self, symbol: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._base_uri}/{symbol}") as resp:
                try:
                    quote = (await resp.json())["chart"]["result"][0]["indicators"][
                        "quote"
                    ][0]
                    return {
                        "open": quote["open"][0],
                        "high": quote["high"][0],
                        "low": quote["low"][0],
                        "close": quote["close"][0],
                        "volume": quote["volume"][0],
                    }
                except TypeError:
                    pass
                except KeyError as e:
                    print(
                        "today quote failed, presumably delisted symbol, keyerror, ",
                        symbol,
                        e,
                    )

    async def get_today_quote_v10(self, symbol: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self._base_uri_v10}/{symbol}?modules=price,financialData"
            ) as resp:
                try:
                    quote = (await resp.json())["quoteSummary"]["result"][0]["price"]

                    return {
                        "open": quote["regularMarketOpen"]["raw"],
                        "high": quote["regularMarketDayHigh"]["raw"],
                        "low": quote["regularMarketDayLow"]["raw"],
                        "close": quote["regularMarketPrice"]["raw"],
                        "volume": quote["regularMarketVolume"]["raw"],
                    }
                except TypeError:
                    pass
                except KeyError as e:
                    print(
                        "today quote failed, presumably delisted symbol, keyerror, ",
                        symbol,
                        e,
                    )

    async def get_live_quote(self, symbol):
        pass

    async def quotes_for_tickers(self, tickers, on_quote):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        "wss://streamer.finance.yahoo.com/", heartbeat=300
                    ) as ws:
                        await ws.send_json({"subscribe": tickers})
                        async for msg in ws:
                            pd = PricingData()
                            pd.ParseFromString(base64.b64decode(msg.data))
                            await on_quote(pd)
            except Exception as ex:
                print("an exception occured", ex)

    async def quotes_for(self, tickers, on_quote):
        chunk_size = 60
        for i in range(0, len(tickers), chunk_size):
            chunked = tickers[i : i + chunk_size]
            asyncio.create_task(self.quotes_for_tickers(chunked, on_quote))

    async def get_key_statistic_float_shares(self, symbol):
        async with aiohttp.ClientSession(skip_auto_headers=["user-agent"]) as session:
            async with session.get(
                f"https://finance.yahoo.com/quote/{symbol}/key-statistics?p={symbol}",
            ) as resp:
                html = await resp.text()
                for line in html.splitlines():
                    if "root.App.main = " in line:
                        data = line.replace("root.App.main = ", "")[:-1]
                        data = json.loads(data)
                        float_shares = data["context"]["dispatcher"]["stores"][
                            "QuoteSummaryStore"
                        ]["defaultKeyStatistics"]["floatShares"]
                        return float_shares
