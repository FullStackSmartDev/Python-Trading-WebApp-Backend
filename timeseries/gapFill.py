import asyncio
import datetime

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from db import DB
from .cron import store_candles
from .yahoo_finance import Interval, YahooFinance

from .db import get_candle_times, get_first_record

db = DB()
yf = YahooFinance()

import linecache
import sys


pd.set_option(
    "display.max_rows",
    20,
    "display.min_rows",
    20,
    "display.max_columns",
    None,
    "display.width",
    1000,
)
pd.options.mode.chained_assignment = None


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print(
        'Exception in ({}, LINE {} "{}"): {}'.format(
            filename, lineno, line.strip(), exc_obj
        )
    )


async def fillGaps(
    beginningYear=2015, dryRun=False, adjust_to_ipo=True, allow_empty_stocks=True
):

    mainStart = datetime.datetime(beginningYear, 1, 1)
    mainEnd = datetime.datetime.today()

    allMarketDaysDf = await getMarketDays(mainStart, mainEnd)

    allstocks = await db.get_symbols()
    # allstocks = ["UBER"]

    # should be modular for week / month by just specifying different wanted days, and sending the interval so db check correctly...
    # month is a challenge however -- how to get months from days?
    # This might be a problem too, since I'm not sure our data is canonical months at the moment (i.e. == yahoo or tradingview)
    for stock in allstocks:

        # reset vals in case they were modified by previous stock
        start = mainStart
        end = mainEnd
        marketDaysDf = allMarketDaysDf

        try:
            timerstart = datetime.datetime.now()

            currentCandlesDf = await getCurrentCandles(
                start=start, end=end, stock=stock, interval="1d"
            )

            if adjust_to_ipo:
                firstDate = await get_first_record(symbol=stock, interval="1d")

                try:
                    firstDate = int(firstDate["values"][0][0] / (10 ** 9))
                    firstDate = datetime.datetime.utcfromtimestamp(firstDate)
                    start = firstDate
                except:
                    print("Error getting stock first date:")
                    pass

                marketDaysDf = marketDaysDf[marketDaysDf["date"] > start]

            # If no data in the DB, perhaps we're missing the stock entirely... (More likely not available from Yahoo.)
            if isinstance(currentCandlesDf, bool):
                print("No data found for", stock, "in our db")
                if allow_empty_stocks:
                    missingDaysDf = marketDaysDf
                    print("Allowing empty stocks, using all market days")
                else:
                    continue
            else:
                missingDaysDf = await computeMissingDays(marketDaysDf, currentCandlesDf)

            if missingDaysDf is not None:
                if len(missingDaysDf.index) <= 1:
                    continue

                replaceStart = missingDaysDf.iloc[0]["date"]
                replaceEnd = missingDaysDf.iloc[-1]["date"]

            else:
                # No missing days, either because stock is complete, or there was no data at all
                print("missingDaysDf is None")
                continue

            try:
                newDataDf = await asyncio.wait_for(
                    getnewDataDfFromYahoo(
                        start=replaceStart, end=replaceEnd, stock=stock
                    ),
                    timeout=10.0,
                )

            except asyncio.TimeoutError:
                print("Timeout while waiting for yahoo, continuing...")
                continue

            replacementDaysDf = await selectReplacementDays(newDataDf, missingDaysDf)

            # replacementDaysDf = replacementDaysDf.astype(float)

            replacementDaysDf.volume = replacementDaysDf.volume.astype(float)

            # print(replacementDaysDf.dtypes)

            dictForStorage, replacementDaysDf = await formatDfForStorage(
                replacementDaysDf
            )

            # print(replacementDaysDf)

            # print("marketDaysDf")
            # print(marketDaysDf)
            # print()

            # print("currentCandlesDf")
            # print(currentCandlesDf)

            # print()
            # print()

            # print("missingDaysDf")
            # print(missingDaysDf)

            # print()
            # print()

            # print("replacementDaysDf")
            # print(replacementDaysDf)

            # in the case of a stock listed after the beginning of the replacement period, there will always be missing days

            if dryRun:
                print("Dry Run, not saving data to DB")
                continue

            if len(replacementDaysDf.index) >= 1:
                await storeDict(dictForStorage)

                timerend = datetime.datetime.now()
                delta = timerend - timerstart

                try:

                    print(
                        "stock:",
                        stock,
                        "market days:",
                        len(marketDaysDf.index),
                        "\n",
                        "current candles:",
                        # len(currentCandlesDf.index)
                        "\n",
                        "missing:",
                        len(missingDaysDf.index),
                        "\n",
                        "replacing:",
                        len(replacementDaysDf.index),
                        "\n",
                        "Took ",
                        delta.total_seconds(),
                        " seconds",
                    )
                    print()
                except:
                    # sometimes (as when we currently have no data for a stock) we don't have a currentCandlesdf, so can't log index.
                    PrintException()
                    pass

        except:
            PrintException()


async def getMarketDays(start, end):
    # mcal is a module forked from Quantopian's Zipline package
    # it can identify all market days between two dates (weekdays that aren't holidays)
    # NASDAQ calendar == NYSE calendar
    try:
        nasdaq = mcal.get_calendar("NASDAQ")

        market_days = []
        for day in nasdaq.valid_days(start, end):
            market_days.append(day)

        # construct a df from the market days, and get a datetime column for later comparison to candle days
        marketDaysDf = pd.DataFrame(market_days)
        marketDaysDf.columns = ["time"]
        marketDaysDf["date"] = pd.to_datetime(marketDaysDf["time"], utc=True)
        marketDaysDf["date"] = marketDaysDf["date"].apply(
            lambda x: x.replace(microsecond=0, second=0, minute=0, hour=0).replace(
                tzinfo=None
            )
        )
        return marketDaysDf

    except:
        # PrintException()
        pass


async def getCurrentCandles(start, end, stock, interval):

    try:
        candles = await get_candle_times(start, end, interval, stock)

        if candles:
            currentCandlesDf = pd.DataFrame(candles["values"])
            currentCandlesDf.columns = candles["columns"]

            currentCandlesDf["date"] = pd.to_datetime(
                currentCandlesDf["time"], unit="ns", utc=True
            )

            # downsample datetimes to just date, for comparison
            currentCandlesDf["date"] = currentCandlesDf["date"].apply(
                lambda x: x.replace(microsecond=0, second=0, minute=0, hour=0).replace(
                    tzinfo=None
                )
            )

            return currentCandlesDf

        else:
            return False

    except Exception:
        PrintException()


async def computeMissingDays(marketDaysDf, currentCandlesDf):

    try:
        # missingDays are those from all market days which are not in the candles data we pulled from the server
        missingDaysDf = marketDaysDf[~marketDaysDf.date.isin(currentCandlesDf.date)]
        return missingDaysDf

    except:
        PrintException()


async def getnewDataDfFromYahoo(start, end, stock):

    new_data = []

    try:

        new_data = await YahooFinance().get_historical_data(
            stock, start, end, Interval.ONE_DAY
        )

    except Exception:
        PrintException()
        print("getting new data failed")
        pass

    try:

        candles_dict = {
            "timestamp": new_data["timestamps"],
            "open": new_data["open"],
            "high": new_data["high"],
            "low": new_data["low"],
            "close": new_data["close"],
            "volume": new_data["volume"],
            "interval": Interval.ONE_DAY.value,
        }

        # print("candles dict", candles_dict)
        newDataDf = pd.DataFrame(candles_dict)

        newDataDf["date"] = pd.to_datetime(newDataDf["timestamp"], unit="s")
        newDataDf["date"] = newDataDf["date"].apply(
            lambda x: x.replace(microsecond=0, second=0, minute=0, hour=0).replace(
                tzinfo=None
            )
        )

        # convert timestamp to nanoseconds from the epoch

        newDataDf["symbol"] = stock
        return newDataDf

    except:
        PrintException()


async def selectReplacementDays(newDataDf, missingDaysDf):

    try:

        replacementDaysDf = newDataDf[newDataDf.date.isin(missingDaysDf.date)]

        return replacementDaysDf

    except:
        PrintException()


async def formatDfForStorage(replacementDaysDf):

    try:
        replacementDaysDf.reset_index()

        replacementDaysDf["timestamp"] = replacementDaysDf["date"].values.astype(
            np.int64
        )

        replacementDaysDf = replacementDaysDf.drop("date", 1)

        return (replacementDaysDf.to_dict(orient="records"), replacementDaysDf)

    except:
        PrintException()


async def storeDict(dictForStorage):

    try:
        return await store_candles(dictForStorage)

    except:
        PrintException()
