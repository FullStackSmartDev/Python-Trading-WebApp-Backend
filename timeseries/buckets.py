import datetime
import linecache
import sys

import numpy as np
import pandas as pd
from strategies import STRATEGIES

from db import DB

from .db import (
    delete_past_bucket_data,
    get_latest_bucket_record,
    get_stocklist_candles,
    store_bucket_candles,
)
from .yahoo_finance import Interval

db = DB()


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print(
        'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(
            filename, lineno, line.strip(), exc_obj
        )
    )


async def update_buckets(app, startover=False):

    print("deleting past bucket data at", datetime.datetime.now())

    # clear out potentially erroneous data from previous buckets
    # put in because there was some weekend data that showed up at one point
    # fast enough that this seems like a reaonsable approach since it's all being overwritten anyway

    try:
        await delete_past_bucket_data()
    except Exception:
        PrintException()

    strategies = [strategy.slug for strategy in STRATEGIES.values()]
    #  remove the all-stocks strategy
    strategies.pop(0)

    print("List of strategies to be updated is:", strategies)

    for bucketname in strategies:

        bucketmeta = await db.get_screened_stocks(bucketname)
        stocklist = []

        # manually remove some erratic stocks from aggregate
        excluded_stocks = [
            "ACI",
            "API",
            "SVA",
            "ACIU",
            "GTH",
            "CNXC",
            "OZON",
            "OAS",
            "AI",
            "CRC",
            "GTH",
            "VITL",
            "CIIC",
            "DUO",
            "FBRX",
            "HIGA",
            "ORPH",
            "CRSA",
            "LRMR",
        ]

        # Filter stocklists for bucket by market cap between 2b and 50b
        included_stocks_print_notice = []
        for stock in bucketmeta["stocks"]:
            try:
                if stock["symbol"] not in excluded_stocks:
                    if 2000 < float(stock["data"]["market_cap"]) < 50000:
                        if float(stock["data"]["years_since_ipo"]) > 0.5:
                            stocklist.append(stock["symbol"])
                            included_stocks_print_notice.append(stock["symbol"])

            except Exception:
                PrintException()

        print("Included stocks for", bucketname, included_stocks_print_notice)
        print()

        # intervals = [Interval.ONE_DAY, Interval.ONE_WEEK, Interval.ONE_MONTH]
        intervals = [Interval.ONE_DAY]

        for interval in intervals:

            interval = interval.value
            bucketdata = []

            try:
                bucketdata = await get_data_to_aggregate(
                    bucketname, stocklist, interval, startover
                )
            except Exception as e:
                print("Bucket failed in getting data to aggregate,", e)

            if bucketdata != []:

                df = await aggregate_bucket(bucketdata, bucketname)

                print(
                    "Bucket: storing data for",
                    bucketname,
                    " ",
                    interval,
                    datetime.datetime.now(),
                )

                await store_bucket_candles(df)

            else:
                print("Bucket: ", bucketname, interval, "up to date")
                pass


async def get_data_to_aggregate(bucketname, stocklist, interval, startover):

    start = []

    if startover != True:
        start = await get_latest_bucket_record(bucketname, interval)

    if start == [] or startover == True:
        start = datetime.datetime.fromisoformat("2015-01-01")
    else:
        start = start["values"][0][0]

    end = datetime.datetime.now()

    try:
        stocklist_candles = await get_stocklist_candles(start, end, interval, stocklist)

    except Exception:
        PrintException()

    return stocklist_candles


async def aggregate_bucket(bucketdata, bucketname):

    # basic aggregation currently using mean.
    # Opted for implementation in pandas vs a continuous query in influxdb because may use non timeseries data sources down the line
    # Perhaps possible to do client-side, but query alone is slow enough, plus we want

    # tooling to do sorting such as squeezing stocks etc.

    df = pd.DataFrame(bucketdata["values"])
    df.columns = bucketdata["columns"]

    try:

        pd.set_option(
            "display.max_rows",
            None,
            "display.min_rows",
            100,
            "display.max_columns",
            None,
            "display.width",
            1000,
        )
        pd.options.mode.chained_assignment = None

        df["time"] = pd.to_datetime(df["time"], unit="ns")

        aggregation_functions = {
            "open": "mean",
            "high": "mean",
            "low": "mean",
            "close": "mean",
            "volume": "mean",
            "interval": "first",
        }

        df = df.astype(
            {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "interval": str,
            }
        )

        # Downsample timestamps to day resolution, group by datetime
        df["time"] = df["time"].apply(
            lambda x: x.replace(microsecond=0, second=0, minute=0, hour=0)
        )

        # alltimes is all unique times, so presumably a list of all correct days
        # assuming at least some stocks have data for each day.
        # if some stocks have data for incorrect days,
        # the others will be interpolated anyway

        alltimes = pd.DataFrame(df.time.unique())

        stocks = df.groupby("symbol")["symbol"].agg(["unique"])

    except Exception as e:
        print("Bucket failed in queueing data repair for,", bucketname, e)

    stock_dataframes = {}
    for i in stocks.iterrows():
        symbol = str(i[0])

        stockdf = df[df["symbol"] == symbol]

        if len(stockdf.index) > 1:

            stock_dataframes[symbol] = {"dataframe": None, "symbol": symbol}

            # ---------------- Repair Data problems --------------
            try:

                #   Compute Rolling z-score in order to exclude erroneous data
                #   (>3, so 99.9% divergent from rolling 10 day window)

                close_mean = stockdf["close"].rolling(window=10).mean()
                close_std = stockdf["close"].rolling(window=10).std()
                zscores = (stockdf["close"] - close_mean) / close_std

                stockdf["close_zscore"] = zscores

                stockdf_no_outliers = stockdf.loc[abs(stockdf["close_zscore"]) < 3]

                #   Add null rows for the days that are missing from the data, or were removed by zscore

                first_date = stockdf_no_outliers["time"].iloc[0]
                alltimes.columns = ["time"]
                sincestart = pd.DataFrame(alltimes[alltimes["time"] >= first_date])

                sincestart["close"] = np.nan
                sincestart["high"] = np.nan
                sincestart["interval"] = stockdf_no_outliers["interval"].iloc[0]
                sincestart["low"] = np.nan
                sincestart["symbol"] = symbol
                sincestart["volume"] = np.nan
                sincestart["open"] = np.nan
                sincestart["close_zscore"] = np.nan

                missingdf = sincestart[~sincestart.time.isin(stockdf_no_outliers.time)]

                withmissing = pd.concat([stockdf_no_outliers, missingdf])
                withmissing = withmissing.sort_values("time").reset_index(drop=True)

                #   Linear interpolation for missing data.
                interpolated = withmissing.interpolate(method="linear")
                interpolated = interpolated.drop_duplicates(subset="time", keep="first")

                stock_dataframes[symbol]["dataframe"] = interpolated

            except Exception as e:
                print(
                    "Stock failed in data repair for stock,",
                    symbol,
                    e,
                    PrintException(),
                )

    try:
        dfs = []
        for stock in stock_dataframes.keys():
            dfs.append(stock_dataframes[stock]["dataframe"])

        concated = pd.concat(dfs)

        finaldf = concated.groupby(concated["time"]).aggregate(aggregation_functions)

        finaldf["bucket"] = bucketname

        return finaldf

    except Exception as e:
        print("Bucket failed in data aggregation ,", e, PrintException())
