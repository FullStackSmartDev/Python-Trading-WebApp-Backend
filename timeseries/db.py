import datetime
import os
import typing

from pytz import timezone

from aioinflux import InfluxDBClient

import linecache
import sys

DB_NAME = "fpc_timeseries"
DB_HOST = os.environ.get("INFLUX_HOST") or "localhost"


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


class OHLCVPoint(typing.TypedDict):
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime.datetime
    interval: str
    symbol: str


class OHLCVPoint_gapfill(typing.TypedDict):
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime.datetime
    interval: str
    symbol: str


class ShortInterestPoint(typing.TypedDict):
    symbol: str
    name: str
    short_interest: float
    days_to_cover_short: float
    float_short: float
    insider_ownership: float
    institutional_investors_ownership_percent: float
    average_daily_volume_30d: float
    price: float
    market_cap: float
    sector: str
    industry: str


async def store_candles(points: typing.Iterable[OHLCVPoint]):
    async with InfluxDBClient(db=DB_NAME, host=DB_HOST) as client:
        await client.create_database(db=DB_NAME)

        points = [
            {
                "time": point["timestamp"],
                "measurement": "ohlcv",
                "tags": {
                    "symbol": point["symbol"],
                    "interval": point["interval"],
                },
                "fields": {
                    "open": point["open"],
                    "high": point["high"],
                    "low": point["low"],
                    "close": point["close"],
                    "volume": point["volume"],
                },
            }
            for point in points
        ]

        await client.write(points)


async def store_candles_gapFill(points: typing.Iterable[OHLCVPoint_gapfill]):
    async with InfluxDBClient(db=DB_NAME, host=DB_HOST) as client:
        await client.create_database(db=DB_NAME)

        points = [
            {
                "time": point["timestamp"],
                "measurement": "ohlcv",
                "tags": {
                    "symbol": point["symbol"],
                    "interval": point["interval"],
                },
                "fields": {
                    "open": point["open"],
                    "high": point["high"],
                    "low": point["low"],
                    "close": point["close"],
                    "volume": point["volume"],
                },
            }
            for point in points
        ]

        await client.write(points)


async def store_bucket_candles(dataframe):
    async with InfluxDBClient(db="fpc_buckets", host=DB_HOST) as client:
        await client.create_database(db="fpc_buckets")

        await client.write(
            dataframe, measurement="agg_ohlcv", tag_columns=["bucket", "interval"]
        )


async def store_short_interest(points: typing.Iterable[ShortInterestPoint]):
    async with InfluxDBClient(db=DB_NAME, host=DB_HOST) as client:
        await client.create_database(db=DB_NAME)
        points = [
            {
                "time": point["timestamp"],
                "measurement": "short_interest",
                "tags": {
                    "symbol": point["symbol"],
                },
                "fields": {
                    "name": point["fundamentals"]["name"],
                    "short_interest": point["fundamentals"]["short_interest"] or None,
                    "days_to_cover_short": point["fundamentals"]["days_to_cover_short"]
                    or None,
                    "float_short": point["fundamentals"]["float_short"] or None,
                    "insider_ownership": point["fundamentals"]["insider_ownership"]
                    or None,
                    "institutional_investors_ownership_percent": point["fundamentals"][
                        "institutional_investors_ownership_percent"
                    ]
                    or None,
                    "average_daily_volume_30d": point["fundamentals"][
                        "average_daily_volume_30d"
                    ]
                    or None,
                    "price": point["fundamentals"]["price"] or None,
                    "market_cap": point["fundamentals"]["market_cap"] or None,
                    "sector": point["fundamentals"]["sector"],
                    "industry": point["fundamentals"]["industry"],
                },
            }
            for point in points
        ]

        await client.write(points)


# Handle mutliple dbs, default to DB_NAME
async def select_query(query, db=DB_NAME, host=DB_HOST):

    # print(query)
    async with InfluxDBClient(db=db, host=host) as client:
        res = await client.query(query)
        if "series" in res["results"][0]:
            return res["results"][0]["series"][0]
        else:
            return []


async def get_candles(
    start: datetime.datetime,
    end: datetime.datetime,
    interval: str,
    symbol: str,
    db: str = None,
):
    start = int(start.timestamp() * (10 ** 9))
    end = int(end.timestamp() * (10 ** 9))
    return await select_query(
        "SELECT open, high, low, close, volume, interval, symbol FROM ohlcv WHERE"
        f" interval='{interval}' AND symbol='{symbol}' AND time <= {end} AND time >="
        f" {start}",
        db=db or DB_NAME,
    )


async def get_period_OHLV(interval: str, symbol: str):

    if interval == "1d":
        return {}

    try:

        now = datetime.datetime.now(timezone("EST"))
        end = int(now.timestamp()) * (10 ** 9)

        if interval == "1wk":

            today0 = now.replace(hour=0, minute=0, second=0)
            last_monday = today0 - datetime.timedelta(days=now.weekday())
            start = int(last_monday.timestamp()) * (10 ** 9)

        if interval == "1mo":

            first_day_of_month = now.replace(day=1, hour=0, minute=0, second=0)
            start = int(first_day_of_month.timestamp()) * (10 ** 9)

        day_vals = await select_query(
            "SELECT open, high, low, close, volume FROM ohlcv WHERE"
            f" interval='1d' AND symbol='{symbol}' AND time <= {end} AND time >="
            f" {start}",
            DB_NAME,
        )

        result = {"timestamp": 0, "open": 0, "high": 0, "low": 0, "volume": 0}

        # first timestamp in period
        # make sure it's 4pm since lightweight charts seems to care
        # wk and mo use different conversion schemes apparently
        # appears to be a bug in our timestamping?
        # why are we altering yahoo timestamps anyway?

        if interval == "1wk":
            print(day_vals)
            first_time_in_period = datetime.datetime.fromtimestamp(
                day_vals["values"][0][0] / (10 ** 9)
            )

        if interval == "1mo":
            first_time_in_period = datetime.datetime.utcfromtimestamp(
                day_vals["values"][0][0] / (10 ** 9)
            )

        normalized = first_time_in_period.replace(hour=16, minute=0, second=0)
        normalized = int(normalized.timestamp()) * (10 ** 9)
        result["timestamp"] = normalized

        result["open"] = day_vals["values"][0][1]

        result["high"] = day_vals["values"][0][2]
        result["low"] = day_vals["values"][0][3]

        # tohlcv = time, open, high, low, close, volume

        for tohlcv in day_vals["values"]:
            # time = tohlcv[0]
            # open = tohlcv[1]
            high = tohlcv[2]
            low = tohlcv[3]
            # close = tohlcv[4]
            volume = tohlcv[5]

            if high > result["high"]:
                result["high"] = high
            if low < result["low"]:
                result["low"] = low

            result["volume"] = result["volume"] + volume

        return result

    except Exception:
        print("failed making new patched candle")
        PrintException()
        return {}


async def get_short_interest(symbol: str):
    return await select_query(f"SELECT * FROM short_interest WHERE symbol='{symbol}'")


async def get_candle_times(
    start: datetime.datetime,
    end: datetime.datetime,
    interval: str,
    symbol: str,
    db: str = None,
):
    start = int(start.timestamp() * (10 ** 9))
    end = int(end.timestamp() * (10 ** 9))
    return await select_query(
        "SELECT close FROM ohlcv WHERE"
        f" interval='{interval}' AND symbol='{symbol}' AND time <= {end} AND time >="
        f" {start}",
        db=db or DB_NAME,
    )


async def get_stocklist_candles(
    start: datetime.datetime, end: datetime.datetime, interval: str, symbols: list
):
    # List of symbols as select statement string
    syms = ""
    syms = syms.join([f"symbol='{symbol}' OR " for symbol in symbols])
    syms = "(" + syms[:-4] + ")"

    start = int(start.timestamp() * (10 ** 9))
    end = int(end.timestamp() * (10 ** 9))

    return await select_query(
        "SELECT open, high, low, close, volume, interval, symbol FROM ohlcv WHERE"
        f" {syms} AND time <= {end} AND time > {start} AND interval='{interval}'"
    )


async def get_bucket_candles(
    start: datetime.datetime, end: datetime.datetime, interval: str, bucket: str
):
    start = int(start.timestamp() * (10 ** 9))
    end = int(end.timestamp() * (10 ** 9))

    # clip incomplete data
    # days = (5 * 86400 * 10**9)
    # end = (int(end.timestamp() * (10 ** 9)) - days)

    return await select_query(
        "SELECT open, high, low, close, volume FROM agg_ohlcv WHERE"
        f" interval='{interval}' AND bucket='{bucket}' AND time <= {end} AND time >="
        f" {start}",
        "fpc_buckets",
    )


async def get_all_bucket_candles(start: datetime.datetime, end: datetime.datetime):
    start = int(start.timestamp() * (10 ** 9))
    end = int(end.timestamp() * (10 ** 9))

    return await select_query(
        "SELECT open, high, low, close, volume, bucket FROM agg_ohlcv WHERE"
        f" interval='1d' AND time <= {end} AND time >= {start}",
        "fpc_buckets",
    )


async def get_latest_bucket_record(bucket: str, interval: str):

    return await select_query(
        f"SELECT LAST(open) FROM agg_ohlcv WHERE bucket = '{bucket}' AND interval ="
        f" '{interval}'",
        "fpc_buckets",
    )


async def delete_past_bucket_data():

    # Delete from 2000 onward
    return await select_query(
        "DELETE FROM agg_ohlcv where time > 946684800000000000", "fpc_buckets"
    )


async def get_latest_record(symbol: str, interval: str):
    return await select_query(
        "SELECT LAST(open), open, high, low, close, volume FROM ohlcv WHERE symbol ="
        f" '{symbol}' AND interval = '{interval}'"
    )


async def get_first_record(symbol: str, interval: str):

    return await select_query(
        f"SELECT FIRST(open) FROM ohlcv WHERE symbol = '{symbol}' AND interval ="
        f" '{interval}'"
    )


async def get_latest_timestamp(symbol: str, interval: str):
    res = await get_latest_record(symbol, interval)
    if "values" in res:
        return res["values"][0][0] / (10 ** 9)
    else:
        return 0


async def get_day_coverage(
    start: datetime.datetime, end: datetime.datetime, interval: str
):

    start = int(start.timestamp() * (10 ** 9))
    end = int(end.timestamp() * (10 ** 9))

    return await select_query(
        f"SELECT time, symbol, close FROM ohlcv WHERE interval = '{interval}' AND time"
        f" <= {end} AND time >= {start}"
    )


def influx_res_to_dict(influx_result):
    res = []
    for row in influx_result["values"]:
        row_dict = {}
        for i, column in enumerate(influx_result["columns"]):
            if column == "time":
                row_dict["timestamp"] = row[i]
            else:
                row_dict[column] = row[i]
        res.append(row_dict)
    return res
