import asyncio
import datetime

import click

from db import DB
from timeseries.db import influx_res_to_dict, select_query

DB_NAME = "fmp"
SERIES_NAME = "historical_1d"
DB_HOST = "fpc-influx"

MONGO_HOST = "fpc-mongo"


async def get_all_series_fmp():
    res = []
    for series in influx_res_to_dict(
        await select_query(f"SHOW SERIES FROM {SERIES_NAME}", db=DB_NAME, host=DB_HOST)
    ):
        key = series["key"]
        datum = {}
        for keyval in key.split(",")[1:]:
            key, val = keyval.split("=", 1)
            datum[key] = val
        res.append(datum)
    return res


async def timerange(symbol):
    first = await select_query(
        f"SELECT FIRST(close) FROM {SERIES_NAME} WHERE symbol='{symbol}'",
        db=DB_NAME,
        host=DB_HOST,
    )
    last = await select_query(
        f"SELECT LAST(close) FROM {SERIES_NAME} WHERE symbol='{symbol}'",
        db=DB_NAME,
        host=DB_HOST,
    )
    return {
        "start": first[0]["timestamp"],
        "end": last[0]["timestamp"],
    }


def day_to_month_date(day):
    days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    month = 1
    while day - days[month - 1] > 0:
        day -= days[month - 1]
        month += 1

    return f'{f"0{month}" if month < 10 else month}/{f"0{day}" if day < 10 else day}'


async def create_symbol_seasonality_influx(symbol, series, db):
    days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    seasonality = [[0] * x for x in days]

    data = influx_res_to_dict(
        await select_query(
            f"SELECT * FROM {series} where symbol='{symbol}'", db=db, host=DB_HOST
        )
    )

    for datum in data:
        ts = datetime.datetime.fromtimestamp(datum["timestamp"] / 10 ** 9)
        month = ts.month - 1
        day = ts.day - 1

        # fmp data
        if "changePercent" in datum:
            changePercent = datum["changePercent"]
        # yahoo data
        else:
            try:
                changePercent = (datum["close"] - datum["open"]) / datum["open"]
            except ZeroDivisionError:
                changePercent = 0

        seasonality[month][day] = (seasonality[month][day] + changePercent) / 2

    res = []

    for i, month in enumerate(seasonality, 1):
        for j, day in enumerate(month, 1):
            res.append(seasonality[i - 1][j - 1])

    return list(
        map(
            lambda tup: {
                "change": tup[1],
                "day": tup[0],
                "label": day_to_month_date(tup[0]),
            },
            enumerate(res, 1),
        )
    )


async def store_in_mongo(seasonality, symbol, _type):
    mongo = DB(MONGO_HOST)
    await mongo.db.seasonality.find_one_and_update(
        {"symbol": symbol},
        {"$set": {"symbol": symbol, "seasonality": seasonality, "type": _type}},
        upsert=True,
    )


async def create_seasonality():
    all_fmp_series = [
        {**x, "measurement": "historical_1d", "db": "fmp"}
        for x in await (get_all_series_fmp())
    ]
    all_yahoo_series = [
        {
            "symbol": symbol,
            "instrument_type": "stock",
            "measurement": "ohlcv",
            "db": "fpc_timeseries",
        }
        for symbol in await (DB(MONGO_HOST).get_symbols())
    ]

    for series in [*all_fmp_series, *all_yahoo_series]:
        symbol = series["symbol"]
        instrument_type = series["instrument_type"]
        measurement = series["measurement"]
        db = series["db"]

        try:
            print(instrument_type, symbol)
            seasonality = await (
                create_symbol_seasonality_influx(symbol, measurement, db)
            )
            await (store_in_mongo(seasonality, symbol, instrument_type))
        except Exception as err:
            print("error occurred for", symbol, err)


@click.command()
def click_seasonality():
    asyncio.run(create_seasonality())


if __name__ == "__main__":
    click_seasonality()
