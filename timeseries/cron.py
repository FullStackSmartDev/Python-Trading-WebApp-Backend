import asyncio
import datetime
import time
from typing import List

import aiocron


from .db import get_latest_timestamp, store_candles
from .yahoo_finance import Interval, YahooFinance

# from timeseries.buckets import update_buckets

cron_map = {
    # "1m": "* 9-16 * * 1-5",
    Interval.ONE_MINUTE: "* * * * * *",
    Interval.TWO_MINUTE: "*/2 9-16 * * 1-5",
    Interval.FIVE_MINUTE: "*/5 9-16 * * 1-5",
    Interval.FIFTEEN_MINUTE: "*/15 9-16 * * 1-5",
    Interval.THIRTY_MINUTE: "*/30 9-16 * * 1-5",
    Interval.SIXTY_MINUTE: "0 9-16 * * 1-5",
    Interval.NINTY_MINUTE: "*/90 9-16 * * 1-5",
    Interval.ONE_HOUR: "0 9-16 * * 1-5",
    # Interval.ONE_DAY: "*/15 9-16 * * 1-5",
    # Interval.ONE_DAY: "0 16 * * 1-5",
    Interval.ONE_DAY: "0 20 * * 1-5",
    Interval.ONE_WEEK: "0 20 * * 1-5",
    Interval.ONE_MONTH: "0 20 * * 1-5",
    Interval.FIVE_DAY: "0 20 * * 5",
    # Interval.ONE_WEEK: "0 0 * * 0",
    # Interval.ONE_WEEK: "*/15 9-16 * * 1-5",
    # Interval.ONE_MONTH: "*/15 9-16 * * 1-5",
    # Interval.ONE_MONTH: "0 0 1 */1 *",
    Interval.THREE_MONTH: "0 0 1 */3 *",
}


# @aiocron.crontab(cron_map[Interval.ONE_MINUTE])
# async def get_latest_price(stocks=["AAPL", "NVDA", "GOOG"]):
#     for stock in stocks:
#         print(stock, await yf.get_last_price(stock))


def calibrate_timestamp(ts: int, interval: Interval):
    og_ts = datetime.datetime.fromtimestamp(ts)
    normalized_date_ts = datetime.datetime(
        day=og_ts.day, month=og_ts.month, year=og_ts.year
    )

    if interval == Interval.ONE_DAY:
        return normalized_date_ts
    elif interval == Interval.ONE_WEEK:
        return normalized_date_ts - datetime.timedelta(
            days=normalized_date_ts.isoweekday() - 1
        )
    elif interval == Interval.ONE_MONTH:
        return normalized_date_ts - datetime.timedelta(days=normalized_date_ts.day - 1)

    return datetime.datetime.fromtimestamp(ts)


async def update_data(interval: Interval, stocks: List[str]):
    # print(interval)

    for stock in stocks:
        print(f"update_data for {stock}")
        try:
            start = datetime.datetime.fromtimestamp(
                await get_latest_timestamp(symbol=stock, interval=interval.value)
            )
            # print(start)

            end = datetime.datetime.now()
            new_data = await YahooFinance().get_historical_data(
                stock, start, end, interval
            )

            # print(start, end, stock)
            # delta = (end - start).total_seconds()

            # skip = False
            # if interval == Interval.ONE_DAY:
            # if delta < 24 * 60 * 60:
            # skip = True
            # elif interval == Interval.ONE_WEEK:
            # if delta < 7 * 24 * 60 * 60:
            # skip = True
            # elif interval == Interval.ONE_MONTH:
            # if delta < 25 * 24 * 60 * 60:
            # skip = True

            # if skip:
            # print(f"skipping {stock} for interval {interval}")

            points = []
            symbol = stock.replace("-", ".") if "-" in stock else stock

            for timestamp, open, high, low, close, volume in zip(
                new_data["timestamps"],
                new_data["open"],
                new_data["high"],
                new_data["low"],
                new_data["close"],
                new_data["volume"],
            ):
                ts = calibrate_timestamp(timestamp, interval)

                if all([open, high, low, close]):
                    points.append(
                        dict(
                            timestamp=ts,
                            open=open,
                            high=high,
                            low=low,
                            close=close,
                            volume=volume,
                            symbol=symbol,
                            interval=interval.value,
                        )
                    )
                else:
                    print(
                        f"skipping {ts} for {stock}: ohlc: [{open}, {high}, {low},"
                        f" {close}]"
                    )

            await store_candles(points)

        except Exception as ex:
            print(f"update_data failed for {stock} because: {ex}")
            # most exceptions here were because symbol was delisted, so silent exception handling for now
            continue


async def update_data_chunked(interval, stocks, chunk=60):
    tasks = []
    start_time = time.time()
    print(f"started update at {time.strftime('%H:%M:%S')}")
    for i in range(0, len(stocks), chunk):
        chunked = stocks[i : i + chunk]
        tasks.append(asyncio.create_task(update_data(interval, chunked)))
    await asyncio.wait(tasks)
    end_time = time.time()
    print(f"data update finished for {interval} in {end_time - start_time} seconds")


def update_data_factory(interval, stocks):
    async def _wrapper():
        return await update_data_chunked(interval, stocks)

    return _wrapper


async def create_crontabs(client):
    stocks = []
    async for stock in client.db.strategies.all_stocks.find():
        symbol = (
            stock["symbol"].replace(".", "-")
            if "." in stock["symbol"]
            else stock["symbol"]
        )
        stocks.append(symbol)

    to_update = [Interval.ONE_DAY, Interval.ONE_WEEK, Interval.ONE_MONTH]

    for duration in to_update:
        # one off update on each restart
        # print(
        #     "-------- One off data update for",
        #     duration,
        #     "beginning at ",
        #     datetime.datetime.today(),
        # )

        # Commented out because gap filling happens at night now with ./gapFill

        # await update_data_chunked(duration, stocks)
        # update cron
        aiocron.crontab(cron_map[duration])(update_data_factory(duration, stocks))

    # One off update of buckets each restart
    # print("-------- Restart Bucket Update beginning at ", datetime.datetime.today())
    # await update_buckets(app=None, startover=True)
