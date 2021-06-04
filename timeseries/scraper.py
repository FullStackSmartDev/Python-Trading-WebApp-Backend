import asyncio
import json
import os
import traceback

from yahoo_finance import Interval, YahooFinance


async def scrape_all_data(symbol):
    print(f"scraping {symbol}")
    yf = YahooFinance()

    # make directory
    os.makedirs("data", exist_ok=True)
    os.makedirs(f"data/{symbol}", exist_ok=True)

    tasks = {}

    for key in Interval.__members__.keys():
        interval = getattr(Interval, key)
        tasks[key] = asyncio.create_task(yf.get_all_data(symbol, interval))

    await asyncio.wait([task for task in tasks.values()])

    for interval in tasks:
        try:
            res = tasks[interval].result()
        except Exception as e:
            print(e)
            print(f"failed {symbol}")
            traceback.print_exc()
        else:
            with open(
                os.path.join("data", symbol, f"{symbol}-{interval}.json"), "w"
            ) as f:
                f.write(json.dumps(res))


async def main():
    symbols = []
    # with open("stocks.csv") as f:
    # first = True
    # for line in f.readlines():
    # if first:
    # first = False
    # continue
    # symbols.append(line.split(",")[0])

    with open("failed") as f:
        symbols = f.read().split("\n")

    # print(symbols)
    # yf = YahooFinance()
    # to = datetime.datetime.now()
    # frm = to - datetime.timedelta(days=30)

    # data = await yf.get_historical_data(symbol, frm, to, Interval.ONE_MINUTE)
    # with open("data.json", "w") as f:
    # f.write(json.dumps(data))
    for symbol in symbols:
        await scrape_all_data(symbol)
        # input('next: ?')


asyncio.run(main())
