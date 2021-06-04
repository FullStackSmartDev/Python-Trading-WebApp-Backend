import asyncio
import datetime
import json
import logging
import os
import time

import aiocron
import aiohttp
import lxml.html
from aiohttp import web
from bson import json_util
from dotenv import load_dotenv

from cache import Cache
from db import DB
from FMP import FinancialModelingPrep
from investor_deck import InvestorDeck
from strategies import STRATEGIES, Strategy
from timeseries.buckets import get_data_to_aggregate, update_buckets
from timeseries.cron import create_crontabs, update_data
from timeseries.db import (
    get_all_bucket_candles,
    get_bucket_candles,
    get_candles,
    get_day_coverage,
    get_latest_record,
    get_period_OHLV,
    get_short_interest,
    get_stocklist_candles,
    influx_res_to_dict,
    store_short_interest,
)
from timeseries.gapFill import fillGaps
from timeseries.today import TickerManager
from timeseries.yahoo_finance import Interval, YahooFinance

load_dotenv()
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s", level=logging.INFO
)

db = DB()
tickermanager = TickerManager()
routes = web.RouteTableDef()

try:
    investorDeckApi = InvestorDeck(os.environ["INVESTOR_DECK_API_KEY"])
except Exception as e:
    print("Missing Environ INVESTOR_DECK_API_KEY", e)
    investorDeckApi = "MISSING"


@routes.get("/stocks")
async def get_stocks(request):
    stocks = await db.get_symbols()
    return web.json_response(stocks)


@routes.get("/search")
async def get_search(request):
    result = await db.get_search_options()
    return web.json_response(result)


@routes.get("/short-interest")
async def get_short_interest_data(request):
    query = request.query
    symbol = query["symbol"]
    res = await get_short_interest(symbol)
    return web.json_response(influx_res_to_dict(res))


@routes.get("/quotes")
async def get_stock_quotes(request):
    query = request.query
    symbol = query["symbol"]
    interval = query["interval"]
    try:
        start = datetime.datetime.fromisoformat(query["start"])
        end = datetime.datetime.fromisoformat(query["end"])
    except ValueError:
        return web.json_response(
            {"error": "Invalid date format: Use ISO format YYYY-MM-DD"}
        )
    res = await get_candles(start=start, end=end, symbol=symbol, interval=interval)
    # patch today's ohlcv

    if "latest" in query:

        try:
            patched_candle = {}
            # need to get close value no matter what, so grab day candle
            try:
                day_candle = tickermanager.get_ohlcv(symbol)
            except KeyError:
                day_candle = {
                    "timestamp": None,
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": None,
                }

            if interval == "1wk":
                try:
                    patched_candle = await get_period_OHLV(interval, symbol)
                    patched_candle["close"] = day_candle["close"]
                    print(patched_candle)
                except KeyError:
                    patched_candle = {
                        "timestamp": None,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": None,
                        "volume": None,
                    }

            elif interval == "1mo":
                try:
                    patched_candle = await get_period_OHLV(interval, symbol)
                    patched_candle["close"] = day_candle["close"]

                except KeyError:
                    patched_candle = {
                        "timestamp": None,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": None,
                        "volume": None,
                    }

            # if first day of month, won't have data in db yet, so use day candle
            if interval == "1mo" and datetime.date.today() == 1:
                patched_candle = day_candle

            # if first day of week, won't have data in db yet, so use day candle
            if interval == "1wk" and datetime.datetime.today().weekday() == 0:
                patched_candle = day_candle

            if interval == "1d":
                patched_candle = day_candle

            if all(
                [
                    patched_candle[x]
                    for x in ["timestamp", "open", "high", "low", "close"]
                ]
            ):

                res["values"].append(
                    [
                        patched_candle["timestamp"],
                        patched_candle["open"],
                        patched_candle["high"],
                        patched_candle["low"],
                        patched_candle["close"],
                        patched_candle["volume"],
                        interval,
                        symbol,
                        "patched",
                    ]
                )
            else:
                print("patched_candle failing for interval:", interval, patched_candle)
        except Exception as e:
            print("Patching failed for ", symbol, e)

    return web.json_response(res)


@routes.get("/bucket")
async def bucket_candles(request):

    query = request.query
    interval = query["interval"]
    bucket = query["bucket"]

    try:
        start = datetime.datetime.fromisoformat(query["start"])
        end = datetime.datetime.fromisoformat(query["end"])

    except ValueError:
        return web.json_response(
            {"error": "Invalid date format: Use ISO format YYYY-MM-DD"}
        )

    res = await get_bucket_candles(
        start=start, end=end, interval=interval, bucket=bucket
    )

    return web.json_response(res)


@routes.get("/algosort")
async def algo_sort(request):

    query = request.query
    strategy = query["strategy"]
    interval = query["interval"]

    try:
        start = datetime.datetime.fromisoformat(query["start"])
        end = datetime.datetime.fromisoformat(query["end"])

    except ValueError:
        return web.json_response(
            {"error": "Invalid date format: Use ISO format YYYY-MM-DD"}
        )

    stocklist = await db.get_stocks_in_strategy(strategy)
    stocklist_candles = await get_stocklist_candles(start, end, interval, stocklist)

    response = stocklist_candles
    spy = await get_candles(start=start, end=end, symbol="SPY", interval="1d")
    response["spy"] = spy

    return web.json_response(response)


@routes.get("/dashboard")
async def dashboard_candles(request):

    query = request.query
    # interval = query["interval"]

    try:
        start = datetime.datetime.fromisoformat(query["start"])
        end = datetime.datetime.fromisoformat(query["end"])

    except ValueError:
        return web.json_response(
            {"error": "Invalid date format: Use ISO format YYYY-MM-DD"}
        )

    bucket_candles = await get_all_bucket_candles(start, end)

    spy = await get_candles(start=start, end=end, symbol="SPY", interval="1d")

    bucket_candles["spy"] = spy
    return web.json_response(bucket_candles)


@routes.get("/today-debug")
async def get_today_ohlcv(request):
    return web.json_response(tickermanager.get_all_ohlcv())


@routes.get("/today-debug/{ticker}")
async def get_today_ohlcv_ticker(request):
    return web.json_response(tickermanager.get_ohlcv(request.match_info["ticker"]))


async def get_last_points(symbols):
    res = {}
    for symbol in symbols:
        res[symbol] = {}
        for interval in [Interval.ONE_DAY, Interval.ONE_WEEK, Interval.ONE_MONTH]:
            res[symbol][interval.value] = influx_res_to_dict(
                await get_latest_record(symbol, interval.value)
            )
    return res


@routes.get("/debug/last-point")
async def get_last_point_all(request):
    symbols = await db.get_symbols()
    return web.json_response(await get_last_points(symbols))


@routes.get("/debug/last-point/{symbol}")
async def get_last_point_symbol(request):
    return web.json_response(await get_last_points([request.match_info["symbol"]]))


@routes.get("/debug/update/{symbol}")
async def update_symbol_data(request):
    symbol = request.match_info["symbol"]
    for interval in [Interval.ONE_DAY, Interval.ONE_WEEK, Interval.ONE_MONTH]:
        await update_data(interval, [symbol])
    return await get_last_point_symbol(request)


@routes.get("/debug-datecoverage")
async def datecoverage(request):

    query = request.query

    try:
        start = datetime.datetime.fromisoformat(query["start"])
        end = datetime.datetime.fromisoformat(query["end"])
        interval = query["interval"]

    except ValueError:
        return web.json_response(
            {"error": "Invalid date format: Use ISO format YYYY-MM-DD"}
        )
    return web.json_response(
        await get_day_coverage(start=start, end=end, interval=interval)
    )


@routes.get("/debug-buckets")
async def debugbuckets(request):

    query = request.query

    stocklist = json.loads(query["stocklist"])
    bucketname = query["bucketname"]

    return web.json_response(
        await get_data_to_aggregate(bucketname, stocklist, "1d", startover=True)
    )


@routes.get("/debug-reaggregate-buckets_ow38u5o283u523g45n235tlkwj35235k")
async def reaggregate_buckets_web(request):

    asyncio.ensure_future(update_buckets(app=None, startover=True))
    return web.json_response("Beginning async aggregation")


@routes.get("/debug-gapfill_98w3y593852435jnk43645646n")
async def gapfill_web(request):
    asyncio.ensure_future(
        fillGaps(
            beginningYear=1970,
            dryRun=False,
            adjust_to_ipo=False,
            allow_empty_stocks=True,
        )
    )
    return web.json_response("Beginning GapFill")


@routes.get("/")
async def handle(request):
    name = request.match_info.get("name", "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)


@routes.get("/screener/{slug}")
async def screened_stocks(request):
    return web.json_response(
        await db.get_screened_stocks(request.match_info["slug"]), dumps=json_util.dumps
    )


@routes.get("/meta/screener/{slug}")
async def screener_meta(request):
    slug = request.match_info["slug"]
    return web.json_response(
        await db.db.strategies.find_one({"slug": slug}), dumps=json_util.dumps
    )


@routes.get("/meta/{stock}/{screener}")
async def screened_stock_details(request):
    stock = request.match_info["stock"].upper()
    screener_slug = request.match_info["screener"]
    data = await db.get_stock_meta_for_strategy(stock, screener_slug)
    try:
        data["presentation_urls"] = await investorDeckApi.get_company_presentation_urls(
            stock
        )
    except Exception:
        data["presentation_urls"] = ""
    return web.json_response(
        data,
        dumps=json_util.dumps,
    )


async def update_data_ipos():
    symbols = await db.get_symbols_for_strategy("recently-listed")
    print("updating IPOs data for", symbols)
    for interval in [Interval.ONE_DAY, Interval.ONE_WEEK, Interval.ONE_MONTH]:
        await update_data(interval, symbols)


async def update_fundamentals(screener, csv):
    await db.update_fundamentals_data(screener, csv)
    if STRATEGIES[int(screener)].slug == "recently-listed":
        await update_data_ipos()


@routes.get("/debug/update-ipos")
async def update_ipos(request):
    asyncio.create_task(update_data_ipos())
    return web.json_response({"status": "ok"})


@routes.post("/fundamentals")
async def update_fundamentals_data(request):
    json = await request.json()
    print(json)
    csv = json["csv"]
    screener = json["screener"]
    if int(screener) == 15:
        # store short-interest timeseries
        strategy = Strategy.from_yml("short-interest")
        stocks = strategy.csv_to_db_object(csv)
        now = datetime.datetime.today()
        snapped_timestamp = datetime.datetime(
            day=15 if now.day >= 15 else 1, month=now.month, year=now.year
        )
        stocks = [{**stock, "timestamp": snapped_timestamp} for stock in stocks]
        asyncio.create_task(store_short_interest(stocks))
    else:
        asyncio.create_task(update_fundamentals(screener, csv))
    return web.Response(
        text="ok",
        # headers={
        # "Access-Control-Allow-Methods": "OPTIONS, GET, POST",
        # "Access-Control-Allow-Origin": "*",
        # "Access-Control-Allow-Headers": "X-PINGOTHER, Content-Type",
        # }
    )


@routes.get("/news/{symbol}")
async def get_stock_news(request):
    cache = request.app["cache"]
    FMP = request.app["FMP"]

    symbol = request.match_info["symbol"].upper()
    cacheKey = f"news:{symbol.upper()}"

    data = await cache.getCachedOrGetFromSourceAndCache(
        cacheKey, FMP.get_interleaved_news(symbol), expiry=datetime.timedelta(days=1)
    )
    return web.json_response(data)


@routes.get("/sm")
async def get_yahoo_symbols(request):
    q = request.query.get("q")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": q, "quotesCount": 10},
        ) as response:
            return web.json_response(await response.json())


# @routes.options("/fundamentals")
async def options_req(request):
    return web.Response(
        headers={
            "Access-Control-Allow-Methods": "OPTIONS, GET, POST",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "X-PINGOTHER, Content-Type",
        }
    )


async def start_mongo(app):
    await db.init()
    print("mongo connected")


yf = YahooFinance()


# at every hour from 8 to 16 from monday through friday
@aiocron.crontab("0 8-16 * * 1-5")
async def get_latest_price(stocks=["AAPL", "NVDA", "GOOG"]):
    async for stock in db.db.strategies.all_stocks.find(
        {}, {"symbol": 1, "fundamentals.shares_outstanding": 1}
    ):
        try:
            symbol = stock["symbol"]
            shares_outstanding = float(stock["fundamentals"]["shares_outstanding"])
            price = await yf.get_last_price(symbol)

            new_market_cap = price * shares_outstanding
            await db.db.strategies.all_stocks.update_one(
                {"symbol": symbol},
                {"$set": {"fundamentals.market_cap": new_market_cap}},
            )
            # print(f"updating {symbol}'s market_cap: {new_market_cap}")

        except Exception as e:
            print(f"price: {price}, shares_outstanding: {shares_outstanding}")
            print(f"Exception in updating market cap {e}")


# once everyday
@aiocron.crontab("0 0 * * *")
async def update_float_shares():
    for symbol in await db.get_symbols():
        try:
            float_shares = await yf.get_key_statistic_float_shares(symbol)
            if "raw" in float_shares:
                await db.db.strategies.all_stocks.update_one(
                    {"symbol": symbol},
                    {"$set": {"fundamentals.float_shares": float_shares["raw"]}},
                )
            else:
                logging.debug(f"no float shares data for {symbol}")
                await db.db.strategies.all_stocks.update_one(
                    {"symbol": symbol},
                    {"$set": {"fundamentals.float_shares": None}},
                )
        except Exception:
            logging.exception(
                f"exception occurred getting and setting float shares for {symbol}",
            )


# refresh ticker after market open 9:30 EST
def refresh_tickers_factory():
    async def _wrapper():
        print(f"refreshing tickers at {datetime.datetime.now()}")
        await tickermanager.bootstrap()
        print(f"refreshing tickers finished at {datetime.datetime.now()}")

    return _wrapper


def clear_tickers_factory():
    async def _wrapper():
        print(f"clearing tickers at {datetime.datetime.now()}")
        tickermanager.clear_all()
        print(f"clearing tickers finished at {datetime.datetime.now()}")

    return _wrapper


async def schedule_tickermanager_actions(app):

    # Yahoo's endpoints have some instability after market open, so we refresh multiple times to eventually fill with
    # correct values. Whole bootstrapping takes ~ 30 sec
    aiocron.crontab("32 9 * * 1-5")(refresh_tickers_factory())
    aiocron.crontab("0 10 * * 1-5")(refresh_tickers_factory())

    # EOD snag all canonical values (remove drift caused by streaming price)
    aiocron.crontab("05 16 * * 1-5")(refresh_tickers_factory())
    aiocron.crontab("20 16 * * 1-5")(refresh_tickers_factory())

    # Clear last day's tickers.
    aiocron.crontab("0 0 * * *")(clear_tickers_factory())


# Reaggregate at 4 in the morning
@aiocron.crontab("0 4 * * 1-5")
async def reaggregate_buckets():
    await update_buckets(app=None, startover=True)


async def schedule_cron(app):
    asyncio.create_task(create_crontabs(db))


# Fill gaps at 2 in the morning
@aiocron.crontab("0 2 * * *")
async def schedule_gapFill():
    await fillGaps()


async def start_tickermanager(app):
    tickermanager.set_symbols(await db.get_symbols())
    start_time = time.time()
    print(f"starting bootstrapping {start_time}, at {datetime.datetime.now()}")
    await tickermanager.bootstrap()
    print(f"bootstrapping finished in {time.time() - start_time} seconds")
    asyncio.create_task(tickermanager.start())


async def attach_cache(app):
    cache = Cache()
    await cache.init()
    app["cache"] = cache


async def attach_fmp(app):
    app["FMP"] = FinancialModelingPrep(os.environ.get("FMP_KEY"))


def init_cron(cron):
    async def _decorated(app):
        asyncio.create_task(cron.func())

    return _decorated


async def get_lockup_data():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://www.marketbeat.com/ipos/lockup-expirations/"
        ) as resp:
            html = await resp.text()
            tree = lxml.html.fromstring(html)

            data = []

            for tr in tree.xpath("//table/tbody/tr"):
                try:
                    tds = tr.xpath(".//td")
                    [
                        current_price,
                        expiration_date,
                        number_of_shares,
                        initial_share_price,
                        offer_size,
                        date_period,
                    ] = [x.text for x in tds[1:]]
                    symbol = tds[0].xpath('.//div[@class="ticker-area"]')[0].text

                    parse_number = lambda x: float(
                        x.strip().replace("$", "").replace(",", "")
                    )

                    data.append(
                        {
                            "current_price": parse_number(current_price),
                            "symbol": symbol,
                            "expiration_date": expiration_date,
                            "number_of_shares": parse_number(number_of_shares),
                            "initial_share_price": parse_number(initial_share_price),
                            "offer_size": parse_number(offer_size),
                            "date_period": date_period,
                        }
                    )
                except Exception:
                    logging.exception(
                        "exception occurred in parsing a row of ipo lockup"
                    )

            return data


@aiocron.crontab("0 0 * * *")
async def patch_lockup_data():
    lockup_data = await get_lockup_data()
    for datum in lockup_data:
        symbol = datum["symbol"]
        await db.db.strategies.all_stocks.update_one(
            {"symbol": symbol},
            {"$set": {"fundamentals.ipo_lockup_data": datum}},
        )


app = web.Application(client_max_size=1024 * 1000 * 10)
app.on_startup.append(start_mongo)
app.on_startup.append(attach_cache)
app.on_startup.append(attach_fmp)

if "DEV" not in os.environ:
    app.on_startup.append(schedule_cron)
    app.on_startup.append(start_tickermanager)
    app.on_startup.append(schedule_tickermanager_actions)
app.on_startup.append(init_cron(update_float_shares))
app.on_startup.append(init_cron(patch_lockup_data))

app.add_routes(routes)
app.add_routes(
    [
        web.options("/{tail:.*}", options_req)
        # web.options("/fundamentals", options_req),
        # web.options("/screener/{slug}", options_req),
        # web.options("/meta/{stock}/{slug}", options_req),
    ]
)

if __name__ == "__main__":
    port = 8080
    try:
        port = int(os.environ.get("PORT"))
    except (ValueError, TypeError):
        pass
    web.run_app(app, port=port)
