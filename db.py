import asyncio
from typing import List

from motor.motor_asyncio import AsyncIOMotorClient

from strategies import STRATEGIES, Strategy


class DB:
    def __init__(self, host="localhost"):
        self.client = AsyncIOMotorClient(host=host)

    @property
    def db(self):
        return self.client["fpc"]

    async def init(self):
        strategies = [strategy.meta for strategy in STRATEGIES.values()]
        await self.db.strategies.delete_many({})
        await self.db.strategies.insert_many(strategies)

    async def update_fundamentals_data(self, screener, csv):
        strategy = STRATEGIES[int(screener)]
        stocks = strategy.csv_to_db_object(csv)
        # print(stocks)

        prev_stocks = await self.get_stocks_in_strategy(strategy.slug)

        old_stocks = []
        new_stocks = []

        for stock in stocks:
            if stock["symbol"] in prev_stocks:
                old_stocks.append(stock)
            else:
                new_stocks.append(stock)

        collection_name = strategy.slug.replace("-", "_")

        # update existing stocks
        for stock in old_stocks:
            await self.db.strategies[collection_name].find_one_and_update(
                {"symbol": stock["symbol"]}, {"$set": stock}
            )

        # insert new stocks
        if len(new_stocks) > 0:
            await self.db.strategies[collection_name].insert_many(new_stocks)

        # delete stocks no longer in list
        to_delete = []
        current_stocks = [x["symbol"] for x in stocks]
        for stock in prev_stocks:
            if stock not in current_stocks:
                to_delete.append(stock)

        if len(to_delete) > 0 and strategy.slug != "all-stocks":
            await self.db.strategies[collection_name].delete_many(
                {"symbol": {"$in": to_delete}}
            )

    async def get_stock_data(self, strategy_slug, symbol):
        all_stocks_data = await self.db.strategies.all_stocks.find_one(
            {"symbol": symbol}
        )

        if all_stocks_data is None:
            return None

        strategy_data = await self.db.strategies[
            strategy_slug.replace("-", "_")
        ].find_one({"symbol": symbol}) or {"fundamentals": {}}

        return {
            **all_stocks_data["fundamentals"],
            **self.sub_dict(
                strategy_data["fundamentals"],
                Strategy.from_yml(strategy_slug).precedented_attrs,
            ),
        }

    async def get_screened_stocks(self, screener_slug):
        stocks = []
        async for stock in self.db.strategies[screener_slug.replace("-", "_")].find():
            stock = {"symbol": stock["symbol"]}
            stock_data = await self.get_stock_data(screener_slug, stock["symbol"])
            if stock_data is None:
                continue
            stock.update({"data": stock_data})
            stocks.append(stock)

        meta = await self.db.strategies.find_one({"slug": screener_slug})
        common_meta = await self.db.strategies.find_one({"slug": "all-stocks"})
        return {"meta": meta, "stocks": stocks, "common_meta": common_meta}

    async def get_stocks_in_strategy(self, screener_slug):
        stocks = []
        async for stock in self.db.strategies[screener_slug.replace("-", "_")].find():
            stocks.append(stock["symbol"])

        return stocks

    @staticmethod
    def sub_dict(data: dict, needed_keys: List[str]):
        return {key: data[key] for key in needed_keys if key in data}

    async def get_stock_meta_for_strategy(self, symbol, strategy_slug):
        all_stocks_meta = await self.db.strategies.find_one({"slug": "all-stocks"})
        strategy_meta = await self.db.strategies.find_one({"slug": strategy_slug})

        return {
            "symbol": symbol,
            "strategy": strategy_meta,
            "attrs_slug_to_name": {
                **all_stocks_meta["attrs_slug_to_name"],
                **self.sub_dict(
                    strategy_meta["attrs_slug_to_name"],
                    Strategy.from_yml(strategy_slug).precedented_attrs,
                ),
            },
            "meta": await self.get_stock_data(strategy_slug, symbol),
        }

    async def get_symbols_for_strategy(self, slug):
        stocks = []
        async for stock in self.db.strategies[slug.replace("-", "_")].find():
            stocks.append(stock["symbol"])
        return stocks

    async def get_symbols(self):
        return await self.get_symbols_for_strategy("all-stocks")

    async def get_search_options(self):

        searchOptions = []

        async for strategy in self.db.strategies.find():
            async for stock in self.db.strategies[
                strategy["slug"].replace("-", "_")
            ].find():

                try:
                    obj = {
                        "strategy-slug": strategy["slug"],
                        "strategy-name": strategy["name"],
                        "symbol": stock["symbol"],
                        "name": stock["fundamentals"]["name"],
                    }

                except KeyError:
                    obj = {
                        "strategy-slug": strategy["slug"],
                        "strategy-name": strategy["name"],
                        "symbol": stock["symbol"],
                        "name": stock["fundamentals"]["company"],
                    }

                if strategy["slug"] == "all-stocks":
                    obj["exchange"] = stock["fundamentals"]["exchange_name"]

                searchOptions.append(obj)

        return searchOptions


async def main():
    db = DB()
    await db._init_task


if __name__ == "__main__":
    asyncio.run(main())
