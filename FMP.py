import datetime
from urllib.parse import urljoin

import aiohttp


class FinancialModelingPrep:
    def __init__(self, key):
        self._key = key
        self._basepath = "https://financialmodelingprep.com"

    async def _get(self, url, *args, **kwargs):
        async with aiohttp.ClientSession() as session:
            if "params" in kwargs:
                params = kwargs.pop("params")
                params["apikey"] = self._key
                kwargs["params"] = params
            async with session.get(self._get_url(url), *args, **kwargs) as response:
                return await response.json()

    def _get_url(self, endpoint):
        return urljoin(self._basepath, endpoint)

    async def get_income_statement(self, symbol, limit=120, period="quarter"):
        return await self._get(
            (f"/api/v3/income-statement/{symbol}"),
            params={"limit": limit, "period": period},
        )

    async def get_news(self, symbol, limit=50):
        return await self._get(
            "/api/v3/stock_news", params={"limit": limit, "tickers": symbol}
        )

    async def get_press_releases(self, symbol, limit=50):
        return await self._get(
            f"/api/v3/press-releases/{symbol}", params={"limit": limit}
        )

    def _parse_date(self, date: str) -> datetime.datetime:
        [date, time] = date.split(" ")
        return datetime.datetime.fromisoformat(f"{date}T{time}")

    async def get_interleaved_news(self, symbol):
        news = await self.get_news(symbol)
        press_releases = await self.get_press_releases(symbol)

        # symbol, publishedDate, title, image, site, text, url
        res = []
        for new in news:
            new["publishedDate"] = self._parse_date(new["publishedDate"])
            new["isPressRelease"] = False
            res.append(new)

        for press_release in press_releases:
            res.append(
                {
                    "symbol": symbol,
                    "publishedDate": self._parse_date(press_release["date"]),
                    "title": press_release["title"],
                    "image": "",
                    "site": "",
                    "text": press_release["text"],
                    "url": "",
                    "isPressRelease": True,
                }
            )

        return list(
            map(
                lambda x: {**x, "publishedDate": x["publishedDate"].isoformat()},
                sorted(res, key=lambda x: x["publishedDate"], reverse=True),
            )
        )
