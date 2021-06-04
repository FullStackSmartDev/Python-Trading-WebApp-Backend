from urllib.parse import urljoin, urlparse, parse_qs
import re
import datetime

import aiohttp
import typing


class CompanyPresentation(typing.TypedDict):
    Author: str
    AuthorEmail: str
    AuthorPic: str
    CreatedDate: str  # YYYY-MM-DDTHH:MM:SS
    CreatedPresentationDate: str  # Mar2018
    Description: str
    DocumentId: str
    IsApproved: bool
    IsCompany: bool
    IsLike: bool
    MarketCap: str
    PresentationType: int
    Thumbnail: str
    Ticker: str
    Title: str
    TotalDownloads: int
    TotalLikes: int
    TotalShares: int
    TotalViews: int
    UpdatedDate: str
    Url: str


class CompanyPresentationList(typing.TypedDict):
    Documents: typing.List[CompanyPresentation]


class InvestorDeck:
    _base_url = "https://api.investordeck.com/"

    def __init__(self, api_key):
        self._key = api_key

    def _get_url(self, endpoint):
        return urljoin(self._base_url, endpoint)

    async def _get(self, url: str, params: dict = {}):
        """Helper method for creating authenticated GET requests"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"api-token": self._key}) as response:
                data = await response.json()
                return data

    async def get_company_presentations(
        self, symbol, limit=1000, offset=0
    ) -> CompanyPresentationList:
        """Get company presentations"""
        return await self._get(
            self._get_url(f"/company/archives/{symbol}/{limit}/{offset}")
        )

    @staticmethod
    def _get_expiry_from_aws_url(url: str) -> int:
        parsed_url = urlparse(url)
        parsed_qs = parse_qs(parsed_url.query)
        for key in parsed_qs:
            if key.lower() == "expires":
                if len(parsed_qs[key]) > 0:
                    return float(parsed_qs[key][0])
        return datetime.datetime.now().timestamp()

    @staticmethod
    def _parse_created_presentation_date(created_date: str):
        # format - Mar2018
        match = re.match(r"(\D+)(\d+)", created_date)
        month = match.group(1).lower()
        months = [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ]
        for i, regualar_month in enumerate(months, 1):
            if month in regualar_month:
                month = i
                break

        year = int(match.group(2))
        return datetime.datetime.combine(
            datetime.date(year, month, 1), datetime.datetime.min.time()
        )

    async def get_company_presentation_urls(self, symbol):
        presentations = await self.get_company_presentations(symbol)
        res = []
        for presentation in presentations["Documents"]:
            res.append(
                {
                    "url": presentation["Url"],
                    "expiry": self._get_expiry_from_aws_url(presentation["Url"]),
                    "date_created": presentation["CreatedPresentationDate"],
                    "ts_created": self._parse_created_presentation_date(
                        presentation["CreatedPresentationDate"]
                    ).timestamp(),
                }
            )

        try:
            return sorted(
                res,
                key=lambda x: self._parse_created_presentation_date(x["date_created"]),
            )
        except Exception as e:
            print("couldnt sort company presentations by date", e)
            return res
