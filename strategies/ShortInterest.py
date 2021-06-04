from .Category import Category
from .Strategy import Strategy
from .Type import Type

# not a strategu, but need for CSV parsing
class ShortInterest(Strategy):
    def __init__(self):
        self._id = 15
        self._name = "Time Series-Short Interest, Insider %, Institutional %"
        self._slug = "short-interest"
        self._encoded = [
            (0, "Symbol", "symbol", Type.STRING),
            (1, "Name", "name", Type.STRING),
            (2, "Short Interest", "short_interst", Type.AMOUNT),
            (3, "Days to Cover Short", "days_to_cover_short", Type.NUMBER),
            (4, "Percent of Float Short", "float_short", Type.PERCENT),
            (
                5,
                "Insider Ownership Percent",
                "insider_ownership",
                Type.PERCENT,
            ),
            (
                6,
                "Institutional Investers Ownership Percent",
                "institutional_investors_ownership_percent",
                Type.PERCENT,
            ),
            (7, "30 Day Average Daily Volume", "average_daily_volume_30d", Type.AMOUNT),
            (8, "Price", "price", Type.NUMBER),
            (9, "Market Cap", "market_cap", Type.AMOUNT_MILLIONS),
            (10, "Sector", "sector", Type.STRING),
            (11, "Industry", "industry", Type.STRING),
        ]

        self._filterable = []

        self._sortable = []

        self._notes = ""
        self._category = Category.CUSTOM
        self._dials = []
