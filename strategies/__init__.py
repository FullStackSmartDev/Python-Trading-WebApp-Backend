from .Strategy import Strategy

yamls = [
    "all-stocks",
    "balance-sheet-strength",
    "cash-kings",
    "earnings-yield",
    "four-stars",
    "insider-ownership",
    "recently-listed",
    "movers-and-shakers",
    "real-buybacks",
    "revenue-growth",
    "rnd",
    "rocket-stocks",
    "short-squeeze-leaders",
    "short-squeeze-reversals",
    "short-interest",
]

STRATEGIES = {}
for yml in yamls:
    try:
        strategy = Strategy.from_yml(yml)
        STRATEGIES[strategy.id] = strategy

    except Exception as e:
        print(e)
