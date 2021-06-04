import asyncio
import datetime
import math

import click
import numpy
import pandas

from timeseries.db import store_short_interest


@click.command()
@click.argument("csv")
def import_csv(csv):
    df = pandas.read_csv(csv)
    points = []
    for i, row in df.iterrows():
        if row.empty:
            continue
        try:
            [month, day, year] = row.date.split("/")
            day = 1 if int(day) < 15 else 15
            timestamp = datetime.datetime(
                day=int(day), month=int(month), year=int("20" + year)
            )
        except Exception as ex:
            print("Cant process", row, ex)
            continue

        def getval(attr, caster=float):
            try:
                val = caster(row[attr])
                if isinstance(val, str):
                    return val
                if numpy.isnan(val):
                    val = None
            except (ValueError, TypeError):
                val = None
            return val

        symbol = getval("Symbol", str)
        if symbol is None:
            continue

        point = {
            "symbol": getval("Symbol", str),
            "name": getval("ShortSqueeze.com Short Interest Data", str),
            "short_interest": getval("Total Short Interest"),
            "days_to_cover_short": getval("Days to Cover"),
            "float_short": getval("Short % of Float"),
            "insider_ownership": getval("% Insider Ownership"),
            "institutional_investors_ownership_percent": getval(
                "% Institutional Ownership"
            ),
            "average_daily_volume_30d": getval("Avg. Daily Vol."),
            "price": getval("Price"),
            "market_cap": getval("Market Cap"),
            "sector": getval("Sector", str),
            "industry": getval("Industry", str),
            "timestamp": timestamp,
        }

        points.append(point)

    chunk_size = 10000

    for i in range(math.ceil(len(points) / chunk_size)):
        asyncio.get_event_loop().run_until_complete(
            store_short_interest(points[i * chunk_size : (i + 1) * chunk_size])
        )
        print("pushing points", i * chunk_size, (i + 1) * chunk_size)


if __name__ == "__main__":
    import_csv()
