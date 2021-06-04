import datetime
import os
import unittest

import pytest
import timeseries.db
from aioinflux import InfluxDBClient


@pytest.mark.asyncio
class TestTimeseries(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db_name = "fpc_timeseries_test"
        self.db_host = os.environ.get("INFLUX_HOST") or "localhost"

        # create test database
        async with InfluxDBClient(db=self.db_name, host=self.db_host) as client:
            await client.create_database(db=self.db_name)

        # patch default db
        timeseries.db.DB_NAME = "fpc_timeseries_test"

    async def test_store_candles_overwriting(self):
        timestamp = datetime.datetime.today()
        points = [
            {
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 1000,
                "interval": "1d",
                "symbol": "AAPL",
                "timestamp": int(timestamp.timestamp()) * (10 ** 9),
            },
        ]
        await timeseries.db.store_candles(points)
        earlier = timeseries.db.influx_res_to_dict(
            await timeseries.db.get_candles(
                timestamp - datetime.timedelta(minutes=100000),
                timestamp + datetime.timedelta(minutes=100000),
                "1d",
                "AAPL",
            )
        )
        self.assertListEqual(earlier, points)

        # overwrite
        points[0]["close"] = 12
        await timeseries.db.store_candles(points)
        after = timeseries.db.influx_res_to_dict(
            await timeseries.db.get_candles(
                timestamp - datetime.timedelta(minutes=100000),
                timestamp + datetime.timedelta(minutes=100000),
                "1d",
                "AAPL",
            )
        )
        self.assertListEqual(after, points)

        # test same point, different timestamp, should be 2 rows
        point2 = points[0]
        point2.update(
            {"timestamp": int(datetime.datetime.today().timestamp()) * (10 ** 9)}
        )
        points.append(point2)
        await timeseries.db.store_candles(points)
        final = timeseries.db.influx_res_to_dict(
            await timeseries.db.get_candles(
                timestamp - datetime.timedelta(minutes=100000),
                timestamp + datetime.timedelta(minutes=100000),
                "1d",
                "AAPL",
            )
        )
        self.assertNotEqual(final, points)

    async def asyncTearDown(self):
        # drop database at the end of test
        async with InfluxDBClient(db=self.db_name, host=self.db_host) as client:
            await client.drop_database(db=self.db_name)
