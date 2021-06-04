import datetime

import timeseries.cron
from timeseries.cron import calibrate_timestamp
from timeseries.yahoo_finance import Interval


def test_calibrate_timestamp():
    mocked_today = today = datetime.datetime(
        day=22, month=7, year=2020, hour=12, minute=35, second=54
    )

    # patch datetime in timeseries.cron
    class MockedDateTime(datetime.datetime):
        @staticmethod
        def today():
            return mocked_today

    timeseries.cron.datetime.datetime = MockedDateTime

    # 22nd July 2020 - Tuesday
    weekday = today.timestamp()
    one_day_out = datetime.datetime(day=22, month=7, year=2020)
    one_week_out = datetime.datetime(day=20, month=7, year=2020)
    one_month_out = datetime.datetime(day=1, month=7, year=2020)

    assert calibrate_timestamp(weekday, Interval.ONE_DAY) == one_day_out
    assert calibrate_timestamp(weekday, Interval.ONE_WEEK) == one_week_out
    assert calibrate_timestamp(weekday, Interval.ONE_MONTH) == one_month_out
