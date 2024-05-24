import datetime
from typing import Any


def convert_date(data: Any) -> datetime.datetime.date: ...


def convert_datetime(data: Any) -> datetime.datetime: ...


def convert_timestamp(data: Any) -> datetime.datetime.timestamp: ...
