import datetime
from typing import Any
import uuid


def convert_date(data: Any) -> datetime.date:
    """This function provides a base conversion mechanism 
    for transforming dates stored in the database into Python
    `datetime.date` objects. It is registered via `sqlite3.register_converter`, 
    to convert date fields when data is fetched from the database, ensuring 
    that date values are properly interpreted as Python date objects"""
    data = data.decode('utf-8')
    return datetime.datetime.strptime(data, '%Y-%m-%d')


def convert_datetime(data: Any) -> datetime.datetime:
    """This function provides a base conversion mechanism 
    for transforming dates stored in the database into Python
    `datetime.datetime` objects. It is registered via `sqlite3.register_converter`, 
    to convert date fields when data is fetched from the database, ensuring 
    that date values are properly interpreted as Python date objects"""
    data = data.decode('utf-8')
    try:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f%z')
    except:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')


def convert_timestamp(data: Any) -> datetime.datetime:
    """This function provides a base conversion mechanism 
    for transforming dates stored in the database into Python
    `datetime.datetime.timestamp` objects. It is registered via 
    `sqlite3.register_converter`, to convert date fields when data is 
    fetched from the database, ensuring that date values are properly 
    interpreted as Python date objects"""
    return datetime.datetime.fromtimestamp(int(data))


def convert_boolean(data: Any) -> bool:
    """This function provides a base conversion mechanism 
    for transforming boolean values stored in the database into Python
    `bool` objects. It is registered via `sqlite3.register_converter`, 
    to convert boolean fields when data is fetched from the database, ensuring 
    that boolean values are properly interpreted as Python bool objects"""
    decoded_data = data.decode('utf-8')
    return True if decoded_data == '1' else False


def convert_uuid(data: Any) -> uuid.UUID:
    """This function provides a base conversion mechanism 
    for transforming UUID values stored in the database into Python
    `str` objects. It is registered via `sqlite3.register_converter`, 
    to convert UUID fields when data is fetched from the database, ensuring 
    that UUID values are properly interpreted as Python str objects"""
    return uuid.UUID(data.decode('utf-8'))
