import datetime


def convert_date(data):
    """This function provides a base conversion mechanism 
    for transforming dates stored in the database into Python
    `datetime.date` objects. It is registered via `sqlite3.register_converter`, 
    to convert date fields when data is fetched from the database, ensuring 
    that date values are properly interpreted as Python date objects"""
    data = data.decode('utf-8')
    return datetime.datetime.strptime(data, '%Y-%m-%d')


def convert_datetime(data):
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


def convert_timestamp(data):
    """This function provides a base conversion mechanism 
    for transforming dates stored in the database into Python
    `datetime.datetime.timestamp` objects. It is registered via 
    `sqlite3.register_converter`, to convert date fields when data is 
    fetched from the database, ensuring that date values are properly 
    interpreted as Python date objects"""
    return datetime.datetime.fromtimestamp(int(data))

