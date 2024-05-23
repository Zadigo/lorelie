import datetime


def convert_date(data):
    """Base converter for transforming
    database dates to Python datatime.date
    objects"""
    data = data.decode('utf-8')
    return datetime.datetime.strptime(data, '%Y-%m-%d')


def convert_datetime(data):
    """Base converter for transforming
    database datetimes to Python datatime.datetime
    objects"""
    data = data.decode('utf-8')
    try:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f%z')
    except:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
