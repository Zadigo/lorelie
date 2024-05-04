import secrets
import asyncio
import time
import pathlib
from collections import namedtuple

# from lorelie.conf import settings
from lorelie import tables
from lorelie.expressions import Case, When
from lorelie.fields import BooleanField, CharField, Field, JSONField
from lorelie.functions import Count, ExtractYear, Lower, Max
from lorelie.migrations import Migrations
from lorelie.tables import Database, Table
from lorelie.backends import connections
from lorelie.tables import databases

# https://code.djangoproject.com/ticket/17741

# fields = [
#     CharField('url', max_length=500, unique=True),
#     BooleanField('visited', default=False),
#     Field('created_on')
# ]
# table = Table('something_urls', database_name='scraping', fields=fields)
# table.prepare()

# setattr(settings, 'PROJECT_PATH', pathlib.Path('.'))

table1 = Table('url', fields=[
    CharField('url')
])
table2 = Table('business', fields=[
    CharField('name')
])
database = Database('my_database', table1, table2)
# database.make_migrations()
# database.migrate()
database.objects.filter('url', )

# print(databases.database_map)
# print(connections.connections_map)
# print(connections.created_connections)


# async def main():
#     value = await database.objects.aget('business', id=12)
#     print(value)

# asyncio.run(main())

# print(database.objects.filter('business',
#     id__in=[15],
#     name__contains='Kendall')
# )
# print('New object', database.objects.create('url', url='http://google.com'))
# print('All', database.objects.all('url'))
# print('First', database.objects.first('url'))
# a = database.objects.last('business')

# a = database.objects.get('business', id__eq=12)
# a.delete()
# a['name'] = 'Kendall is love'
# # a.name = 'I changed this value'
# b = a.save()
# print(b)
# print(b.name)
# print(a.name)
# print('Filter', database.objects.filter('url', id__eq=3))
# print('Get', database.objects.get('url', id__eq=1))
# print('Get', database.objects.annotate('url', lowered_url=Lower('url')))
# print(dict(database.objects.first('url')))
# database.objects.create('business', name='Gucci')
# a = database.objects.annotate('business', url_count=Count('name'))
# print([vars(x) for x in a])
# print(database.objects.as_values('business', 'id', 'name'))
# print(database.objects.as_dataframe('business', 'id', 'name'))

# cases = When('name__eq=Gucci', 'Vuitton')
# case = Case(cases)
# print(database.objects.annotate('business', simple=case))


# while True:
#     name = secrets.token_hex(nbytes=5)
#     database.objects.create('business', name=f'Kendall_{name}')
#     item = database.objects.as_values('business', 'id', 'name')
#     print(item)
#     time.sleep(20)


# async def get_data():
#     return database.objects.last('business')


# async def save1(t):
#     h = secrets.token_hex(nbytes=5)
#     database.objects.create('business', name=f'Julie_{h}')
#     d = await t.create_task(get_data())
#     print(d)
#     await asyncio.sleep(10)


# async def save2(t):
#     h = secrets.token_hex(nbytes=5)
#     database.objects.create('business', name=f'Kylie_{h}')
#     await asyncio.sleep(13)


# async def main():
#     while True:
#         async with asyncio.TaskGroup() as t:
#             await t.create_task(save1(t))
#             await t.create_task(save2(t))
#         await asyncio.sleep(5)

# asyncio.run(main())

# def make_migrations(*tables):
#     """Writes the physical changes to the
#     local tables into the `migrations.json` file"""
#     import pathlib

#     from lorelie.conf import settings
#     settings['PROJECT_PATH'] = pathlib.Path(__file__).parent.parent.parent.joinpath('tests/testproject')
#     migrations = Migrations()
#     migrations.has_migrations = True
#     instances = {table.name: table}
#     migrations.migrate(instances)


# def migrate(*tables):
#     """Applies the migrations from the local
#     `migrations.json` file to the database"""
#     import pathlib

#     from lorelie.conf import settings
#     settings['PROJECT_PATH'] = pathlib.Path(__file__).parent.parent.parent.joinpath('tests/testproject')
#     migrations = Migrations()
#     instances = {table.name: table}
#     migrations.check(table_instances=instances)


# make_migrations()

# migrate()


# TODO: Implement cases
# 1. case when '1' then '2' else '3' end
# 2 case when '1' then '3' when '2' then '4'  else '5' end
# case {condition} end
# when {condition} then {then_value} else {else_value}

# TODO: Implement group by
# select rowid, *, count(rowid) from groupby rowid
# select rowid, *, count(rowid) from groupby rowid order by count(rowid) desc
# select rowid, *, count(rowid) from groupby rowid having count(rowid) > 1


# database = Database('seen_urls', table)
# database.make_migrations()
# database.migrate()

# table.create(url='http://google.com', visited=True)

# obj = namedtuple('Object', ['url'])
# table.bulk_create([obj('http://example.com')])
# import datetime
# table.create(url='http://example.com/1', visited=False, created_on=str(datetime.datetime.now()))

# r = table.get(rowid=4)

# r = table.filter(url__startswith='http')
# r = table.filter(url__contains='google')
# r = table.filter(rowid__in=[1, 4, 6])
# TODO: Use Field.to_database before evaluating the
# value to the dabase
# r = table.filter(rowid__in=[1, 4, 6], visited=False)
# r = table.filter(rowid__gte=3)
# r = table.filter(rowid__lte=3)
# r = table.filter(url__contains='/3')
# r = table.filter(url__startswith='http://')
# r = table.filter(url__endswith='/3')
# r = table.filter(rowid__range=[1, 3])
# r = table.filter(rowid__ne=1)
# r = table.filter(url__isnull=True)

# r['url'] = 'http://google.com/3'

# table.create(url='http://example.com')

# r = table.annotate(lowered_url=Lower('url'))
# r = table.annotate(uppered_url=Upper('url'))
# r = table.annotate(url_length=Length('url'))
# r = table.annotate(year=ExtractYear('created_on'))
# r = table.annotate(year=Max('rowid'))
# r = r.values()

# r = table.order_by('rowid')

# print(r)

# import time

# count = 1

# while True:
#     table.create(url=f'http://example.com/{count}')
#     count = count + 1
#     time.sleep(5)
#     print(table.all())


# from django.db.models import Model

# m = Model.objects.aget()
# m.asave()
