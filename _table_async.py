import asyncio

from lorelie.fields import base
from lorelie.database.base import Database
from lorelie.tables import Table


async def create_data(db, firstname, lastname):
    db.objects.create(
        'artists',
        firstname=firstname,
        lastname=lastname
    )
    await asyncio.sleep(5)


async def get_data(db):
    queryset = db.objects.all('artists')
    print(queryset)
    await asyncio.sleep(15)


async def main():
    table = Table('artists', fields=[
        base.CharField('firstname', null=True),
        base.CharField('lastname', null=True),
        base.DateTimeField('created_on', auto_add=True)
    ])
    db = Database(table, name='artists')
    db.migrate()

    while True:
        await create_data(db, 'Kendall', 'Jenner')
        await get_data(db)

if __name__ == '__main__':
    asyncio.run(main())
