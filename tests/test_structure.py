import unittest

from lorelie import constraints, log_queries
from lorelie.database.base import Database
from lorelie.database.functions.text import Lower
from lorelie.database.indexes import Index
from lorelie.database.tables.base import Table
from lorelie.expressions import Q
from lorelie.fields.base import CharField, IntegerField


class TestStructure(unittest.TestCase):
    """This test evaluates the base functionnalities
    that are expected from the API which is create a
    value, get, filter and ... on an overall basis"""

    def test_global_structure(self):
        table = Table(
            'athletes',
            fields=[
                CharField('name'),
                IntegerField('age', min_value=14),
                IntegerField('height', min_value=150, default=165)
            ],
            constraints=[
                constraints.UniqueConstraint(
                    'unique_name',
                    fields=['name']
                )
            ],
            indexes=[
                Index(
                    'height',
                    fields=['height'],
                    condition=Q(height__gte=180)
                )
            ],
            str_field='name',
            ordering=['name']
        )
        db = Database(table)
        db.migrate()

        table.objects.create(name='Victoria Azarenka', age=35, height=182)

        qs = table.objects.all()
        self.assertGreater(qs.count(), 0)

        item = qs.first()
        self.assertEqual(item.name, 'Victoria Azarenka')

        qs = table.objects.annotate(lowered_name=Lower('name'))
        self.assertGreater(qs.count(), 0)

        qs = table.objects.filter(
            Q(name='Victoria Azarenka') |
            Q(name='Caitlyn Clark') &
            Q(age__gte=15) &
            Q(height__gte=165)
        )
        self.assertGreater(qs.count(), 0)

        # a = [
        #     "select rowid, name from sqlite_schema where type='table' and name not like 'sqlite_%';",
        #     "select rowid, name from sqlite_schema where type='table' and name not like 'sqlite_%';",
        #     "select rowid, name from sqlite_schema where type='table' and name not like 'sqlite_%';",
        #     "select rowid, name from sqlite_schema where type='table' and name not like 'sqlite_%';",
        #     'create table if not exists lorelie_migrations (name text not null unique, table_name text not null, migration text not null, applied datetime null, id integer primary key autoincrement not null);',
        #     "select rowid, name from sqlite_schema where type='table' and name not like 'sqlite_%';",
        #     'create table if not exists athletes (name text not null, age integer not null check(age>14), height integer default 165 not null check(height>150), id integer primary key autoincrement not null, unique(name));',
        #     "select type, name, tbl_name, sql from sqlite_master where type='index';",
        #     'begin; create index idx_height_6f9f360ee4 on athletes (height) where height>=180; commit;',
        #     "insert into athletes (name, age, height) values('Victoria Azarenka', 35, 182) returning name, age, height, id, rowid;",
        #     'select * from athletes order by name asc;',
        #     'select *, lower(name) as lowered_name from athletes order by name asc;',
        #     "select * from athletes where (name='Victoria Azarenka' or (name='Caitlyn Clark' and age>=15 and height>=165)) order by name asc;"
        # ]
