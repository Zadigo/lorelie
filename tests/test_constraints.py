from sqlite3 import IntegrityError

import fields

from lorelie.constraints import (CheckConstraint, MaxValueConstraint,
                                 MinValueConstraint, UniqueConstraint)
from lorelie.database.base import Database
from lorelie.expressions import Q
from lorelie.fields.base import CharField
from lorelie.database.tables.base import Table
from lorelie.test.testcases import LorelieTestCase


class TestCheckConstraint(LorelieTestCase):
    def test_structure(self):
        instance = CheckConstraint('some_name', Q(name='Kendall'))
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "check(name='Kendall')")

    def test_table_level_constraints_creation(self):
        constraint = CheckConstraint('my_constraint', Q(name__eq='Kendall'))
        table = Table(
            'my_table',
            fields=[CharField('name')],
            constraints=[constraint]
        )
        db = Database(table)
        db.migrate()

    def test_create_table_constraints(self):
        table = self.create_constrained_table()
        db = Database(table)
        db.migrate()

        with self.assertRaises(IntegrityError):
            table.objects.create(name='Kendall')

        with self.assertRaises(IntegrityError):
            table.objects.create(name='Aurélie', height=110)


class TestUniqueConstraint(LorelieTestCase):
    def test_structure(self):
        constraint = UniqueConstraint('test_name', fields=['name'])
        result = constraint.as_sql(self.create_connection())
        self.assertEqual(result, 'unique(name)')

    def test_create_table_unique_constraint(self):
        table = self.create_unique_constrained_table()
        db = Database(table)
        db.migrate()

        table.objects.create(name='Kendall', height=175)

        with self.assertRaises(IntegrityError):
            table.objects.create(name='Kendall', height=175)


class TestMinValueConstraint(LorelieTestCase):
    def test_structure(self):
        field = fields.IntegerField('age')
        constraint = MinValueConstraint(14, field)
        result = constraint.as_sql(self.create_connection())
        self.assertEqual(result, 'check(age>14)')


class TestMaxValueConstraint(LorelieTestCase):
    def test_structure(self):
        field = fields.IntegerField('age')
        constraint = MaxValueConstraint(14, field)
        result = constraint.as_sql(self.create_connection())
        self.assertEqual(result, 'check(age<14)')
