from sqlite3 import IntegrityError

from lorelie import fields
from lorelie.constraints import (CheckConstraint, MaxValueConstraint,
                                 MinValueConstraint, UniqueConstraint)
from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.expressions import F, Q
from lorelie.fields.base import CharField
from lorelie.test.testcases import LorelieTestCase


class TestCheckConstraint(LorelieTestCase):
    def test_structure(self):
        instance = CheckConstraint('some_name', Q(name='Kendall'))
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "check(name='Kendall')")

        instance = CheckConstraint('another_name', Q(age__gt=18))
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, 'check(age>18)')

    def test_constraint_with_F_expression(self):
        instance = CheckConstraint('test_name', Q(age__gt=F('minimum_age')))
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "check(age>'F(minimum_age)')")

    def test_constraint_with_incorrect_expression(self):
        with self.assertRaises(ValueError):
            CheckConstraint('bad_constraint', 'this is not a Q expression')

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
