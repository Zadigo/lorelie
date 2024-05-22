import unittest
from sqlite3 import IntegrityError

from lorelie.constraints import CheckConstraint
from lorelie.database.base import Database
from lorelie.expressions import Q
from lorelie.fields.base import CharField
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestConstraints(LorelieTestCase):
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
        db = Database()(table)
        table.prepare(db)

    def test_create_table_constraints(self):
        table = self.create_constrained_table()
        db = Database(table)
        db.migrate()

        with self.assertRaises(IntegrityError):
            db.objects.create('celebrities', name='Kendall')

        with self.assertRaises(IntegrityError):
            db.objects.create('celebrities', name='Aurélie', height=110)

    def test_create_table_unique_constraint(self):
        table = self.create_unique_constrained_table()
        db = Database(table)
        db.migrate()

        db.objects.create('celebrities', name='Kendall', height=175)

        with self.assertRaises(IntegrityError):
            db.objects.create('celebrities', name='Kendall', height=175)

        