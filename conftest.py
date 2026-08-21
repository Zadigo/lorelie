from unittest.mock import MagicMock

import pytest

from lorelie.backends import SQLiteBackend
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.expressions import Q
from lorelie.fields.base import (
    CharField,
    DateTimeField,
    IntegerField,
)


@pytest.fixture
def table():
    fields = [
        CharField('name'),
        IntegerField('height', min_value=150, default=152),
        DateTimeField('updated_on', auto_update=True),
        DateTimeField('created_on', auto_add=True)
    ]

    return Table('celebrities', fields=fields)


@pytest.fixture
def constrained_table():
    table = Table(
        'celebrities',
        fields=[
            CharField('name', null=True),
            IntegerField('height', null=True)
        ],
        constraints=[
            CheckConstraint('no_kendall', Q(name__ne='Kendall')),
            CheckConstraint('height_over_150', Q(height__gte=150))
        ]
    )
    return table


@pytest.fixture
def unique_constrained_table():
    table = Table(
        'celebrities',
        fields=[
            CharField('name', null=True),
            IntegerField('height', null=True)
        ],
        constraints=[
            UniqueConstraint('my_constraint', fields=['name', 'height'])
        ]
    )
    return table


@pytest.fixture
def database():
    db = Database(table, log_queries=False)
    db.migrate()

    return db


@pytest.fixture
def connection():
    return SQLiteBackend()


@pytest.fixture
def mocked_table():
    return MagicMock(spec=Table)


@pytest.fixture
def mocked_connection():
    return MagicMock(spec=SQLiteBackend)


@pytest.fixture
def empty_migration():
    return {
        "id": "79f47320e4",
        "date": "2025-12-31 22:34:28.716799",
        "number": 1,
        "migrated": False,
        "schema": {}
    }


@pytest.fixture
def full_migration():
    return {
    "id": "79f47320e4",
    "date": "2025-12-31 22:34:28.716799",
    "number": 1,
    "migrated": True,
    "schema": {
        "name": "companies",
        "tables": [
            {
                "name": "company",
                "fields": [
                    [
                        "CharField",
                        "name",
                        {
                            "null": False ,
                            "primary_key": False ,
                            "default": None,
                            "unique": False ,
                            "editable": False ,
                            "max_length": 5
                        }
                    ],
                    [
                        "AutoField",
                        "id",
                        {
                            "null": False ,
                            "primary_key": True,
                            "default": None,
                            "unique": False ,
                            "editable": False 
                        }
                    ]
                ],
                "indexes": [],
                "constraints": [],
                "ordering": [],
                "str_field": "id"
            },
            {
                "name": "migrations",
                "fields": [
                    [
                        "CharField",
                        "name",
                        {
                            "null": False ,
                            "primary_key": False ,
                            "default": None,
                            "unique": True,
                            "editable": False 
                        }
                    ],
                    [
                        "CharField",
                        "db_name",
                        {
                            "null": False ,
                            "primary_key": False ,
                            "default": None,
                            "unique": False ,
                            "editable": False 
                        }
                    ],
                    [
                        "JSONField",
                        "migration",
                        {
                            "null": False ,
                            "primary_key": False ,
                            "default": None,
                            "unique": False ,
                            "editable": False 
                        }
                    ],
                    [
                        "DateTimeField",
                        "applied",
                        {
                            "null": True,
                            "primary_key": False ,
                            "default": None,
                            "unique": False ,
                            "editable": False 
                        }
                    ],
                    [
                        "AutoField",
                        "id",
                        {
                            "null": False ,
                            "primary_key": True,
                            "default": None,
                            "unique": False ,
                            "editable": False 
                        }
                    ]
                ],
                "indexes": [],
                "constraints": [],
                "ordering": [],
                "str_field": "name"
            }
        ]
    }
}
