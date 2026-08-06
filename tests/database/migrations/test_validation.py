import pytest

from lorelie.database.migrations.validation import JsonMigrationSchema

EMPTY_MIGRATION = {
    'id': '79f47320e4',
    'date': '2025-12-31 22:34:28.716799',
    'number': 1,
    'migrated': False,
    'schema': {}
}


def test_empty_migration_validation():
    model = JsonMigrationSchema(**EMPTY_MIGRATION)
    assert model.id == EMPTY_MIGRATION['id']


MIGRATION_WITH_SCHEMA = {
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
                            "null": False,
                            "primary_key": False,
                            "default": None,
                            "unique": False,
                            "editable": False,
                            "max_length": 5
                        }
                    ],
                    [
                        "AutoField",
                        "id",
                        {
                            "null": False,
                            "primary_key": True,
                            "default": None,
                            "unique": False,
                            "editable": False
                        }
                    ]
                ],
                "indexes": [],
                "constraints": [],
                "ordering": [],
                "str_field": "id"
            }
        ]
    }
}

def test_json_migration_with_table():
    model = JsonMigrationSchema(**MIGRATION_WITH_SCHEMA)
    assert model.id == MIGRATION_WITH_SCHEMA['id']


@pytest.fixture
def migrated_json():
    return JsonMigrationSchema(**MIGRATION_WITH_SCHEMA)


def test_get_table_names(migrated_json):
    result = migrated_json._table_names
    assert isinstance(result, set)
    assert 'company' in result


@pytest.mark.parametrize(
    'name',
    [
        'company',
        None
    ]
)
def test_get_table(migrated_json, name):
    result = migrated_json.get_table(name)
    if name is None:
        assert result is None
    else:
        assert result is not None
        assert result.name == name


def test_get_table_fields(migrated_json):
    result = migrated_json.get_table_fields('company')
    assert result is not None
    assert isinstance(result, list)

    for item in result:
        assert isinstance(item, list)


def test_get_table_field(migrated_json):
    result = migrated_json.get_table_field('company', 'name')
    assert result is not None
    assert isinstance(result, tuple)
    assert result[1] == 'name'
