
import pytest

from lorelie.database.migrations.validation import JsonMigrationSchema, validate_date
from tests.database.migrations.utils import COMPLETE_MIGRATED_SCHEMA, EMPTY_MIGRATION


def test_empty_migration_validation():
    model = JsonMigrationSchema(**EMPTY_MIGRATION)
    assert model.id == EMPTY_MIGRATION['id']


def test_json_migration_with_table():
    model = JsonMigrationSchema(**COMPLETE_MIGRATED_SCHEMA)
    assert model.id == COMPLETE_MIGRATED_SCHEMA['id']


@pytest.fixture
def migrated_json():
    return JsonMigrationSchema(**COMPLETE_MIGRATED_SCHEMA)


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


def test_validate_date(migrated_json):
    result = validate_date(migrated_json.date)
    assert result is not None
