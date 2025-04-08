from lorelie.database import registry, triggers
from lorelie.database.base import Database
from lorelie.database.manager import DatabaseManager
from lorelie.fields.base import CharField
from lorelie.database.tables.base import Table


# TODO: The user should be able to register a custom manager

class CustomManager(DatabaseManager):
    def test_trigger(self, table):
        # TEST: This is a test function for testing
        # trigger functions on the database
        selected_table = self.before_action(table)
        registry.registered_triggers.run_named_triggers(
            name='pre_save',
            table=selected_table
        )


table = Table('names', fields=[CharField('fullname')])
db = Database(table)
db.migrate()


@db.register_trigger(triggers.PRE_SAVE, table=table)
def test_trigger(table, **kwargs):
    print(table)


db.objects.create('names', fullname='Kendall Jenner')
db.objects.test_trigger('names')
