import dataclasses
import functools
from collections import OrderedDict
from dataclasses import field

# TODO: Complete the TriggerMap which are kind of
# signals that get called when the database accomplishes
# a certain action e.g. pre save, post save, post delete etc


@dataclasses.dataclass
class TriggersMap:
    container: list = field(default_factory=list)
    # unlinked: list = field(default_factory=list)
    # pre_init: list = field(default_factory=list)
    # post_init: list = field(default_factory=list)
    # pre_save: list = field(default_factory=list)
    # post_save: list = field(default_factory=list)
    # pre_delete: list = field(default_factory=list)
    # post_delete: list = field(default_factory=list)

    def __hash__(self):
        return hash((len(self.container)))

    # @functools.lru_cache(maxsize=100)
    def list_functions(self, table, trigger_name):
        """Returns the list of functions for a given
        specific trigger name"""
        def get_values():
            for table_name, trigger, func in self.container:
                if table_name == table.name:
                    yield (trigger, func)
        return list(get_values())

    @functools.lru_cache(maxsize=100)
    def list_unlinked_functions(self):
        def get_values():
            for table_name, trigger, func in self.container:
                if table_name is None:
                    yield (trigger, func)
        return list(get_values())

    def run_named_triggers(self, name=None, table=None, **kwargs):
        funcs = self.list_unlinked_functions()
        if name is not None and table is not None:
            funcs.extend(self.list_functions(table, name))

        for trigger_name, func in funcs:
            if not callable(func):
                raise ValueError(
                    f"{func} should be a callable"
                )

            if trigger_name == name:
                func(table=table, **kwargs)


class MasterRegistry:
    """This class is responsible for storing and tracking 
    the core components of Lorelie, such as triggers and 
    databases. It serves as a central registry, providing 
    access to these elements for other parts of the 
    project"""

    current_database = None
    registered_triggers = TriggersMap()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.current_database}>"

    def register_database(self, database):
        from lorelie.database.base import Database

        if not isinstance(database, Database):
            raise ValueError(f"'{database}' should be an instance of database")
        self.current_database = database


registry = MasterRegistry()
