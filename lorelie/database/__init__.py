from collections import OrderedDict


class MasterRegistry:
    """This class is responsible for storing and tracking 
    the core components of Lorelie, such as tables and the database. 
    It serves as a central registry, providing access to these 
    elements for other parts of the project"""

    current_database = None
    known_tables = OrderedDict()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.current_database}>"

    def register_database(self, database):
        from lorelie.database.base import Database
        if not isinstance(database, Database):
            raise ValueError(f"{database} should be an instance of database")
        self.current_database = database
        for name, table in database.table_map.items():
            self.known_tables[name] = table


registry = MasterRegistry()
