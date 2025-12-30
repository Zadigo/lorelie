from typing import ClassVar


class Transaction:
    innner_sql: ClassVar[str] = 'begin transaction {inner}'
    execution_map = []

    def begin(self):
        pass

    def add(self, func):
        pass

    def savepoint(self, name=None):
        pass

    def commit_savepoint(self, name=None):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


# transaction = Transaction()
# transaction.begin()
# transaction.add(table.objects.create)(name='Test Name')
# transaction.add(table.objects.create)(name='Another Name')
# s1 = transaction.savepoint()
# transaction.commit_savepoint(s1)
# transaction.rollback()
# transaction.commit()

# class Transaction:
#     innner_sql = 'begin transaction {inner}'
#     execution_map = []
#     memorized_savepoints = set()

#     def __init__(self):
#         from lorelie.database.tables.base import databases

#         self.sql_tokens = []
#         self.backend = None
#         self.committed = False
#         self.registered_databaes = databases

#     def __enter__(self):
#         return self

#     def __exit__(self, exc_type, exc_value, traceback):
#         connection = connections.get_last_connection()
#         # If the user did not call commit, then auto-commit
#         # the transaction regardless
#         if not self.committed:
#             sql = connection.simple_join(self.sql_tokens)

#         return False

#     def begin(self, table, *, database=None, **kwargs):
#         if isinstance(table, str):
#             if database is not None:
#                 database = self.registered_databaes[database]
#                 table = None

#         if not isinstance(table, Table):
#             raise ValueError('Table sould be an instance of Table')

#     def savepoint(self, name=None):
#         if name is None:
#             name = secrets.token_hex(nbytes=5)
#         self.memorized_savepoints.add(name)
#         statement = self.finalize_sql(f'savepoint {name}')
#         self.sql_tokens.append(statement)
#         return name

#     def commit_savepoint(self):
#         pass

#     def rollback_savepoint(self, name=None):
#         """Rollsback to the indicated savepoint if provided
#         otherwise rollsback to the last one"""
#         if name is None:
#             name = self.memorized_savepoints[-1]
#         else:
#             previous_savepoints = list(self.memorized_savepoints)
#             name = previous_savepoints[previous_savepoints.index(name)]
#         statement = f'rollback to {name}'
#         self.sql_tokens.append(self.finalize_sql(statement))

#     def commit(self):
#         statement = self.finalize_sql('commit')
#         self.sql_tokens.append(statement)
#         self.committed = True

#     def rollback(self):
#         statement = self.finalize_sql('rollback')
#         self.sql_tokens.append(statement)


# transaction = Transaction()
