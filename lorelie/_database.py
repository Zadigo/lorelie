# import pathlib

# from lorelie.conf import settings
# from lorelie.fields import CharField
# from lorelie.tables import Database, Table

# setattr(settings, 'PROJECT_PATH', pathlib.Path('.'))

# table1 = Table('my_table', fields=[
#     CharField('name')
# ])

# table2 = Table('other_table', fields=[
#     CharField('country')
# ])

# db = Database('my_database', table1, table2)
# # db.make_migrations()
# # db.migrate()
# # table1.create(name='Kendall')
# v = table1.first()
# print(vars(v))
