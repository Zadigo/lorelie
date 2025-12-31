# from contextvars import ContextVar
# from contextlib import asynccontextmanager
# import pydantic
# from fastapi import FastAPI, Request

# from lorelie.database.base import Database
# from lorelie.database.tables.base import Table
# from lorelie.fields.base import CharField

# database: ContextVar[Database] = ContextVar('database')
# table: ContextVar[Table] = ContextVar('table')

# app = FastAPI()

# fields = [CharField('name', max_length=100)]

import pathlib


from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField, IntegerField

fields = [
    CharField('name', max_length=5),
    IntegerField('employees', min_value=1)
]
tb = Table('company', fields=fields)
db = Database(tb, name='companies', path=pathlib.Path('.'))
db.migrate()

print(tb.objects.create(name='Test Company 1'))


# @asynccontextmanager
# async def initialize_app(_app: FastAPI):
#     # instance = await db
#     # instance.migrate()

#     # db = Database(tb, name='companies')
#     # db.migrate()

#     yield


# @app.middleware('http')
# async def add_user_info(request: Request, call_next):
#     # request.state.database = database.get()
#     # request.state.table = table.get()

#     response = await call_next(request)
#     return response


# class CompanyModel(pydantic.BaseModel):
#     name: str


# @app.post('/create')
# async def create_item(request: Request, company: CompanyModel):
#     # tb = database.get('tb')
#     # print(tb)
#     # row = request.state.table.objects.create(**company.model_dump_json())

#     tb = Table('company', fields=fields)
#     db = Database(tb, name='companies')

#     row = tb.objects.create(name=company.name)
#     return row.id, 200
#     # return {}, 200
