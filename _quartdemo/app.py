from contextvars import ContextVar
from contextlib import asynccontextmanager
import pydantic
from fastapi import FastAPI, Request

from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField

database: ContextVar[Database] = ContextVar('database')
table: ContextVar[Table] = ContextVar('table')

app = FastAPI()

fields = [CharField('name', max_length=10)]

tb = Table('company', fields=fields)
db = Database(tb, name='companies')

db.migrate()


@asynccontextmanager
async def initialize_app(_app: FastAPI):
    print('Database migrated')
    yield


@app.middleware('http')
async def add_user_info(request: Request, call_next):
    # request.state.database = database.get()
    # request.state.table = table.get()

    response = await call_next(request)
    return response


class CompanyModel(pydantic.BaseModel):
    name: str


@app.post('/create')
async def create_item(request: Request, company: CompanyModel):
    # tb = database.get('tb')
    # print(tb)
    # row = request.state.table.objects.create(**company.model_dump_json())
    # company = tb.objects.create(**company.model_dump())
    company = await tb.objects.acreate(**company.model_dump())
    return await tb.objects.avalues('id', 'name'), 200
