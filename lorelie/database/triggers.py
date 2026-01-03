from collections import defaultdict
from functools import wraps, partial
from typing import Callable, DefaultDict, Optional
from warnings import deprecated

from lorelie.constants import DatabaseEvent, PythonEvent
from lorelie.lorelie_typings import TypeDatabase, TypeTable


TypeTrigerCallable = Callable[[], None]

TypeTriggerPythonDict = DefaultDict[
    PythonEvent,
    list[tuple[TypeTrigerCallable, TypeTable, str]]
]


@deprecated('Not yet implemented')
class DatabaseTrigger:
    """Database triggers are created directly in the database
    and are not managed by the Lorelie ORM.
    """

    template_sql = 'create trigger if not exists {name} {event} on {table} begin {condition} end'

    def __init__(self, name: str, event: DatabaseEvent):
        pass

    def as_sql(self) -> str:
        raise NotImplementedError()


class PythonTrigger:
    """Python triggers are managed by the Lorelie ORM
    and are not created directly in the database. They
    execute pure Python code when certain events occur.
    """

    container: TypeTriggerPythonDict = defaultdict(list)

    def __iter__(self):
        for _, values in self.container.items():
            yield values

    def __len__(self):
        return len(self.container)

    def create(self, event: PythonEvent, func: TypeTrigerCallable, table: str | TypeTable, name: str | None = None):
        trigger_name: str = name if name is not None else func.__name__
        events = self.container[event]
        events.append((func, table, trigger_name))

    def run(self, event: PythonEvent, trigger_element: TypeTable | TypeDatabase):
        """Run all registered Python triggers for a given event."""
        from lorelie.database.base import Database
        from lorelie.database.tables.base import Table

        items = self.container[event]

        for func, table, name in items:
            if isinstance(trigger_element, Database):
                continue

            if isinstance(trigger_element, Table):
                if table == trigger_element:
                    func()


class TriggerManager:
    def __init__(self):
        print('Initializing TriggerManager')
        self.python_events = PythonTrigger()

    def register_python(self, event: PythonEvent, table: str | TypeTable, *, name: Optional[str] = None):
        def wrapper(func: TypeTrigerCallable):
            trigger_name = name if name is not None else func.__name__

            @wraps(func)
            def inner(*args, **kwargs):
                func(*args, **kwargs)

            self.python_events.create(event, func, table, trigger_name)
            return partial(inner, table=table)

        return wrapper


trigger = TriggerManager()
