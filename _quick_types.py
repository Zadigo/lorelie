import enum
from ast import TypeVarTuple
from typing import Callable, Generic, NamedTuple, NotRequired, ReadOnly, TypeVar, Final, ParamSpec, TypedDict, TypeAlias, Unpack


T = TypeVar('T', bound=str)  # Only allow str types
E = TypeVar('E')  # Allow any type
R = TypeVar('R', int, float)  # Only allow int and float types


def google(name: T) -> T:
    return name


def something(value: E) -> E:
    return value


def another(value: R) -> R:
    return value


a = google('example')  # This should be valid
b = google(123)        # This should raise a type checker error


c = something('example')  # This should be valid
d = something(123)        # This should also be valid

e = another(3.14)        # This should be valid


class EmptyClass:
    # Indicates that 'value' is a constant and should not be reassigned
    value: Final[int] = 42


u = EmptyClass()
u.value = 100  # This should raise a type checker error

PP = ParamSpec('P')  # No type arguments
RR = TypeVar('R')


def decorator(func: Callable[PP, RR]) -> Callable[PP, RR]:
    def wrapper(*args: PP.args, **kwargs: PP.kwargs) -> RR:
        print("Before call")
        result = func(*args, **kwargs)  # Forward all params
        print("After call")
        return result
    return wrapper


@decorator
def example(name: str, age: int, city: str = "NYC") -> str:
    return f"{name}, {age}, {city}"


rr = example('Alice', 30, city='LA')  # This should be valid


class ConfigDict(TypedDict):
    host: str
    port: int
    use_ssl: NotRequired[bool]
    sortable: ReadOnly[bool]


m: ConfigDict = {
    'host': 'localhost',
    'port': 8080,
    'use_ssl': True
}  # This should be valid

m['use_ssl'] = False  # This should be valid


e: TypeAlias = dict[str, int]  # This should be valid


TypeTuple = TypeVarTuple('TypeTuple')  # This should be valid


class MovieDict(TypedDict):
    title: str
    year: int
    rating: float


class Movie:
    def __init__(self, **data: Unpack[MovieDict]) -> None:
        rating = data['rating']
        title = data['title']


N = TypeVar('N', int, float)  # Bounded type variable


class QuickNumber(Generic[N]):  # Generic class with bounded type variable
    def __init__(self, item: N) -> None:
        self.item = item

    def double(self, number: N) -> N:
        return self.item * number


q = QuickNumber(10)      # Type: QuickNumber[int]
z = q.double(5)          # Type: int, value: 50

q2 = QuickNumber(3.14)   # Type: QuickNumber[float]
z2 = q2.double(2.0)      # Type: float, value: 6.28


class Ty(NamedTuple):
    name: str
    value: int


ty_instance = Ty(name='example', value=42)


class Color(enum.Enum):
    RED = 'red'
    GREEN = 'green'
    BLUE = 'blue'


GGA = tuple[str, Color]


def facebook(a: GGA):
    c, d = a
    d.value
