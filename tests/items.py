import random


import dataclasses
from dataclasses import field


@dataclasses.dataclass
class Celebrity:
    name: str
    height: int = field(default_factory=lambda: random.randint(151, 200))
    # age: int = field(default_factory=lambda: random.randint(20, 70))


def create_random_celebrities(num_celebrities: int):
    for _ in range(num_celebrities):
        name = f'Celebrity_{random.randint(1, 1000)}'
        yield Celebrity(name=name)


def get_random_celebrity(num_celebrities: int = 100) -> Celebrity:
    return random.choice(list(create_random_celebrities(num_celebrities)))
