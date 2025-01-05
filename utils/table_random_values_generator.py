import random
from typing import List

from data_structures.hash_table import HashTable
from db_components.column import Column
from utils.date import Date

SMALL_ASCII = 'abcdefghijklmnopqrstuvwxyz'
BIG_ASCII = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
LETTERS = SMALL_ASCII + BIG_ASCII


def generate_random_date():
    return Date.generate_random_date_string()


def generate_random_number():
    if random.choice([True, False]):
        return random.randint(0, 10_000)
    else:
        return random.uniform(0, 1000)


def generate_random_string(max_size: int):
    size = random.randint(1, max_size)

    result = []
    for _ in range(size):
        result.append(random.choice(LETTERS))

    return ''.join(result)


def generate_random_rows(columns: List[Column], count: int):
    generate_methods = HashTable([
        ("date", generate_random_date),
        ("number", generate_random_number),
    ])

    for _ in range(count):
        row = HashTable(size=len(columns))

        for column in columns:
            if column.column_type == "string":
                row[column.column_name] = generate_random_string(column.MAX_SIZE)
            else:
                row[column.column_name] = generate_methods[column.column_type]()

        yield row
