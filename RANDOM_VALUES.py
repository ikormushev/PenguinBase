import random

from data_structures.hash_table import HashTable
from db_components.column import Column

small_ascii = 'abcdefghijklmnopqrstuvwxyz'
big_ascii = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
email_domains = ["abv.bg", "gmail.com", "outlook.com"]


def get_table_columns():
    return [Column("id", "number", is_primary_key=True),
            Column("name", "string", default_value="No Name", max_value=50),
            Column("email", "string"),
            Column("created_at", "date")]


def get_table_insert_rows():
    table_insert_rows = []

    for i in range(1, 1001):
        row = HashTable(size=4)
        random_name_length = random.randint(1, 40)
        random_email_length = random.randint(1, 5)

        row["id"] = i
        row["name"] = (random.choice(big_ascii) +
                       "".join([random.choice(small_ascii)
                                for _ in range(random_name_length)]))
        row["email"] = "".join([random.choice(small_ascii)
                                for _ in range(random_email_length)]) + "@" + random.choice(email_domains)
        row["created_at"] = f"{random.randint(1, 31):02}.{random.randint(1, 12):02}.{random.randint(1900, 2100)}"
        table_insert_rows.append(row)
    return table_insert_rows
