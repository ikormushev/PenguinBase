import os.path
from typing import List

from RANDOM_VALUES import get_table_columns, get_table_insert_rows
from data_structures.hash_table import HashTable
from db_components import table
from db_components.column import Column
from db_components.table import Table
from db_components.metadata import Metadata

from utils.errors import TableError
from data_structures.dynamic_queue import DynamicQueue

DB_DIRECTORY = "pbdb_files"


def load_metadata(table_name: str):
    file_path = os.path.join(DB_DIRECTORY, f"{name}.meta")

    if not os.path.exists(file_path):
        raise TableError(f"Meta data of table {table_name} not found!")

    table_metadata = Metadata.load_metadata(file_path)
    return table_metadata


# TODO - create index for the PK column
def create_table(table_name: str, columns: List[Column]):
    if not os.path.exists(DB_DIRECTORY):
        os.makedirs(DB_DIRECTORY)

    Table.create_table(DB_DIRECTORY, table_name, columns)

    print(f"Table '{table_name}' created successfully!")


def drop_table(table: Table):
    table.drop_table()


def table_info(table: Table):
    table.metadata.display_table_metadata(table.data_file_path)


def get_table_rows(table: Table, rows_nums: DynamicQueue):

    metadata_columns = table.metadata.columns
    result = ""

    for col in metadata_columns:
        result += f"  {col.column_name}: {col.column_type}  |"
    print(f"{result}")
    print("-" * len(result))

    for curr_row in table.get_rows(rows_nums):
        row_result = ""
        for col in metadata_columns:
            value = curr_row[col.column_name]
            row_result += f"  {value}  |"
        print(row_result)


def insert_into_table(table: Table, row_data: HashTable):
    table.insert(row_data)


def delete_table_rows(table: Table, rows_nums: DynamicQueue):
    table.delete_rows(rows_nums)


def defragment_table(table: Table):
    table.defragment()


def create_index(table: Table, index_name: str, column_name: str):
    table.create_new_index(index_name, column_name)


def drop_index(table: Table, index_name: str):

    table.drop_index(index_name)


def check_index(table: Table, index_name: str):
    table.check_index(index_name)


table_columns = get_table_columns()

table_insert_rows = get_table_insert_rows()

tables = HashTable()
valid_commands = ["CREATE TABLE", "EXIT", "INSERT INTO",
                  "TABLEINFO", "DROP TABLE", "GET ROW",
                  "DELETE FROM", "DEFRAGMENT",
                  "CREATE INDEX", "DROP INDEX", "CHECK INDEX"]

while True:
    command = input("Command name: ")
    if command not in valid_commands:
        print("Enter a valid command.")
        continue

    if command == "EXIT":
        break

    name = input("Name of table: ")

    if "CREATE TABLE" in command:
        create_table(name, table_columns)
        continue

    if tables.search(name) is None:
        tables[name] = Table(DB_DIRECTORY, name)
    table = tables[name]

    if "INSERT INTO" in command:
        for row in table_insert_rows:
            insert_into_table(table, row)
    elif "TABLEINFO" in command:
        table_info(table)
    elif "DROP TABLE" in command:
        drop_table(table)
    elif "GET ROW" in command:
        given_rows = input("Rows Numbers or 'ALL': ")
        new_rows = DynamicQueue()

        if given_rows == "ALL":
            metadata = load_metadata(name)
            for i in range(1, metadata.rows_count + 1):
                new_rows.enqueue(i)
        else:
            given_rows = given_rows.split(",")
            for r in given_rows:
                if r != "":
                    new_rows.enqueue(int(r))

        get_table_rows(table, new_rows)
    elif "DELETE FROM" in command:
        from_where = input("'ROW' or 'WHERE': ")

        if from_where == "ROW":
            new_rows = DynamicQueue()
            given_rows = input("Row Numbers or 'HALF': ")

            if given_rows == "HALF":
                metadata = load_metadata(name)
                for i in range(1, metadata.rows_count // 2):
                    new_rows.enqueue(i)
            else:
                given_rows = given_rows.split(",")
                for r in given_rows:
                    if r != "":
                        new_rows.enqueue(int(r))

            delete_table_rows(table, new_rows)
        elif from_where == "WHERE":
            ...
    elif "DEFRAGMENT" in command:
        defragment_table(table)
    elif "INDEX" in command:
        index_name = input("Index Name: ")
        if "CREATE" in command:
            column_name = input("Column Name: ")
            create_index(table, index_name, column_name)
        elif "DROP" in command:
            drop_index(table, index_name)
        elif "CHECK" in command:
            check_index(table, index_name)
