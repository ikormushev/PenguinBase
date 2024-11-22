import os.path

from RANDOM_VALUES import get_table_columns, get_table_insert_rows
from db_components.table import Table
from db_components.metadata import Metadata

from utils.errors import TableDoesNotExistError
from data_structures.dynamic_queue import DynamicQueue

DB_DIRECTORY = "pbdb_files"


def load_metadata(table_name: str):
    file_path = os.path.join(DB_DIRECTORY, f"{name}.meta")

    if not os.path.exists(file_path):
        raise TableDoesNotExistError(f"Meta data of table {table_name} not found!")

    table_metadata = Metadata.load_metadata(file_path)
    return table_metadata


# TODO - create index for the PK column
def create_table(table_name, columns):
    if not os.path.exists(DB_DIRECTORY):
        os.makedirs(DB_DIRECTORY)

    metadata_file = os.path.join(DB_DIRECTORY, f"{table_name}.meta")
    data_file = os.path.join(DB_DIRECTORY, f"{table_name}.data")
    if os.path.exists(metadata_file) or os.path.exists(data_file):
        print(f"Error: Table '{table_name}' already exists.")
        return

    metadata = Metadata(table_name=table_name, columns=columns, metadata_file_path=metadata_file)
    metadata.save_metadata()

    open(data_file, "w").close()

    print(f"Table '{table_name}' created successfully!")


def drop_table(table_name):
    meta_file = os.path.join(DB_DIRECTORY, f"{table_name}.meta")
    data_file = os.path.join(DB_DIRECTORY, f"{table_name}.data")

    try:
        os.remove(meta_file)
        print(f"Deleted: {meta_file}")
    except FileNotFoundError:
        print(f"Warning: Metadata file '{meta_file}' not found.")
        return

    try:
        os.remove(data_file)
        print(f"Deleted: {data_file}")
    except FileNotFoundError:
        print(f"Warning: Data file '{data_file}' not found.")
        return

    print(f"Table '{table_name}' dropped successfully!")


def table_info(table_name: str):
    metadata = load_metadata(table_name)
    data_path = os.path.join(DB_DIRECTORY, f"{table_name}.data")
    metadata.display_table_metadata(data_path)


def get_table_rows(table_name, rows):
    new_table = Table(data_file_path=os.path.join(DB_DIRECTORY, f"{table_name}.data"),
                      metadata_file_path=os.path.join(DB_DIRECTORY, f"{table_name}.meta"))

    metadata_columns = new_table.metadata.columns
    result = ""

    for col in metadata_columns:
        result += f"  {col.column_name}: {col.column_type}  |"
    print(f"{result}")
    print("-" * len(result))

    for row in new_table.get_rows(rows):
        row_result = ""
        for col in metadata.columns:
            value = row[col.column_name]
            row_result += f"  {value}  |"
        print(row_result)


def insert_into_table(table_name, row_data):
    new_table = Table(data_file_path=os.path.join(DB_DIRECTORY, f"{table_name}.data"),
                      metadata_file_path=os.path.join(DB_DIRECTORY, f"{table_name}.meta"))
    new_table.insert(row_data)


def delete_table_rows(table_name, rows):
    new_table = Table(data_file_path=os.path.join(DB_DIRECTORY, f"{table_name}.data"),
                      metadata_file_path=os.path.join(DB_DIRECTORY, f"{table_name}.meta"))
    new_table.delete_rows(rows)


table_columns = get_table_columns()

table_insert_rows = get_table_insert_rows()


while True:
    command = input("Command name: ")
    if command == "EXIT":
        break

    name = input("Name of table: ")

    if "CREATE TABLE" in command:
        create_table(name, table_columns)
    elif "INSERT INTO" in command:
        for row in table_insert_rows:
            insert_into_table(name, row)
    elif "TABLEINFO" in command:
        table_info(name)
    elif "DROP TABLE" in command:
        drop_table(name)
    elif "GET ROW" in command:
        given_rows = input("Rows Numbers or 'ALL': ")
        if given_rows == "ALL":
            metadata = load_metadata(name)
            new_rows = DynamicQueue()
            for i in range(1, metadata.rows_count + 1):
                new_rows.enqueue(i)
            get_table_rows(name, new_rows)
        else:
            given_rows = given_rows.split(",")
            new_rows = DynamicQueue()

            for r in given_rows:
                new_rows.enqueue(int(r))

            get_table_rows(name, new_rows)
    elif "DELETE FROM" in command:
        from_where = input("'ROW' or 'WHERE': ")
        if from_where == "ROW":
            given_rows = input("Row Numbers: ")
            given_rows = given_rows.split(",")
            new_rows = DynamicQueue()

            for r in given_rows:
                if r != "":
                    new_rows.enqueue(int(r))
            delete_table_rows(name, new_rows)
        elif from_where == "WHERE":
            ...
    else:
        print("Enter a valid command.")
