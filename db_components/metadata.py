import os
from typing import List

from data_structures.hash_table import HashTable
from db_components.column import Column
from db_components.freeslot import FreeSlot
from db_components.index import TableIndex
from utils.extra import format_size


class Metadata:
    def __init__(self, table_name="", metadata_file_path="", columns: List[Column] | None = None):
        self.table_name = table_name
        self.metadata_file_path = metadata_file_path
        self.columns = columns

        self.rows_count = 0
        self.free_slots = []
        self.table_end = 0
        self.first_offset = -1
        self.last_offset = -1

        self.indexes = HashTable()

    def save_metadata(self):
        try:
            with open(self.metadata_file_path, "w") as f:
                f.write(f"Title:{self.table_name}\n")
                f.write(f"Total Columns:{len(self.columns)}\n")
                f.write(f"Columns:\n")

                for column in self.columns:
                    result = f" {column.column_name}|{column.column_type}"
                    if column.default_value:
                        result += f'|default:"{column.default_value}"'

                    if column.is_primary_key:
                        result += f"|primary_key"
                    f.write(f"{result}\n")
                f.write(f"Rows:{self.rows_count}\n")
                f.write(f"Free Slots:{','.join(str(s) for s in self.free_slots)}\n")  # TODO - recreate .join()?
                f.write(f"Table End:{self.table_end}\n")
                f.write(f"Offsets:{self.first_offset}|{self.last_offset}\n")

                f.write(f"Indexes:{','.join(str(ind) for col, ind in self.indexes.items())}\n")  # TODO - recreate .join()?
        except Exception as e:
            print(e)

    @staticmethod
    def load_metadata(metadata_file: str):
        table_metadata = Metadata(metadata_file_path=metadata_file)

        with open(metadata_file) as f:
            lines = f.readlines()

        table_name = lines[0][:-1].split(":")[1]  # TODO - recreate .split()
        columns_count = int(lines[1].split(":")[1])  # TODO - recreate .split()

        columns = []
        columns_last_index = 3 + columns_count

        for line in lines[3:columns_last_index]:
            parts = line.strip().split("|")  # TODO - recreate .split()
            col_name = parts[0]
            col_type = parts[1]
            default_value = None
            is_primary_key = False

            if len(parts) > 2:
                default_value = parts[2].split("default:")[0]

                if len(parts) > 3:
                    is_primary_key = True

            columns.append(Column(col_name, col_type, default_value, is_primary_key))

        rows_count = int(lines[columns_last_index].split(":")[1])
        columns_last_index += 1
        initial_free_slots = lines[columns_last_index][:-1].split(":")[1].split(",")  # TODO - recreate .split()
        columns_last_index += 1

        free_slots = []
        for free_slot in initial_free_slots:
            if free_slot:
                slot_pos, slot_len = free_slot.split("|")   # TODO - recreate .split()
                slot = FreeSlot(slot_position=int(slot_pos), slot_length=int(slot_len))
                free_slots.append(slot)

        table_end = int(lines[columns_last_index].split(":")[1])   # TODO - recreate .split()
        columns_last_index += 1

        first_offset, last_offset = lines[columns_last_index].split(":")[1].split("|")
        columns_last_index += 1
        first_offset = int(first_offset)
        last_offset = int(last_offset)

        initial_indexes = lines[columns_last_index][:-1].split(":")[1].split(",")  # TODO - recreate .split()
        indexes = HashTable()
        for index in initial_indexes:
            if index:
                column_name, index_name, index_path = index.split("|")
                for column in columns:
                    if column.column_name == column_name:
                        index = TableIndex(column=column, index_name=index_name, index_path=index_path)
                        indexes[column_name] = index

        table_metadata.table_name = table_name
        table_metadata.columns = columns
        table_metadata.rows_count = rows_count
        table_metadata.free_slots = free_slots
        table_metadata.table_end = table_end
        table_metadata.first_offset = first_offset
        table_metadata.last_offset = last_offset
        table_metadata.indexes = indexes
        return table_metadata

    def display_table_metadata(self, data_path: str):
        print("-----------------------------")
        print(f"Total number of rows: {self.rows_count}.")
        print(f"Metadata file size: {format_size(os.path.getsize(self.metadata_file_path))}")
        print(f"Data file size: {format_size(os.path.getsize(data_path))}")

        result = ""
        for col in self.columns:
            result += f"{col.column_name}: {col.column_type}\n"
        print(f"Columns: \n{result}", end="")
        print(f"Indexes:{','.join(str(ind) for _, ind in self.indexes.items())}\n")
        print("-----------------------------")
