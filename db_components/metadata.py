import os
from typing import List

from data_structures.hash_table import HashTable
from db_components.column import Column
from db_components.freeslot import FreeSlot
from db_components.index import TableIndex
from utils.errors import TableError
from utils.extra import format_size


class Metadata:
    def __init__(self, table_name="", metadata_file_path="", columns: HashTable | None = None):
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
        indexes = [index for index in self.indexes.items()]
        try:
            with open(self.metadata_file_path, "w") as f:
                f.write(f"Title:{self.table_name}\n")
                f.write(f"Total Columns:{self.columns.size}\n")
                f.write(f"Columns:\n")

                for _, column in self.columns.items():
                    f.write(f" {column}\n")

                f.write(f"Rows:{self.rows_count}\n")
                f.write(f"Free Slots:{','.join(str(s) for s in self.free_slots)}\n")  # TODO - recreate .join()?
                f.write(f"Table End:{self.table_end}\n")
                f.write(f"Offsets:{self.first_offset}|{self.last_offset}\n")

                f.write(f"Indexes:{len(indexes)}")  # TODO - recreate .join()?
                for _, ind in indexes:
                    f.write(f"\n{ind}")
        except Exception as e:
            raise TableError("Error with table!")

    @staticmethod
    def load_metadata(metadata_file: str):
        table_metadata = Metadata(metadata_file_path=metadata_file)

        with open(metadata_file) as f:
            lines = f.readlines()

        table_name = lines[0][:-1].split(":")[1]  # TODO - recreate .split()
        columns_count = int(lines[1].split(":")[1])  # TODO - recreate .split()

        columns = HashTable(size=columns_count)
        columns_last_index = 3 + columns_count

        for line in lines[3:columns_last_index]:
            parts = line.strip().split("|")  # TODO - recreate .split()
            col_name = parts[0]
            col_type = parts[1]

            constraints = HashTable()
            curr_index = 2
            while curr_index < len(parts):
                constraint_name, constraint_value = parts[curr_index].split(":")
                constraints[constraint_name] = constraint_value
                curr_index += 1
            columns[col_name] = Column(column_name=col_name, column_type=col_type, given_constraints=constraints)

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

        first_offset, last_offset = lines[columns_last_index].split(":")[1].split("|")   # TODO - recreate .split()
        columns_last_index += 1
        first_offset = int(first_offset)
        last_offset = int(last_offset)

        indexes = HashTable()
        total_indexes_count = int(lines[columns_last_index].split(":")[1])   # TODO - recreate .split()
        columns_last_index += 1

        for _ in range(total_indexes_count):
            index_info = lines[columns_last_index].split("|")   # TODO - recreate .split()
            column_name = index_info[0]
            index_name = index_info[1]
            index_path = index_info[2]
            pointer_list_data_path = index_info[3]
            column = columns[column_name]
            index = TableIndex(column=column,
                               index_name=index_name,
                               index_path=index_path,
                               pointer_list_data_path=pointer_list_data_path)
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
        result = (f"-----------------------------\n"
                  f"Total number of rows: {self.rows_count}.\n"
                  f"Metadata file size: {format_size(os.path.getsize(self.metadata_file_path))}.\n"
                  f"Data file size: {format_size(os.path.getsize(data_path))}.\n")

        cols = ""
        for _, col in self.columns.items():
            cols += f"{col.column_name}: {col.column_type}, {col.constraints}\n"
        result += f"Columns:\n{cols}\n"
        result += f"Indexes:{','.join(str(ind) for _, ind in self.indexes.items())}\n"
        result += "-----------------------------"
        return result
