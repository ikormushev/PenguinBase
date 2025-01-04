import os

from data_structures.hash_table import HashTable
from db_components.column import Column
from db_components.freeslot import FreeSlot
from db_components.index import TableIndex
from utils.errors import TableError
from utils.extra import format_size, polynomial_rolling_hash


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
        metadata_content = [
            f"Title:{self.table_name}\n",
            f"Total Columns:{len(self.columns)}\n",
            "Columns:\n"
        ]
        for _, column in self.columns.items():
            metadata_content.append(f"{column}\n")

        metadata_content.append(f"Rows:{self.rows_count}\n")
        metadata_content.append(f"Free Slots:{','.join(str(s) for s in self.free_slots)}\n")  # TODO - recreate .join()?
        metadata_content.append(f"Table End:{self.table_end}\n")
        metadata_content.append(f"Offsets:{self.first_offset}|{self.last_offset}\n")

        metadata_content.append(f"Indexes:{len(self.indexes)}")  # TODO - recreate .join()?
        for _, ind in self.indexes.items():
            metadata_content.append(f"\n{ind}")

        metadata_str = "".join(metadata_content)
        metadata_hash = polynomial_rolling_hash(metadata_str.encode())

        try:
            with open(self.metadata_file_path, "w") as f:
                f.write(f"Total Lines:{len(metadata_content)}\n")
                f.write(f"Hash:{metadata_hash}\n")
                f.write(metadata_str)
        except Exception as e:
            raise TableError("Error saving the metadata")

    @staticmethod
    def load_metadata(metadata_file: str):
        table_metadata = Metadata(metadata_file_path=metadata_file)

        with open(metadata_file) as f:
            lines = f.readlines()

        curr_index = 0
        total_lines = int(lines[curr_index].split(":")[1][:-1])
        if len(lines) != total_lines + 2:
            raise TableError("Table metadata file does not have the correct number of lines")
        curr_index += 1

        table_hash_number = int(lines[curr_index].split(":")[1][:-1])
        curr_index += 1
        metadata_str = "".join(lines[curr_index:])
        metadata_hash = polynomial_rolling_hash(metadata_str.encode())
        if table_hash_number != metadata_hash:
            raise TableError("Table metadata hash does not match")

        table_name = lines[curr_index][:-1].split(":")[1]  # TODO - recreate .split()
        curr_index += 1

        columns_count = int(lines[curr_index].split(":")[1])  # TODO - recreate .split()
        curr_index += 1

        columns = HashTable(size=columns_count)
        curr_index += 1
        columns_last_index = curr_index + columns_count

        for line in lines[curr_index:columns_last_index]:
            parts = line[:-1].split("|")  # TODO - recreate .split()
            col_name = parts[0]
            col_type = parts[1]

            constraints = HashTable()
            col_index = 2
            while col_index < len(parts):
                constraint_name, constraint_value = parts[col_index].split(":")   # TODO - recreate .split()
                constraints[constraint_name] = constraint_value
                col_index += 1
            columns[col_name] = Column(column_name=col_name, column_type=col_type, given_constraints=constraints)

        curr_index += columns_count

        rows_count = int(lines[curr_index].split(":")[1])   # TODO - recreate .split()
        curr_index += 1
        initial_free_slots = lines[curr_index][:-1].split(":")[1].split(",")  # TODO - recreate .split()
        curr_index += 1

        free_slots = []
        for free_slot in initial_free_slots:
            if free_slot:
                slot_pos, slot_len = free_slot.split("|")   # TODO - recreate .split()
                slot = FreeSlot(slot_position=int(slot_pos), slot_length=int(slot_len))
                free_slots.append(slot)

        table_end = int(lines[curr_index].split(":")[1])   # TODO - recreate .split()
        curr_index += 1

        first_offset, last_offset = lines[curr_index].split(":")[1].split("|")   # TODO - recreate .split()
        curr_index += 1
        first_offset = int(first_offset)
        last_offset = int(last_offset)

        indexes = HashTable()
        total_indexes_count = int(lines[curr_index].split(":")[1])   # TODO - recreate .split()
        curr_index += 1

        for _ in range(total_indexes_count):
            index_info = lines[curr_index].split("|")   # TODO - recreate .split()
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
            curr_index += 1

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
