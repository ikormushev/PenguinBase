import os
from typing import List

from db_components.column import Column
from db_components.freeslot import FreeSlot
from utils.extra import format_size


class Metadata:
    def __init__(self, table_name="", metadata_file_path="", columns: List[Column] | None = None,
                 rows_count=0, free_slots: List[FreeSlot] | None = None,
                 table_end=0, first_offset=-1, last_offset=-1):
        self.table_name = table_name
        self.metadata_file_path = metadata_file_path
        self.columns = columns

        self.rows_count = rows_count
        self.free_slots = free_slots if free_slots is not None else []
        self.table_end = table_end
        self.first_offset = first_offset
        self.last_offset = last_offset

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
        initial_free_slots = lines[columns_last_index + 1][:-1].split(":")[1].split(",")  # TODO - recreate .split()

        free_slots = []
        for free_slot in initial_free_slots:
            if free_slot:
                slot_pos, slot_len = free_slot.split("|")   # TODO - recreate .split()
                slot = FreeSlot(slot_position=int(slot_pos), slot_length=int(slot_len))
                free_slots.append(slot)

        table_end = int(lines[columns_last_index + 2].split(":")[1])   # TODO - recreate .split()
        first_offset, last_offset = lines[columns_last_index + 3].split(":")[1].split("|")
        first_offset = int(first_offset)
        last_offset = int(last_offset)

        table_metadata.table_name = table_name
        table_metadata.columns = columns
        table_metadata.rows_count = rows_count
        table_metadata.free_slots = free_slots
        table_metadata.table_end = table_end
        table_metadata.first_offset = first_offset
        table_metadata.last_offset = last_offset
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
        print("-----------------------------")
