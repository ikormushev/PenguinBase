import os
import struct
from typing import List

from data_structures.hash_table import HashTable
from utils.date import Date
from utils.errors import TableError
from utils.extra import reverse_array, polynomial_rolling_hash


class MergeSortHandler:
    def __init__(self, directory: str, table_name: str, order_by_col: str | None = None,
                 distinct_cols: HashTable | None = None, order: str = "ASC", chunk_size: int = 1000):
        self.directory = directory
        self.table_name = table_name
        self.order_by_col = order_by_col
        self.distinct_cols = distinct_cols
        self.order = order
        self.chunk_size = chunk_size

    def select_merge_sort(self, table_rows) -> str:
        chunk_files = []
        in_memory_chunk = []
        count = 0

        for row in table_rows:
            in_memory_chunk.append(row)
            count += 1
            if count >= self.chunk_size:
                tmp_path = self.write_sorted_chunk(in_memory_chunk, len(chunk_files) + 1)
                chunk_files.append(tmp_path)
                in_memory_chunk = []
                count = 0

        if in_memory_chunk:
            tmp_path = self.write_sorted_chunk(in_memory_chunk, len(chunk_files) + 1)
            chunk_files.append(tmp_path)

        if self.distinct_cols:
            final_file = self.multiway_merge_distinct(chunk_files)
        else:
            final_file = self.multiway_merge_no_distinct(chunk_files)

        for temp_path in chunk_files:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        return final_file

    def key_func(self, row: HashTable) -> tuple:
        """
            Key_parts is a composite key for a specific situation:
            1) order_by_col ->  primary key
            2) distinct_cols -> primary key
            3) both -> order_by_col as primary, distinct_cols for tie-break
        """

        key_parts = []

        if self.order_by_col is not None:
            key_parts.append(row[self.order_by_col])

        if self.distinct_cols:
            for dc_name, _ in self.distinct_cols.items():
                if dc_name != self.order_by_col:
                    key_parts.append(row[dc_name])

        return tuple(key_parts)

    def write_sorted_chunk(self, rows: List[HashTable], chunk_file_num: int) -> str:
        """
            Sort the rows in memory and write them to a temp file.
        """
        sorted_rows = self.mergesort_in_memory(rows)
        if self.order == "DESC":
            sorted_rows = reverse_array(sorted_rows)

        tmp_path = os.path.join(self.directory, f"{self.table_name}_chunk_{id(self)}_{chunk_file_num}.temp")

        with open(tmp_path, "wb") as f:
            for row in sorted_rows:
                self.write_row(f, row)

        return tmp_path

    def multiway_merge_distinct(self, chunk_files: List[str]) -> str:
        """
            Merge chunk_files into one final sorted file, skipping duplicates.
        """
        file_handles = [open(cf, "rb") for cf in chunk_files]
        buffers = [self.read_next_row(fh) for fh in file_handles]

        final_path = os.path.join(self.directory, f"{self.table_name}_merge_sort.temp")
        open(final_path, 'wb').close()

        last_distinct_key = None

        with open(final_path, "wb") as out_f:
            while True:
                chosen_index = -1
                chosen_row = None

                for i in range(len(buffers)):
                    row = buffers[i]

                    if row is None:
                        continue

                    if chosen_row is None:
                        chosen_index = i
                        chosen_row = row
                    else:
                        if self.compare_rows(row, chosen_row) < 0:
                            chosen_index = i
                            chosen_row = row

                if chosen_index == -1:
                    break

                # Skip duplicates
                current_dkey = tuple(chosen_row[dc_name] for dc_name, _ in self.distinct_cols.items())
                if current_dkey != last_distinct_key:
                    self.write_row(out_f, chosen_row)
                    last_distinct_key = current_dkey

                buffers[chosen_index] = self.read_next_row(file_handles[chosen_index])

        for fh in file_handles:
            fh.close()

        return final_path

    def multiway_merge_no_distinct(self, chunk_files: List[str]) -> str:
        """
            Merge chunk_files into one final sorted file, NOT skipping duplicates.
        """
        file_handles = [open(cf, "rb") for cf in chunk_files]
        buffers = [self.read_next_row(fh) for fh in file_handles]

        final_path = os.path.join(self.directory, f"{self.table_name}_merge_sort.temp")
        open(final_path, 'wb').close()

        with open(final_path, "wb") as out_f:
            while True:
                chosen_index = -1
                chosen_row = None

                for i in range(len(buffers)):
                    row = buffers[i]
                    if row is None:
                        continue
                    if chosen_row is None:
                        chosen_index = i
                        chosen_row = row
                    else:
                        if self.compare_rows(row, chosen_row) < 0:
                            chosen_index = i
                            chosen_row = row

                if chosen_index == -1:
                    break

                self.write_row(out_f, chosen_row)
                buffers[chosen_index] = self.read_next_row(file_handles[chosen_index])

        for fh in file_handles:
            fh.close()

        return final_path

    def compare_rows(self, row1: HashTable, row2: HashTable) -> int:
        """
            Compare two rows by the same composite key used in write_sorted_chunk.
            1) r1 < r2 -> negative
            2) r1 == r2 -> 0
            3) r1 > r2 -> positive
        """
        if self.order_by_col is not None:
            primary1 = row1[self.order_by_col]
            primary2 = row2[self.order_by_col]
            if primary1 != primary2:
                compare_value = (primary1 > primary2) - (primary1 < primary2)

                if self.order == "ASC":
                    return compare_value
                else:
                    return -compare_value

        if self.distinct_cols:
            for col_name, _ in self.distinct_cols.items():
                if col_name == self.order_by_col:
                    continue

                val1 = row1[col_name]
                val2 = row2[col_name]

                if val1 != val2:
                    compare_value = (val1 > val2) - (val1 < val2)
                    return compare_value

        return 0

    def mergesort_in_memory(self, rows: List[HashTable]):
        n = len(rows)

        if n <= 1:
            return rows

        mid = n // 2
        left = self.mergesort_in_memory(rows[:mid])
        right = self.mergesort_in_memory(rows[mid:])
        return self.merge_two_lists(left, right)

    def merge_two_lists(self, left: List[HashTable], right: List[HashTable]):
        result = []
        i = 0
        j = 0

        while i < len(left) and j < len(right):
            if self.key_func(left[i]) <= self.key_func(right[j]):
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1

        while i < len(left):
            result.append(left[i])
            i += 1

        while j < len(right):
            result.append(right[j])
            j += 1

        return result

    def write_row(self, file_handle, row: HashTable):
        row_data = self.serialize_row(row)
        length = len(row_data)
        length_bytes = struct.pack("i", length)
        row_hash_val = polynomial_rolling_hash(length_bytes + row_data)
        row_hash_bytes = struct.pack("I", row_hash_val)

        file_handle.write(row_hash_bytes)
        file_handle.write(length_bytes)
        file_handle.write(row_data)

    def read_next_row(self, file_handle):
        stored_hash_bytes = file_handle.read(4)  # -> struct.calcsize("I") == 4
        if len(stored_hash_bytes) != 4:
            return None   # Possible behavior - not due to corruption
        stored_hash_val = struct.unpack("I", stored_hash_bytes)[0]

        length_data = file_handle.read(4)   # -> struct.calcsize("i") == 4
        if len(length_data) != 4:
            return None  # Possible behavior - not due to corruption

        length = struct.unpack("i", length_data)[0]
        row_data = file_handle.read(length)
        if len(row_data) < length:
            return None   # Possible behavior - not due to corruption

        computed_hash_val = polynomial_rolling_hash(length_data + row_data)
        if computed_hash_val != stored_hash_val:
            raise TableError("Corrupted file: MergeSort row error")

        row = self.deserialize_row(row_data)
        return row

    def serialize_row(self, row: HashTable) -> bytes:
        row_bytes = b""
        row_bytes += struct.pack("i", len(row))

        for column_name, row_value in row.items():
            row_bytes += struct.pack("i", len(column_name))
            row_bytes += column_name.encode()
            if isinstance(row_value, int):
                row_bytes += b'I' + struct.pack("i", row_value)
            elif isinstance(row_value, float):
                row_bytes += b'F' + struct.pack("d", row_value)
            elif isinstance(row_value, Date):
                row_bytes += b'D' + f"{row_value}".encode()
            elif isinstance(row_value, str):
                value_bytes = row_value.encode()
                row_bytes += b'S' + struct.pack("i", len(value_bytes)) + value_bytes

        return row_bytes

    def deserialize_row(self, row_data: bytes) -> HashTable:
        offset = 0
        total_columns = int(struct.unpack("i", row_data[offset:offset + 4])[0])
        row = HashTable(size=total_columns)
        offset += 4

        for _ in range(total_columns):
            col_name_length = int(struct.unpack("i", row_data[offset:offset + 4])[0])
            offset += 4
            col_name = row_data[offset:offset + col_name_length].decode()
            offset += col_name_length
            type_indicator = row_data[offset:offset + 1]
            offset += 1
            if type_indicator == b'I':
                row[col_name] = int(struct.unpack_from("i", row_data, offset)[0])
                offset += 4
            elif type_indicator == b'F':
                row[col_name] = float(struct.unpack_from("d", row_data, offset)[0])
                offset += 8
            elif type_indicator == b'D':
                value_bytes = row_data[offset:offset + 10]
                offset += 10
                row[col_name] = Date.from_string(value_bytes.decode())
            elif type_indicator == b'S':
                length = struct.unpack_from("i", row_data[offset: offset + 4])[0]
                offset += 4
                value_bytes = row_data[offset:offset + length]
                offset += length
                row[col_name] = value_bytes.decode()
        return row
