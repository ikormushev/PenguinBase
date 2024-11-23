import os
import struct
from typing import List

from data_structures.dynamic_queue import DynamicQueue
from data_structures.hash_table import HashTable
from db_components.column import Column
from db_components.freeslot import FreeSlot
from db_components.metadata import Metadata
from utils.errors import OutOfRangeError, MandatoryColumnError, TableAlreadyExistsError, TableDoesNotExistError
from utils.validators import is_valid_number, is_valid_date


class TableNode:
    def __init__(self, row_data: HashTable | None = None, position=-1, previous_position=-1, next_position=-1):
        self.row_data = row_data if row_data is not None else HashTable()
        self.position = position
        self.previous_position = previous_position
        self.next_position = next_position

    def __str__(self):
        return f"Prev: {self.previous_position}, Pos: {self.position}, Next: {self.next_position}"


class Table:
    def __init__(self, directory: str, table_name: str):
        self.directory = directory
        self.table_name = table_name

        self.data_file_path = os.path.join(self.directory, f"{self.table_name}.data")
        if not os.path.exists(self.data_file_path):
            raise TableDoesNotExistError(f"Data file of '{self.table_name}' not found!")

        self.metadata_file_path = os.path.join(self.directory, f"{self.table_name}.meta")
        if not os.path.exists(self.metadata_file_path):
            raise TableDoesNotExistError(f"Metadata file of '{self.table_name}' not found!")

        self.data_stream = open(self.data_file_path, "r+b")
        self.metadata = Metadata.load_metadata(self.metadata_file_path)

    # @property
    # def data_file_path(self):
    #     return self.__data_file_path
    #
    # @data_file_path.setter
    # def data_file_path(self, value):
    #     if not os.path.exists(value):
    #         raise TableDoesNotExistError(f"Data file of '{self.table_name}' not found!")
    #     self.__data_file_path = value
    #
    # @property
    # def metadata_file_path(self):
    #     return self.__metadata_file_path
    #
    # @metadata_file_path.setter
    # def metadata_file_path(self, value):
    #     if not os.path.exists(value):
    #         raise TableDoesNotExistError(f"Metadata file of '{self.table_name}' not found!")
    #     self.__metadata_file_path = value

    @staticmethod
    def create_table(directory: str, table_name: str, columns: List[Column]):
        metadata_file_path = os.path.join(directory, f"{table_name}.meta")
        data_file_path = os.path.join(directory, f"{table_name}.data")

        if os.path.exists(metadata_file_path) or os.path.exists(data_file_path):
            raise TableAlreadyExistsError(f"Table '{table_name}' already exists!")

        metadata = Metadata(table_name=table_name,
                            columns=columns,
                            metadata_file_path=metadata_file_path)
        metadata.save_metadata()

        open(data_file_path, "w").close()

    def serialize_table_node(self, node: TableNode) -> bytes:
        row_bytes = self.serialize_table_row(node)
        node_format = "iii"
        header = struct.pack(node_format,
                             node.previous_position,
                             node.next_position,
                             len(row_bytes))
        return header + row_bytes

    def deserialize_table_node(self, position: int, file_stream=None) -> TableNode:
        if file_stream is None:
            file_stream = self.data_stream
        file_stream.seek(position)
        node_format = "iii"
        header_size = struct.calcsize(node_format)
        header = file_stream.read(header_size)
        previous_position, next_position, row_size = struct.unpack(node_format, header)

        row_data_bytes = file_stream.read(row_size)
        row_data = self.deserialize_table_row(row_data_bytes)

        return TableNode(row_data=row_data,
                         position=position,
                         previous_position=previous_position,
                         next_position=next_position)

    def serialize_table_row(self, node: TableNode) -> bytes:
        metadata_columns = self.metadata.columns
        row_bytes = b""

        for column in metadata_columns:
            row_value = node.row_data[column.column_name]
            if column.column_type == "number":
                if isinstance(row_value, int):
                    row_bytes += b'I' + struct.pack("i", row_value)
                elif isinstance(row_value, float):
                    row_bytes += b'F' + struct.pack("f", row_value)
            elif column.column_type == "string":
                value_bytes = row_value.encode("utf-8")
                row_bytes += struct.pack("i", len(value_bytes)) + value_bytes
            elif column.column_type == "date":
                row_bytes += row_value.encode("utf-8")

        return row_bytes

    def deserialize_table_row(self, row_data: bytes) -> HashTable:
        metadata_columns = self.metadata.columns
        offset = 0
        row = HashTable(len(metadata_columns))

        for col in metadata_columns:
            if col.column_type == "number":
                type_indicator = row_data[offset:offset + 1]
                offset += 1
                if type_indicator == b'I':  # Integer
                    row[col.column_name] = struct.unpack_from("i",
                                                              row_data,
                                                              offset)[0]
                    offset += struct.calcsize("i")
                elif type_indicator == b'F':  # Float
                    row[col.column_name] = struct.unpack_from("f",
                                                              row_data,
                                                              offset)[0]
                    offset += struct.calcsize("f")
            elif col.column_type == "string":
                length = struct.unpack_from("i", row_data, offset)[0]
                offset += struct.calcsize("i")
                value_bytes = row_data[offset:offset + length]
                offset += length
                row[col.column_name] = value_bytes.decode()
            elif col.column_type == "date":
                value_bytes = row_data[offset:offset + 10]
                offset += 10
                row[col.column_name] = value_bytes.decode()
        return row

    def validate_row(self, row: HashTable) -> HashTable:
        metadata_columns = self.metadata.columns

        for col in metadata_columns:
            try:
                value = row[col.column_name]

                if ((col.column_type == "number" and is_valid_number(value))
                        or (col.column_type == "date" and is_valid_date(value))
                        or (col.column_type == "string" and isinstance(value, str))):
                    is_value_valid = True
                else:
                    raise ValueError(f"Value for column {col.column_name} "
                                     f"has to be of type '{col.column_type}'!")

                # TODO - check for PK uniquness
                # if col.is_primary_key:
                #     check_primary_key(col, value)

                if is_value_valid:
                    row[col.column_name] = value

            except KeyError:
                if col.default_value:
                    row[col.column_name] = col.default_value
                else:
                    raise MandatoryColumnError(f"Column '{col.column_name}' requires a value!")

        return row

    def insert(self, row: HashTable):
        validated_row = self.validate_row(row)
        new_node = TableNode(row_data=validated_row)

        node_bytes = self.serialize_table_node(new_node)
        node_size = len(node_bytes)

        position = None
        for slot in self.metadata.free_slots:
            if node_size <= slot.slot_length:
                position = slot.slot_position
                self.metadata.free_slots.remove(slot)  # TODO - .remove() може ли да се използва?
                break

        if position is None:
            position = self.metadata.table_end
            self.metadata.table_end += node_size

        new_node.position = position

        if self.metadata.last_offset == -1:
            self.metadata.first_offset = position
            self.metadata.last_offset = position
            new_node.previous_position = -1
        else:
            last_node = self.deserialize_table_node(self.metadata.last_offset)
            last_node.next_position = position

            self.data_stream.seek(self.metadata.last_offset)
            self.data_stream.write(self.serialize_table_node(last_node))

            new_node.previous_position = self.metadata.last_offset

            self.metadata.last_offset = position

        new_node.next_position = -1
        node_bytes = self.serialize_table_node(new_node)
        self.data_stream.seek(position)
        self.data_stream.write(node_bytes)
        self.data_stream.close()

        self.metadata.rows_count += 1
        self.metadata.save_metadata()

    def get_rows(self, row_numbers: DynamicQueue):
        # TODO - row_numbers - sorted?
        current_offset = self.metadata.first_offset
        current_row = 1

        while current_offset != -1 and row_numbers.length > 0:
            target_row = row_numbers.peek()
            if target_row is not None and target_row > self.metadata.rows_count:
                break
            node = self.deserialize_table_node(current_offset)

            if current_row == target_row:
                yield node.row_data
                row_numbers.dequeue()

            current_offset = node.next_position
            current_row += 1

        if row_numbers.length > 0:
            raise OutOfRangeError("The table has no more rows.")

    def delete(self, node: TableNode):
        if node.previous_position != -1:
            prev_node = self.deserialize_table_node(node.previous_position)
            prev_node.next_position = node.next_position
            self.data_stream.seek(prev_node.position)
            self.data_stream.write(self.serialize_table_node(prev_node))
        else:
            self.metadata.first_offset = node.next_position

        if node.next_position != -1:
            next_node = self.deserialize_table_node(node.next_position)
            next_node.previous_position = node.previous_position
            self.data_stream.seek(next_node.position)
            self.data_stream.write(self.serialize_table_node(next_node))
        else:
            self.metadata.last_offset = node.previous_position

        node_size = len(self.serialize_table_node(node))
        self.metadata.free_slots.append(FreeSlot(node.position, node_size))

        self.metadata.rows_count -= 1

    def delete_rows(self, row_numbers: DynamicQueue):
        # TODO - row_numbers - sorted?
        start_rows = self.metadata.rows_count
        current_offset = self.metadata.first_offset
        current_row = 1

        while current_offset != -1 and row_numbers.length > 0:
            target_row = row_numbers.peek()
            if target_row is not None and target_row > start_rows:
                break
            node = self.deserialize_table_node(current_offset)

            if current_row == target_row:
                self.delete(node)
                row_numbers.dequeue()

            current_offset = node.next_position
            current_row += 1

        self.metadata.save_metadata()
        self.data_stream.close()

        if row_numbers.length > 0:
            raise OutOfRangeError("The table ran out of rows!")

        # if self.metadata.rows_count > 0:
        #     fragmentation_percentage = (len(self.metadata.free_slots)
        #                                 / self.metadata.rows_count) * 100
        #     if fragmentation_percentage >= 20:
        #         print("File is mostly fragmented")
        #         self.defragment()

    def defragment(self):
        temp_file_path = self.data_file_path + ".temp"
        temp_stream = open(temp_file_path, "w+b")

        current_offset = 0
        new_first_offset = -1
        new_last_offset = -1
        row_count = 0

        old_offset = self.metadata.first_offset
        if old_offset != -1:
            new_first_offset = 0

        while old_offset != -1:
            node = self.deserialize_table_node(old_offset)

            old_offset = node.next_position

            node.previous_position = new_last_offset
            node.position = current_offset

            if new_last_offset != -1:
                last_node = self.deserialize_table_node(new_last_offset, file_stream=temp_stream)
                last_node.next_position = node.position
                serialized_last_node = self.serialize_table_node(last_node)

                temp_stream.seek(last_node.position)
                temp_stream.write(serialized_last_node)

            node.next_position = -1
            serialized_node = self.serialize_table_node(node)
            temp_stream.seek(node.position)
            temp_stream.write(serialized_node)

            new_last_offset = node.position
            current_offset += len(serialized_node)
            row_count += 1

        temp_stream.close()
        self.data_stream.close()

        old_path = self.data_file_path
        os.remove(self.data_file_path)
        os.rename(temp_file_path, old_path)

        self.metadata.first_offset = new_first_offset
        self.metadata.last_offset = new_last_offset
        self.metadata.rows_count = row_count
        self.metadata.table_end = current_offset
        self.metadata.free_slots = []

        self.metadata.save_metadata()
