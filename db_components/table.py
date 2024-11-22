import struct

from data_structures.dynamic_queue import DynamicQueue
from data_structures.hash_table import HashTable
from db_components.freeslot import FreeSlot
from db_components.metadata import Metadata
from utils.errors import OutOfRangeError, MandatoryColumnError
from utils.validators import is_valid_number, is_valid_date


class TableNode:
    def __init__(self, row_data: HashTable | None = None, position=-1, previous_position=-1, next_position=-1):
        self.row_data = row_data if row_data is not None else HashTable()
        self.position = position
        self.previous_position = previous_position
        self.next_position = next_position


class Table:
    def __init__(self, data_file_path: str, metadata_file_path: str):
        self.data_file_path = data_file_path
        self.data_stream = open(data_file_path, "r+b")
        self.metadata = Metadata.load_metadata(metadata_file_path)

    def serialize_table_node(self, node: TableNode) -> bytes:
        row_bytes = self.serialize_table_row(node)
        node_format = "iii"
        header = struct.pack(node_format,
                             node.previous_position,
                             node.next_position,
                             len(row_bytes))
        return header + row_bytes

    def deserialize_table_node(self, position: int) -> TableNode:
        self.data_stream.seek(position)
        node_format = "iii"
        header_size = struct.calcsize(node_format)
        header = self.data_stream.read(header_size)
        previous_position, next_position, row_size = struct.unpack(node_format, header)

        row_data_bytes = self.data_stream.read(row_size)
        row_data = self.deserialize_table_row(row_data_bytes)

        return TableNode(row_data=row_data,
                         position=position,
                         previous_position=previous_position,
                         next_position=next_position)

    def serialize_table_row(self, node: TableNode):
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

    def deserialize_table_row(self, row_data: bytes):
        metadata_columns = self.metadata.columns
        offset = 0
        row = HashTable()

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

    def insert(self, row_data: HashTable):
        validated_row = self.validate_row(row_data)
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

    def delete(self, row: TableNode):
        if row.previous_position != -1:
            prev_node = self.deserialize_table_node(row.previous_position)
            prev_node.next_position = row.next_position
            self.data_stream.seek(prev_node.position)
            self.data_stream.write(self.serialize_table_node(prev_node))
        else:
            self.metadata.first_offset = row.next_position

        if row.next_position != -1:
            next_node = self.deserialize_table_node(row.next_position)
            next_node.previous_position = row.previous_position
            self.data_stream.seek(next_node.position)
            self.data_stream.write(self.serialize_table_node(next_node))
        else:
            self.metadata.last_offset = row.previous_position

        node_size = len(self.serialize_table_node(row))
        self.metadata.free_slots.append(FreeSlot(row.position, node_size))

        self.metadata.rows_count -= 1
        self.metadata.save_metadata()

    def delete_rows(self, row_numbers: DynamicQueue):
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

        if row_numbers.length > 0:
            raise OutOfRangeError("The table has no more rows.")
