import os
import struct
import sys
from typing import List

from data_structures.dynamic_queue import DynamicQueue
from data_structures.hash_table import HashTable
from db_components.freeslot import FreeSlot
from db_components.index import TableIndex
from db_components.merge_sort_handler import MergeSortHandler
from db_components.metadata import Metadata
from query_parser_package.expressions import BinaryOpNode, NotNode, ValueNode
from utils.date import Date
from utils.errors import TableError, ParseError
from settings import PBDB_FILES_PATH
from utils.extra import polynomial_rolling_hash, intersect_unsorted, union_unsorted, difference_unsorted
from utils.table_random_values_generator import generate_random_rows


class TableNode:
    def __init__(self, row_data: HashTable | None = None, position=-1, previous_position=-1, next_position=-1):
        self.row_data = row_data if row_data is not None else HashTable()
        self.position = position
        self.previous_position = previous_position
        self.next_position = next_position

    def __str__(self):
        return f"Prev: {self.previous_position}, Pos: {self.position}, Next: {self.next_position}"

    def filter_row(self, columns_to_filter: HashTable):
        new_row_data = HashTable()
        for column_name, _ in columns_to_filter.items():
            new_row_data[column_name] = self.row_data[column_name]
        return new_row_data


class Table:
    def __init__(self, table_name: str):
        self.table_name = table_name

        self.directory = os.path.join(PBDB_FILES_PATH, table_name)
        if not os.path.exists(self.directory):
            raise TableError(f"Directory of '{self.table_name}' not found!")

        self.data_file_path = os.path.join(self.directory, f"{self.table_name}.data")
        if not os.path.exists(self.data_file_path):
            raise TableError(f"Data file of '{self.table_name}' not found!")

        self.metadata_file_path = os.path.join(self.directory, f"{self.table_name}.meta")
        if not os.path.exists(self.metadata_file_path):
            raise TableError(f"Metadata file of '{self.table_name}' not found!")

        self.metadata = Metadata.load_metadata(self.metadata_file_path)

    @staticmethod
    def check_given_name(name: str) -> bool:

        if len(name) < 3 or len(name) > 64:
            return False

        allowed_characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789'

        for char in name:
            if char not in allowed_characters:
                return False

        return True

    @staticmethod
    def create_table(table_name: str, columns: HashTable):
        if not Table.check_given_name(table_name):
            raise TableError(f"Invalid table name - {table_name}! "
                             f"Size can be between 3 and 64 characters. "
                             f"Valid characters are only a-z, A-Z, 0-9, and _")

        directory = os.path.join(PBDB_FILES_PATH, table_name)
        if os.path.exists(directory):
            raise TableError(f"Table '{table_name}' already exists!")

        os.makedirs(directory)
        metadata_file_path = os.path.join(directory, f"{table_name}.meta")
        data_file_path = os.path.join(directory, f"{table_name}.data")

        metadata = Metadata(table_name=table_name,
                            columns=columns,
                            metadata_file_path=metadata_file_path)
        metadata.save_metadata()

        open(data_file_path, "w").close()

    def serialize_table_row(self, node: TableNode) -> bytes:
        row_bytes = b""

        for column_name, column in self.metadata.columns.items():
            row_value = node.row_data[column_name]

            if column.column_type == "number":
                if isinstance(row_value, int):
                    row_bytes += b'I' + struct.pack("i", row_value)
                elif isinstance(row_value, float):
                    row_bytes += b'F' + struct.pack("d", row_value)
            elif column.column_type == "string":
                value_bytes = row_value.encode()
                row_bytes += struct.pack("i", len(value_bytes)) + value_bytes
            elif column.column_type == "date":
                row_bytes += f"{row_value}".encode()

        return row_bytes

    def deserialize_table_row(self, row_data: bytes) -> HashTable:
        offset = 0
        row = HashTable(size=len(self.metadata.columns))

        try:
            for column_name, col in self.metadata.columns.items():
                if col.column_type == "number":
                    type_indicator = row_data[offset:offset + 1]
                    offset += 1
                    if type_indicator == b'I':
                        row[column_name] = int(struct.unpack_from("i", row_data[offset: offset + 4])[0])
                        offset += 4  # -> struct.calcsize("i") == 4
                    elif type_indicator == b'F':
                        row[column_name] = float(struct.unpack_from("d", row_data[offset: offset + 8])[0])
                        offset += 8  # -> struct.calcsize("d") == 8
                elif col.column_type == "string":
                    length = struct.unpack_from("i", row_data[offset: offset + 4])[0]
                    offset += 4  # -> struct.calcsize("i") == 4
                    value_bytes = row_data[offset:offset + length]
                    offset += length
                    row[column_name] = value_bytes.decode()
                elif col.column_type == "date":
                    value_bytes = row_data[offset:offset + 10]
                    offset += 10  # -> Date object always has a length of 10
                    row[column_name] = Date.from_string(value_bytes.decode())
        except ValueError as ve:
            raise TableError(f"Corrupted file: cannot decode row data: {ve}")

        return row

    def serialize_table_node(self, node: TableNode):
        row_bytes = self.serialize_table_row(node)
        header = struct.pack("iii", node.previous_position, node.next_position, len(row_bytes))

        node_hash_val = polynomial_rolling_hash(header + row_bytes)
        row_hash_bytes = struct.pack("I", node_hash_val)

        return row_hash_bytes + header + row_bytes

    def save_table_node(self, node: TableNode, data_path=None):
        if data_path is None:
            data_path = self.data_file_path

        node_bytes_data = self.serialize_table_node(node)

        with open(data_path, "rb+") as file:
            file.seek(node.position)
            file.write(node_bytes_data)
            file.flush()

    def load_table_node(self, position: int, data_path=None) -> TableNode:
        if data_path is None:
            data_path = self.data_file_path

        with open(data_path, "rb") as file:
            file.seek(position)
            stored_hash_bytes = file.read(4)  # -> struct.calcsize("I") == 4

            if len(stored_hash_bytes) != 4:
                raise TableError(f"Corrupted file: cannot read the node hash")
            stored_hash_val = struct.unpack("I", stored_hash_bytes)[0]
            header_size = struct.calcsize("iii")
            header = file.read(header_size)

            if len(header) != header_size:
                raise TableError(f"Corrupted file: cannot read the node header")

            previous_position, next_position, row_size = struct.unpack("iii", header)
            if row_size < 0:
                raise TableError(f"Corrupted file: row size corrupted")

            row_data_bytes = file.read(row_size)

        computed_hash_val = polynomial_rolling_hash(header + row_data_bytes)

        if computed_hash_val != stored_hash_val:
            raise TableError(f"Corrupted file: data corruption detected for node at position {position}")

        row_data = self.deserialize_table_row(row_data_bytes)

        return TableNode(row_data=row_data,
                         position=position, previous_position=previous_position, next_position=next_position)

    def validate_row(self, row: HashTable) -> HashTable:
        metadata_columns = [col for col in self.metadata.columns.items()]

        for _, col in metadata_columns:
            value = row[col.column_name]

            if value is None:
                if col.DEFAULT:
                    row[col.column_name] = col.DEFAULT
                    continue
                else:
                    raise TableError(f"Column '{col.column_name}' required!")

            converted_value = col.convert_from_string_to_column_value(value)
            col.validate_column_value_size(converted_value)

            row[col.column_name] = converted_value

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
                self.metadata.free_slots.remove(slot)
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
            last_node = self.load_table_node(self.metadata.last_offset)
            last_node.next_position = position

            self.save_table_node(last_node)
            new_node.previous_position = self.metadata.last_offset

            self.metadata.last_offset = position

        new_node.next_position = -1
        self.save_table_node(new_node)
        self._add_row_to_indexes(new_node)
        self.metadata.rows_count += 1
        self.metadata.save_metadata()

    def get_rows(self, row_numbers: List[int]):
        rows_queue = DynamicQueue.from_list_sorted(row_numbers)

        start_rows = self.metadata.rows_count
        if rows_queue.length > start_rows:
            raise TableError(f"Too many rows! Table '{self.table_name}' has only {start_rows} rows!")

        current_offset = self.metadata.first_offset
        current_row = 1

        while current_offset != -1 and rows_queue.length > 0:
            target_row = rows_queue.peek()
            if target_row is not None and target_row > start_rows:
                break
            node = self.load_table_node(current_offset)

            if current_row == target_row:
                yield node.row_data
                rows_queue.dequeue()

            current_offset = node.next_position
            current_row += 1

    def _delete(self, node: TableNode):
        if node.previous_position != -1:
            prev_node = self.load_table_node(node.previous_position)
            prev_node.next_position = node.next_position
            self.save_table_node(prev_node)
        else:
            self.metadata.first_offset = node.next_position

        if node.next_position != -1:
            next_node = self.load_table_node(node.next_position)
            next_node.previous_position = node.previous_position
            self.save_table_node(next_node)
        else:
            self.metadata.last_offset = node.previous_position

        self._delete_row_from_indexes(node)

        node_size = len(self.serialize_table_node(node))
        free_slot = FreeSlot(node.position, node_size)
        self.metadata.free_slots.append(free_slot)

        self.metadata.rows_count -= 1

    def delete_rows(self, row_numbers: List[int]):
        rows_queue = DynamicQueue.from_list_sorted(row_numbers)

        start_rows = self.metadata.rows_count
        if rows_queue.length > start_rows:
            raise TableError(f"Too many rows! Table '{self.table_name}' has only {start_rows} rows!")

        current_offset = self.metadata.first_offset
        current_row = 1

        while current_offset != -1 and rows_queue.length > 0:
            target_row = rows_queue.peek()
            if target_row is not None and target_row > start_rows:
                break
            node = self.load_table_node(current_offset)

            if current_row == target_row:
                try:
                    self._delete(node)
                    self.metadata.save_metadata()
                except Exception as e:
                    raise TableError(f"Error occurred with deleting row at {node.position} with values: {node.row_data}")
                rows_queue.dequeue()

            current_offset = node.next_position
            current_row += 1

    def defragment(self):
        temp_file_path = self.data_file_path + ".temp"
        open(temp_file_path, "wb+").close()

        current_offset = 0
        new_first_offset = -1
        new_last_offset = -1
        row_count = 0

        old_offset = self.metadata.first_offset
        if old_offset != -1:
            new_first_offset = 0

        while old_offset != -1:
            node = self.load_table_node(old_offset)

            old_offset = node.next_position

            node.previous_position = new_last_offset
            node.position = current_offset

            if new_last_offset != -1:
                last_node = self.load_table_node(new_last_offset, temp_file_path)
                last_node.next_position = node.position
                self.save_table_node(last_node, temp_file_path)

            node.next_position = -1
            self.save_table_node(node, temp_file_path)

            new_last_offset = node.position
            current_offset += len(self.serialize_table_node(node))
            row_count += 1

        old_path = self.data_file_path
        os.remove(self.data_file_path)
        os.rename(temp_file_path, old_path)

        self.metadata.first_offset = new_first_offset
        self.metadata.last_offset = new_last_offset
        self.metadata.rows_count = row_count
        self.metadata.table_end = current_offset
        self.metadata.free_slots = []

        self._recreate_index_tree()

        self.metadata.save_metadata()

    def drop_table(self):
        if (not os.path.join(PBDB_FILES_PATH, self.table_name)
                or not os.path.exists(self.data_file_path)
                or not os.path.exists(self.metadata_file_path)):
            raise TableError(f"Table {self.table_name} files are missing")

        for _, index in self.metadata.indexes.items():
            index.delete_index()

        os.remove(self.data_file_path)
        os.remove(self.metadata_file_path)

        try:
            os.rmdir(self.directory)
        except Exception as e:
            raise TableError(f"Failed to remove directory {self.directory}: {e}")

    def _add_row_to_indexes(self, node: TableNode):
        for col_name, index in self.metadata.indexes.items():
            index.add_element_to_index(node.row_data[col_name], node.position)

    def _delete_row_from_indexes(self, node: TableNode):
        for col_name, index in self.metadata.indexes.items():
            index.remove_element_from_index(node.row_data[col_name], node.position)

    def _recreate_index_tree(self):
        for _, index in self.metadata.indexes.items():
            index.delete_index()
            new_index = TableIndex.create_index(index_name=index.index_name,
                                                column=index.column,
                                                index_path=index.index_path,
                                                pointer_list_path=index.pointer_list_data_path)
            self._create_index_tree(new_index)

    def _create_index_tree(self, index: TableIndex):
        index_column = index.column
        current_offset = self.metadata.first_offset

        while current_offset != -1:
            node = self.load_table_node(current_offset)

            column_key = node.row_data[index_column.column_name]
            index.add_element_to_index(column_key, node.position)

            current_offset = node.next_position

    def create_new_index(self, index_name: str, column_name: str):
        column = self.metadata.columns[column_name]

        if column is None:
            raise ParseError(f"Column '{column_name}' does not exist!")

        if self.metadata.indexes.search(column_name) is not None:
            raise TableError(f"Column '{column_name}' already has an index!")

        if not Table.check_given_name(index_name):
            raise TableError(f"Invalid index name - {index_name}! "
                             f"Size can be between 3 and 64 characters. "
                             f"Valid characters are only a-z, A-Z, 0-9, and _")

        index_path = os.path.join(self.directory, f"{index_name}_index.index")
        index_extra_data = os.path.join(self.directory, f"{index_name}_index.data")

        new_index = TableIndex.create_index(index_name=index_name,
                                            column=column,
                                            index_path=index_path,
                                            pointer_list_path=index_extra_data)
        self._create_index_tree(new_index)

        self.metadata.indexes[column_name] = new_index
        self.metadata.save_metadata()

    def drop_index(self, index_name: str):
        for col, index in self.metadata.indexes.items():
            if index.index_name == index_name:
                index.delete_index()
                self.metadata.indexes.delete(col)
                self.metadata.save_metadata()
                return

        raise TableError(f"Index '{index_name}' does not exist!")

    # TODO - Index visualization?
    def check_index(self, index_name: str):
        for _, index in self.metadata.indexes.items():
            if index.index_name == index_name:
                index.print_index()
                return

        raise TableError(f"Index '{index_name}' does not exist!")

    def tableinfo(self):
        return self.metadata.display_table_metadata(self.data_file_path)

    def insert_values(self, rows: List[HashTable]):
        for row in rows:
            self.insert(row)

    def insert_random(self, columns_names: List[str], count: int):
        table_columns_names = [column_name for column_name, _ in self.metadata.columns.items()]
        extracted_columns = []
        for col_name in columns_names:
            if col_name not in table_columns_names:
                raise TableError(f"Column '{col_name}' does not exist!")
            extracted_columns.append(self.metadata.columns[col_name])

        new_rows = generate_random_rows(extracted_columns, count)

        for row in new_rows:
            self.insert(row)

    def _full_scan(self, columns: HashTable):
        current_offset = self.metadata.first_offset
        while current_offset != -1:
            node = self.load_table_node(current_offset)
            yield node.filter_row(columns)
            current_offset = node.next_position

    def _full_scan_and_filter(self, columns: HashTable, where_expr):
        current_offset = self.metadata.first_offset
        while current_offset != -1:
            node = self.load_table_node(current_offset)
            row = node.row_data
            if where_expr.evaluate_expression(row):
                yield node.filter_row(columns)
            current_offset = node.next_position

    def _parse_index_plan(self, bin_expr):
        def flip_operator(op):
            if op == "<":
                return ">"
            if op == "<=":
                return ">="
            if op == ">":
                return "<"
            if op == ">=":
                return "<="
            return op

        if not (isinstance(bin_expr.left, ValueNode) and isinstance(bin_expr.right, ValueNode)):
            return None

        op = bin_expr.operator
        left = bin_expr.left
        right = bin_expr.right
        if op not in ["=", "!=", "<", "<=", ">", ">="]:
            return None

        if left.is_column and not right.is_column:
            col_name = left.value
            constant_val = right.value
        elif right.is_column and not left.is_column:
            col_name = right.value
            constant_val = left.value
            op = flip_operator(op)
        else:
            return None

        if self.metadata.indexes.search(col_name) is None:
            return None

        column = self.metadata.columns[col_name]
        try:
            column.validate_value_type(constant_val)
        except ValueError as e:
            raise ParseError(f"Query error: {e}")

        return HashTable([("col", col_name), ("op", op), ("value", constant_val)])

    def _execute_index_plan(self, plan):
        col = plan["col"]
        op = plan["op"]
        val = plan["value"]
        index = self.metadata.indexes[col]
        column = self.metadata.columns[col]

        if op == "=":
            return index.search(val)
        elif op == "<":
            return index.range_search(column, end=val)
        elif op == "<=":
            return index.range_search(column, end=val)
        elif op == ">":
            return index.range_search(column, start=val)
        elif op == ">=":
            return index.range_search(column, start=val)
        elif op == "!=":
            matched = index.search(val)
            all_val = index.range_search(column)

            return difference_unsorted(all_val, matched)

        return None

    def _evaluate_expression_for_index(self, where_expr):
        if isinstance(where_expr, BinaryOpNode):
            op_up = where_expr.operator
            if op_up == "AND":
                left_offsets = self._evaluate_expression_for_index(where_expr.left)
                right_offsets = self._evaluate_expression_for_index(where_expr.right)

                if left_offsets is None or right_offsets is None:
                    return None

                return intersect_unsorted(left_offsets, right_offsets)
            elif op_up == "OR":
                left_offsets = self._evaluate_expression_for_index(where_expr.left)
                right_offsets = self._evaluate_expression_for_index(where_expr.right)

                if left_offsets is None or right_offsets is None:
                    return None

                return union_unsorted(left_offsets, right_offsets)
            else:
                plan = self._parse_index_plan(where_expr)

                if plan is None:
                    return None

                return self._execute_index_plan(plan)
        return None

    def filter(self, columns: HashTable, where_expr):
        if where_expr is not None:
            offsets_gen = self._evaluate_expression_for_index(where_expr)
            if offsets_gen is not None:
                for offset in offsets_gen:
                    node = self.load_table_node(offset)
                    row = node.row_data

                    if where_expr.evaluate_expression(row):
                        yield node.filter_row(columns)
                return

        if where_expr is None:
            yield from self._full_scan(columns)
        else:
            yield from self._full_scan_and_filter(columns, where_expr)

    def _full_scan_delete(self, where_expr):
        current_offset = self.metadata.first_offset
        while current_offset != -1:
            node = self.load_table_node(current_offset)
            row = node.row_data

            if where_expr.evaluate_expression(row):
                try:
                    self._delete(node)
                    self.metadata.save_metadata()
                except Exception as e:
                    raise TableError(f"Error occurred with deleting row at {node.position} with values: {row}")
            current_offset = node.next_position

    def delete_filtered(self, where_expr):
        if where_expr is None:
            raise ParseError("Delete WHERE clause empty")

        #  Waaayy too overhead to delete using an index:
        #  BTree is used to delete, while also elements are being deleted from it which causes restructuring...
        #  There is even a patent for that...

        self._full_scan_delete(where_expr)

    def select_rows(self, columns: HashTable, where_expr, distinct: bool, order_by):
        filtered_rows = self.filter(columns, where_expr)

        if not distinct and not order_by:
            for row in filtered_rows:
                yield row
            return

        distinct_cols = columns if distinct else None
        order_by_col = order_by.column_name if order_by else None
        order = order_by.direction if order_by else "ASC"

        merge_sort_handler = MergeSortHandler(self.directory, self.table_name,
                                              distinct_cols=distinct_cols,
                                              order_by_col=order_by_col, order=order)

        merged_rows_path = merge_sort_handler.select_merge_sort(filtered_rows)

        if not os.path.exists(merged_rows_path):
            raise TableError(f"MergeSort path '{merged_rows_path}' does not exist!")

        with open(merged_rows_path, "rb") as f:
            while True:
                row = merge_sort_handler.read_next_row(f)
                if row is None:
                    break
                yield row

        if os.path.exists(merged_rows_path):
            os.remove(merged_rows_path)
