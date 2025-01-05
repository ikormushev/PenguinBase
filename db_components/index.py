import os
from data_structures.btree.btree import BTree
from data_structures.hash_table import HashTable
from db_components.column import Column
from utils.date import Date
from utils.errors import TableError


class TableIndex:
    def __init__(self, index_name: str, column: Column, index_path: str, pointer_list_data_path: str):
        self.index_name = index_name
        self.column = column
        self.index_path = index_path
        self.pointer_list_data_path = pointer_list_data_path
        self.index_tree = BTree(index_path, pointer_list_data_path)

    @staticmethod
    def create_index(index_name, column, index_path, pointer_list_path):
        key_types = HashTable([("number", "N"), ("string", "S"), ("date", "D")])

        key_max_value = 0
        if column.column_type == "string":
            key_max_value = column.max_value

        btree = BTree.create_tree(t=3,
                                  key_type=key_types[column.column_type],
                                  key_max_size=key_max_value,
                                  node_file_path=index_path,
                                  pointer_file_path=pointer_list_path)

        return TableIndex(index_name, column, index_path, pointer_list_path)

    def add_element_to_index(self, key, pointer: int):
        self.index_tree.insert(key, pointer)

    def remove_element_from_index(self, key, pointer: int):
        self.index_tree.delete_pointer(key, pointer)

    def delete_index(self):
        if not os.path.exists(self.index_path) or not os.path.exists(self.pointer_list_data_path):
            raise TableError(f"Index files for index {self.index_name} missing")

        os.remove(self.index_path)
        os.remove(self.pointer_list_data_path)

    def search(self, key):
        return self.index_tree.search(key)

    def range_search(self, column, start=None, end=None):
        default_ranges = []

        column_type = column.column_type
        if column_type == "string":
            default_ranges = [" ", "~" * column.MAX_SIZE]
        elif column_type == "number":
            default_ranges = [-float("inf"), float("inf")]
        elif column_type == "date":
            default_ranges = [Date.from_string("01.01.0001"), Date.from_string("31.12.9999")]

        if start is None:
            start = default_ranges[0]
        if end is None:
            end = default_ranges[1]

        yield from self.index_tree.range_search(start, end)

    def print_index(self):
        self.index_tree.print_tree()

    def __str__(self):
        return f"{self.column.column_name}|{self.index_name}|{self.index_path}|{self.pointer_list_data_path}"
