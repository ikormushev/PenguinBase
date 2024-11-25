import os

from data_structures.btree import BTree
from db_components.column import Column


class TableIndex:
    def __init__(self, index_name: str, column: Column, index_path: str, index_tree: BTree | None = None):
        self.index_name = index_name
        self.column = column
        self.index_path = index_path
        self.index_tree = index_tree if index_tree is not None else None

    def load_index(self):
        self.index_tree = BTree.deserialize_tree(self.index_path)

    def save_index(self):
        if self.index_tree is not None:
            self.index_tree.serialize_tree(self.index_path)

    def delete_index(self):
        os.remove(self.index_path)

    def print_index(self):
        if self.index_tree is None:
            self.load_index()

        self.index_tree.print_tree()

    def __str__(self):
        return f"{self.column.column_name}|{self.index_name}|{self.index_path}"
