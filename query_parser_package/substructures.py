from data_structures.hash_table import HashTable
from db_components.column import Column


class ColumnDef:
    def __init__(self, name: str, col_type: str, constraints: HashTable):
        self.name = name
        self.col_type = col_type
        self.constraints = constraints

    def extract_column(self):
        return Column(self.name, self.col_type, self.constraints)

    def __repr__(self):
        return f"ColumnDef(name={self.name}, type={self.col_type}, constraints={self.constraints})"


class OrderByItem:
    def __init__(self, column_name: str, direction='ASC'):
        self.column_name = column_name
        self.direction = direction  # 'ASC' or 'DESC'

    def __repr__(self):
        return f"OrderByItem({self.column_name}, direction={self.direction})"
