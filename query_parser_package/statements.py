from abc import ABC, abstractmethod
from typing import List

from data_structures.hash_table import HashTable
from db_components.table import Table
from query_parser_package.expressions import ExpressionNode
from query_parser_package.substructures import ColumnDef, OrderByItem
from utils.errors import ParseError


class Statement(ABC):
    """
        Base class for all DBMS statements.
    """

    @abstractmethod
    def execute_statement(self):
        ...


class CreateTableStatement(Statement):
    def __init__(self, table_name: str, columns: List[ColumnDef]):
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        return f"CREATE TABLE {self.table_name} {self.columns};"

    def execute_statement(self):
        columns = HashTable(size=len(self.columns))
        for column in self.columns:
            new_column = column.extract_column()
            columns[new_column.column_name] = new_column
        Table.create_table(self.table_name, columns)


class DropTableStatement(Statement):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"DROP TABLE {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.drop_table()


class TableInfoStatement(Statement):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"TABLEINFO {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        tableinfo = table.tableinfo()
        print(tableinfo)


class InsertValuesStatement(Statement):
    def __init__(self, table_name: str, rows: List[HashTable]):
        self.table_name = table_name
        self.rows = rows

    def __repr__(self):
        return f"INSERT INTO {self.table_name} (col1, col2, ...) VALUES {self.rows};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.insert_values(self.rows)


class InsertRandomStatement(Statement):
    def __init__(self, table_name: str, columns_names: List[str], count: int):
        self.table_name = table_name
        self.columns_names = columns_names
        self.count = count

    def __repr__(self):
        return f"INSERT INTO {self.table_name} ({self.columns_names}) RANDOM {self.count};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.insert_random(self.columns_names, self.count)


class GetRowStatement(Statement):
    def __init__(self, table_name: str, row_numbers: List[int]):
        self.table_name = table_name
        self.row_numbers = row_numbers

    def __repr__(self):
        return f"GET ROW {self.row_numbers} FROM {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)

        # TODO - redo the print logic
        for row in table.get_rows(self.row_numbers):
            print(row)


class DeleteRowStatement(Statement):
    def __init__(self, table_name: str, row_numbers: List[int]):
        self.table_name = table_name
        self.row_numbers = row_numbers

    def __repr__(self):
        return f"DELETE FROM {self.table_name} ROW {self.row_numbers};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.delete_rows(self.row_numbers)


class DeleteWhereStatement(Statement):
    def __init__(self, table_name: str, where_expr: ExpressionNode):
        self.table_name = table_name
        self.where_expr = where_expr

    def __repr__(self):
        return f"DELETE FROM {self.table_name} WHERE {self.where_expr};"

    def execute_statement(self):
        table = Table(table_name=self.table_name)
        table.delete_filtered(self.where_expr)


class SelectStatement(Statement):
    def __init__(self, columns: List[str], table_name: str, distinct: bool = False,
                 where_expr: ExpressionNode | None = None, order_by: List[OrderByItem] | None = None):
        self.columns = columns
        self.table_name = table_name
        self.distinct = distinct
        self.where_expr = where_expr
        self.order_by = order_by or []

    def __repr__(self):
        return (f"SELECT {'DISTINCT' if self.distinct else ''} {self.columns} "
                f"FROM {self.table_name} WHERE {self.where_expr} ORDER BY {self.order_by};")

    def execute_statement(self):
        table = Table(self.table_name)

        if len(self.columns) == 1 and self.columns[0] == "*":
            columns_to_show = [col for col in table.metadata.columns.items()]
        else:
            columns_to_show = [col for _, col in table.metadata.columns.items() if col.column_name in self.columns]

        if len(columns_to_show) == 0:
            raise ParseError("No valid columns to show!")

        # TODO - redo the print logic
        for row in table.filter(columns_to_show, self.where_expr):
            print(row)


class CreateIndexStatement(Statement):
    def __init__(self, index_name: str, table_name: str, column_name: str):
        self.index_name = index_name
        self.table_name = table_name
        self.column_name = column_name

    def __repr__(self):
        return f"CREATE INDEX {self.index_name} ON {self.table_name} (self.column_name);"

    def execute_statement(self):
        table = Table(self.table_name)
        table.create_new_index(index_name=self.index_name, column_name=self.column_name)


class DropIndexStatement(Statement):
    def __init__(self, index_name: str, table_name: str):
        self.index_name = index_name
        self.table_name = table_name

    def __repr__(self):
        return f"DROP INDEX {self.index_name} ON {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.drop_index(self.index_name)


class DefragmentTableStatement(Statement):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"DEFRAGMENT {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.defragment()
