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
        return HashTable([("message", f"Successfully created table with name: {self.table_name}")])


class DropTableStatement(Statement):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"DROP TABLE {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.drop_table()
        return HashTable([("message", f"Successfully dropped table with name: {self.table_name}")])


class TableInfoStatement(Statement):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"TABLEINFO {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        tableinfo = table.tableinfo()
        return HashTable([("message", f"Successfully retrieved tableinfo of {self.table_name}"),
                          ("tableinfo", tableinfo), ("table", table)])


class InsertValuesStatement(Statement):
    def __init__(self, table_name: str, rows: List[HashTable]):
        self.table_name = table_name
        self.rows = rows

    def __repr__(self):
        return f"INSERT INTO {self.table_name} (col1, col2, ...) VALUES {self.rows};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.insert_values(self.rows)
        return HashTable([("message", f"Successfully inserted values in {self.table_name}"), ("table", table)])


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
        return HashTable([("message", f"Successfully inserted random values in {self.table_name}"), ("table", table)])


class GetRowStatement(Statement):
    def __init__(self, table_name: str, row_numbers: List[int]):
        self.table_name = table_name
        self.row_numbers = row_numbers

    def __repr__(self):
        return f"GET ROW {self.row_numbers} FROM {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)

        return HashTable([("message", f"Successfully got rows from {self.table_name}"),
                          ("rows", table.get_rows(self.row_numbers)), ("columns", table.metadata.columns), ("table", table)])


class DeleteRowStatement(Statement):
    def __init__(self, table_name: str, row_numbers: List[int]):
        self.table_name = table_name
        self.row_numbers = row_numbers

    def __repr__(self):
        return f"DELETE FROM {self.table_name} ROW {self.row_numbers};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.delete_rows(self.row_numbers)
        return HashTable([("message", f"Successfully deleted rows from {self.table_name}"), ("table", table)])


class DeleteWhereStatement(Statement):
    def __init__(self, table_name: str, where_expr: ExpressionNode):
        self.table_name = table_name
        self.where_expr = where_expr

    def __repr__(self):
        return f"DELETE FROM {self.table_name} WHERE {self.where_expr};"

    def execute_statement(self):
        table = Table(table_name=self.table_name)
        table.delete_filtered(self.where_expr)
        return HashTable([("message", f"Successfully deleted rows from {self.table_name}"), ("table", table)])


class SelectStatement(Statement):
    def __init__(self, columns: List[str], table_name: str, distinct: bool = False,
                 where_expr: ExpressionNode | None = None, order_by: OrderByItem | None = None):
        self.columns = columns
        self.table_name = table_name
        self.distinct = distinct
        self.where_expr = where_expr
        self.order_by = order_by

    def __repr__(self):
        return (f"SELECT {'DISTINCT' if self.distinct else ''} {self.columns} "
                f"FROM {self.table_name} WHERE {self.where_expr} ORDER BY {self.order_by};")

    def execute_statement(self):
        table = Table(self.table_name)

        if len(self.columns) == 1 and self.columns[0] == "*":
            columns_to_show = HashTable([(col_name, col) for col_name, col in table.metadata.columns.items()])
        else:
            invalid_columns = [col_name for col_name in self.columns if table.metadata.columns.search(col_name) is None]
            if invalid_columns:
                raise ParseError(f"Invalid column names: {', '.join(invalid_columns)}")

            columns_to_show = HashTable([(col_name, col) for col_name, col in table.metadata.columns.items()
                                         if col_name in self.columns])

        if len(columns_to_show) == 0:
            raise ParseError("No columns to show!")

        if self.order_by:
            order_by_column_name = self.order_by.column_name

            if columns_to_show.search(order_by_column_name) is None:
                raise ParseError("Invalid ORDER BY column!")

        table_selected_rows_generator = table.select_rows(columns=columns_to_show,
                                                          where_expr=self.where_expr,
                                                          distinct=self.distinct,
                                                          order_by=self.order_by)

        return HashTable([("message", f"Successfully selected rows from {self.table_name}"),
                          ("rows", table_selected_rows_generator), ("columns", columns_to_show), ("table", table)])


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
        return HashTable([("message", f"Successfully created index {self.index_name} for {self.table_name}"), ("table", table)])


class DropIndexStatement(Statement):
    def __init__(self, index_name: str, table_name: str):
        self.index_name = index_name
        self.table_name = table_name

    def __repr__(self):
        return f"DROP INDEX {self.index_name} ON {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.drop_index(self.index_name)
        return HashTable([("message", f"Successfully dropped index {self.index_name} for {self.table_name}"), ("table", table)])


class DefragmentTableStatement(Statement):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"DEFRAGMENT {self.table_name};"

    def execute_statement(self):
        table = Table(self.table_name)
        table.defragment()
        return HashTable([("message", f"Successfully defragmented {self.table_name}"), ("table", table)])
