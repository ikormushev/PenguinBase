from typing import List

from data_structures.hash_table import HashTable
from query_parser_package.expressions import BinaryOpNode, NotNode, ValueNode
from query_parser_package.substructures import ColumnDef, OrderByItem
from query_parser_package.tokens import Token, TokenType
import query_parser_package.statements as st
from utils.date import Date
from utils.errors import ParseError


class QueryParser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0
        self.current_token = self.tokens[self.pos] if self.tokens else None
        self.reached_end = False

    def advance(self):
        self.pos += 1
        if self.pos < len(self.tokens):
            self.current_token = self.tokens[self.pos]

            if self.pos == len(self.tokens) - 1:
                self.reached_end = True
        else:
            self.current_token = Token(TokenType.EOF, '')

    def peek(self):
        peek_pos = self.pos + 1
        if peek_pos < len(self.tokens):
            return self.tokens[peek_pos]
        return Token(TokenType.EOF, '')

    def error(self, message):
        raise ParseError(message)

    def match(self, token_type):
        """
            Check if the current token matches the given type, then advance. If not, raise error.
        """
        if self.current_token.token_type == token_type:
            self.advance()
        else:
            self.error(f"Expected token type {token_type}, got {self.current_token.token_type}")

    def parse(self):
        """
            Entry point for parsing a single statement.
        """
        if self.current_token.token_type == TokenType.CREATE:
            return self.parse_create()
        elif self.current_token.token_type == TokenType.DROP:
            return self.parse_drop()
        elif self.current_token.token_type == TokenType.TABLEINFO:
            return self.parse_tableinfo()
        elif self.current_token.token_type == TokenType.INSERT:
            return self.parse_insert()
        elif self.current_token.token_type == TokenType.GET:
            return self.parse_get()
        elif self.current_token.token_type == TokenType.DELETE:
            return self.parse_delete()
        elif self.current_token.token_type == TokenType.SELECT:
            return self.parse_select()
        elif self.current_token.token_type == TokenType.DEFRAGMENT:
            return self.parse_defragment()
        else:
            self.error(f"Unknown statement starting with token {self.current_token.token_type}")

    @staticmethod
    def check_end_decorator(func):
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)

            self.match(TokenType.SECOL)

            if not self.reached_end:
                self.error("Invalid statement")
            return result

        return wrapper

    @check_end_decorator
    def parse_create(self):
        self.match(TokenType.CREATE)
        if self.current_token.token_type == TokenType.TABLE:
            return self.parse_create_table()
        elif self.current_token.token_type == TokenType.INDEX:
            return self.parse_create_index()
        else:
            self.error("Expected TABLE or INDEX after CREATE")

    def parse_create_table(self):
        self.match(TokenType.TABLE)

        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        self.match(TokenType.LPAREN)

        columns = []

        while self.current_token.token_type != TokenType.RPAREN:
            col_name_token = self.current_token
            self.match(TokenType.IDENTIFIER)

            if self.current_token.token_type == TokenType.COLON:
                self.advance()
            else:
                self.error("Expected ':' in column definition")

            col_type_token = self.current_token
            self.match(TokenType.IDENTIFIER)

            constraints = HashTable()

            while self.current_token.token_type in [TokenType.DEFAULT, TokenType.PRIMARY_KEY, TokenType.MAX_SIZE]:
                constraint_name = self.current_token.value
                self.match(self.current_token.token_type)

                if self.current_token.token_type == TokenType.COLON:
                    self.advance()
                else:
                    self.error(f"Expected ':' after constraint name '{constraint_name}'")

                constraint_value = self.current_token.value
                if self.current_token.token_type == TokenType.NUMBER:
                    self.match(TokenType.NUMBER)
                elif self.current_token.token_type == TokenType.FLOAT:
                    self.match(TokenType.FLOAT)
                elif self.current_token.token_type == TokenType.DATE:
                    self.match(TokenType.DATE)
                else:
                    self.match(TokenType.STRING)

                constraints[constraint_name] = constraint_value

                if self.current_token.token_type == TokenType.COMMA or self.current_token.token_type == TokenType.RPAREN:
                    break

            col_def = ColumnDef(
                name=col_name_token.value,
                col_type=col_type_token.value,
                constraints=constraints
            )
            columns.append(col_def)

            if self.current_token.token_type == TokenType.COMMA:
                self.advance()
            else:
                break

        self.match(TokenType.RPAREN)

        return st.CreateTableStatement(
            table_name=table_name_token.value,
            columns=columns
        )

    def parse_create_index(self):
        self.match(TokenType.INDEX)
        index_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        self.match(TokenType.ON)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        self.match(TokenType.LPAREN)
        column_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)
        self.match(TokenType.RPAREN)

        return st.CreateIndexStatement(
            index_name=index_name_token.value,
            table_name=table_name_token.value,
            column_name=column_name_token.value
        )

    @check_end_decorator
    def parse_drop(self):
        self.match(TokenType.DROP)

        if self.current_token.token_type == TokenType.TABLE:
            return self.parse_drop_table()
        elif self.current_token.token_type == TokenType.INDEX:
            return self.parse_drop_index()
        else:
            self.error("Expected TABLE or INDEX after DROP")

    def parse_drop_table(self):
        self.match(TokenType.TABLE)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)
        return st.DropTableStatement(table_name=table_name_token.value)

    def parse_drop_index(self):
        self.match(TokenType.INDEX)
        index_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        self.match(TokenType.ON)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        return st.DropIndexStatement(index_name=index_name_token.value, table_name=table_name_token.value)

    @check_end_decorator
    def parse_tableinfo(self):
        self.match(TokenType.TABLEINFO)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)
        return st.TableInfoStatement(table_name=table_name_token.value)

    @check_end_decorator
    def parse_insert(self):
        self.match(TokenType.INSERT)
        self.match(TokenType.INTO)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        self.match(TokenType.LPAREN)

        columns = []
        while self.current_token.token_type != TokenType.RPAREN:
            columns.append(self.current_token.value)
            self.match(TokenType.IDENTIFIER)
            if self.current_token.token_type == TokenType.COMMA:
                self.advance()
            else:
                break
        self.match(TokenType.RPAREN)

        if self.current_token.token_type == TokenType.VALUES:
            return self.parse_insert_values(columns, table_name_token.value)
        elif self.current_token.token_type == TokenType.RANDOM:
            return self.parse_insert_random(columns, table_name_token.value)
        else:
            self.error("Expected VALUES or RANDOM!")

    def parse_insert_random(self, columns: List[str], table_name: str):
        self.match(TokenType.RANDOM)

        count = None
        if self.current_token.token_type == TokenType.NUMBER:
            count = int(self.current_token.value)
            self.advance()

        if not count:
            self.error("Expected a number after RANDOM!")

        if count <= 0:
            self.error("Expected a positive number!")

        return st.InsertRandomStatement(table_name=table_name, columns_names=columns, count=count)

    def parse_insert_values(self, columns: List[str], table_name: str):
        self.match(TokenType.VALUES)

        rows = []
        while True:
            self.match(TokenType.LPAREN)
            row_values = []

            while self.current_token.token_type != TokenType.RPAREN:
                row_values.append(self.current_token.value)
                self.advance()
                if self.current_token.token_type == TokenType.COMMA:
                    self.advance()
                else:
                    break
            self.match(TokenType.RPAREN)

            if len(row_values) != len(columns):
                self.error("Invalid number of values")

            row = HashTable(size=len(columns))
            for i in range(len(columns)):
                row[columns[i]] = row_values[i]

            rows.append(row)

            if self.current_token.token_type == TokenType.COMMA:
                self.advance()
            else:
                break

        return st.InsertValuesStatement(
            table_name=table_name,
            rows=rows
        )

    @check_end_decorator
    def parse_get(self):
        self.match(TokenType.GET)
        self.match(TokenType.ROW)
        row_numbers = []
        while self.current_token.token_type == TokenType.NUMBER:
            row_numbers.append(int(self.current_token.value))
            self.advance()
            if self.current_token.token_type == TokenType.COMMA:
                self.advance()
            else:
                break

        self.match(TokenType.FROM)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)
        return st.GetRowStatement(table_name=table_name_token.value, row_numbers=row_numbers)

    @check_end_decorator
    def parse_delete(self):
        self.match(TokenType.DELETE)
        self.match(TokenType.FROM)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)

        if self.current_token.token_type == TokenType.ROW:
            return self.parse_delete_row(table_name_token.value)
        elif self.current_token.token_type == TokenType.WHERE:
            return self.parse_delete_where(table_name_token.value)
        else:
            self.error("Expected ROW or WHERE after DELETE FROM <table_name>")

    def parse_delete_row(self, table_name):
        self.match(TokenType.ROW)

        row_numbers = []
        while self.current_token.token_type == TokenType.NUMBER:
            row_numbers.append(int(self.current_token.value))
            self.advance()
            if self.current_token.token_type == TokenType.COMMA:
                self.advance()
            else:
                break

        if len(row_numbers) < 1:
            self.error("No rows given to delete")

        return st.DeleteRowStatement(table_name=table_name, row_numbers=row_numbers)

    def parse_delete_where(self, table_name):
        self.match(TokenType.WHERE)
        where_expr = self.parse_condition_ast()  # ExpressionNode
        return st.DeleteWhereStatement(table_name=table_name, where_expr=where_expr)

    @check_end_decorator
    def parse_select(self):
        self.match(TokenType.SELECT)

        distinct = False
        if self.current_token.token_type == TokenType.DISTINCT:
            distinct = True
            self.advance()

        columns = []
        while self.current_token.token_type != TokenType.FROM:
            columns.append(self.current_token.value)
            self.advance()
            if self.current_token.token_type == TokenType.COMMA:
                self.advance()
            elif self.current_token.token_type == TokenType.EOF:
                self.error("Unexpected EOF in SELECT columns")

        if len(columns) < 1:
            self.error("No columns given to select")

        self.match(TokenType.FROM)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        where_expr = None
        if self.current_token.token_type == TokenType.WHERE:
            self.advance()
            where_expr = self.parse_condition_ast()   # ExpressionNode

        order_by = None

        if self.current_token.token_type == TokenType.ORDER:
            self.advance()
            self.match(TokenType.BY)
            col_name = self.current_token.value
            self.advance()
            direction = 'ASC'
            if self.current_token.token_type == TokenType.IDENTIFIER:
                if self.current_token.value in ('ASC', 'DESC'):
                    direction = self.current_token.value
                    self.advance()
                else:
                    self.error("Direction can be either ASC or DESC!")
            order_by = OrderByItem(col_name, direction)

        return st.SelectStatement(
            columns=columns,
            table_name=table_name,
            distinct=distinct,
            where_expr=where_expr,
            order_by=order_by
        )

    def parse_condition_ast(self):
        """
            Build an ExpressionNode for the WHERE condition recursively.
        """
        return self.parse_or_expr()

    def parse_or_expr(self):
        node = self.parse_and_expr()
        while self.current_token.token_type == TokenType.OR:
            operator = self.current_token.value.upper()
            self.advance()
            right = self.parse_and_expr()
            node = BinaryOpNode(node, operator, right)
        self.validate_no_extra_tokens()
        return node

    def parse_and_expr(self):
        node = self.parse_not_expr()
        while self.current_token.token_type == TokenType.AND:
            operator = self.current_token.value.upper()
            self.advance()
            right = self.parse_not_expr()
            node = BinaryOpNode(node, operator, right)
        self.validate_no_extra_tokens()
        return node

    def parse_not_expr(self):
        if self.current_token.token_type == TokenType.NOT:
            self.advance()
            expr = self.parse_not_expr()
            return NotNode(expr)
        return self.parse_primary()

    def parse_primary(self):
        if self.current_token.token_type == TokenType.LPAREN:
            self.match(TokenType.LPAREN)
            node = self.parse_condition_ast()
            self.match(TokenType.RPAREN)
            return node
        else:
            return self.parse_comparison()

    def parse_comparison(self):
        left = self.parse_value()
        if self.current_token.token_type in [TokenType.EQ, TokenType.NEQ, TokenType.LT,
                                       TokenType.GT, TokenType.LEQ, TokenType.GEQ]:
            operator = self.current_token.value
            self.advance()
            right = self.parse_value()
            return BinaryOpNode(left, operator, right)
        self.error("Unexpected comparison!")

    def parse_value(self):
        curr_token = self.current_token

        if curr_token.token_type == TokenType.STRING:
            self.advance()
            return ValueNode(curr_token.value)
        elif curr_token.token_type == TokenType.DATE:
            self.advance()
            return ValueNode(Date.from_string(curr_token.value))
        elif curr_token.token_type == TokenType.NUMBER:
            self.advance()
            return ValueNode(int(curr_token.value))
        elif curr_token.token_type == TokenType.FLOAT:
            self.advance()
            return ValueNode(float(curr_token.value))
        elif curr_token.token_type == TokenType.IDENTIFIER:
            self.advance()
            return ValueNode(curr_token.value, is_column=True)
        else:
            self.error(f"Unexpected token in value: {curr_token}")

    def validate_no_extra_tokens(self):
        if self.current_token.token_type not in [
            TokenType.AND,
            TokenType.OR,
            TokenType.NOT,
            TokenType.LPAREN,
            TokenType.RPAREN,
            TokenType.EQ,
            TokenType.NEQ,
            TokenType.LT,
            TokenType.GT,
            TokenType.LEQ,
            TokenType.GEQ,
            TokenType.STRING,
            TokenType.NUMBER,
            TokenType.FLOAT,
            TokenType.IDENTIFIER,
            TokenType.EOF,
            TokenType.ORDER,
            TokenType.BY,
            TokenType.SECOL,
            TokenType.DATE,
        ]:
            self.error(f"Unexpected token in WHERE clause: {self.current_token}")

    @check_end_decorator
    def parse_defragment(self):
        self.match(TokenType.DEFRAGMENT)
        table_name_token = self.current_token
        self.match(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        return st.DefragmentTableStatement(table_name=table_name)
