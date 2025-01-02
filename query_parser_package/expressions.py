from abc import ABC, abstractmethod
from utils.errors import ParseError


class ExpressionNode(ABC):
    """
        Base class for expression nodes.
    """

    @abstractmethod
    def evaluate_expression(self, row):
        ...


class BinaryOpNode(ExpressionNode):
    """
        A node for binary operations.
    """
    def __init__(self, left: ExpressionNode, operator: str, right: ExpressionNode):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f"BinaryOpNode({self.left}, op={self.operator}, {self.right})"

    def evaluate_expression(self, row):
        left = self.left.evaluate_expression(row)
        right = self.right.evaluate_expression(row)

        try:
            if self.operator == "=":
                return left == right
            elif self.operator == "!=":
                return left != right
            elif self.operator == "<":
                return left < right
            elif self.operator == "<=":
                return left <= right
            elif self.operator == ">":
                return left > right
            elif self.operator == ">=":
                return left >= right

            if self.operator == "AND":
                return left and right
            elif self.operator == "OR":
                return left or right
        except TypeError:
            raise ParseError("Comparsion not valid!")


class NotNode(ExpressionNode):
    """
        A node that represents 'NOT <expr>'.
    """
    def __init__(self, expr: ExpressionNode):
        self.expr = expr

    def __repr__(self):
        return f"NotNode({self.expr})"

    def evaluate_expression(self, row):
        result = not self.expr.evaluate_expression(row)
        return result


class ValueNode(ExpressionNode):
    """
        A leaf node, representing a single value:
          - a column name,
          - a string literal,
          - a numeric literal
          - a date literal.
    """
    def __init__(self, value, is_column=False):
        self.value = value
        self.is_column = is_column

    def __repr__(self):
        return f"ValueNode(value={self.value}, column={self.is_column})"

    def evaluate_expression(self, row):
        if self.is_column:
            value = row[self.value]
            return value
        else:
            return self.value
