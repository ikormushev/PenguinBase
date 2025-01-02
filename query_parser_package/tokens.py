class TokenType:
    # General
    EOF = 'EOF'
    IDENTIFIER = 'IDENTIFIER'
    STRING = 'STRING'
    NUMBER = 'NUMBER'
    FLOAT = 'FLOAT'
    DATE = 'DATE'

    # Operators / punctuation
    COMMA = 'COMMA'  # ,
    LPAREN = 'LPAREN'  # (
    RPAREN = 'RPAREN'  # )
    COLON = 'COLON'  # :
    SECOL = 'SECOL'  # ;
    EQ = 'EQ'  # =
    LT = 'LT'  # <
    GT = 'GT'  # >
    NEQ = 'NEQ'  # !=
    LEQ = 'LEQ'  # <=
    GEQ = 'GEQ'  # >=
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'

    # Keywords
    CREATE = 'CREATE'
    TABLE = 'TABLE'
    DROP = 'DROP'
    TABLEINFO = 'TABLEINFO'
    INSERT = 'INSERT'
    INTO = 'INTO'
    VALUES = 'VALUES'
    GET = 'GET'
    ROW = 'ROW'
    FROM = 'FROM'
    DELETE = 'DELETE'
    SELECT = 'SELECT'
    WHERE = 'WHERE'
    ORDER = 'ORDER'
    BY = 'BY'
    DISTINCT = 'DISTINCT'
    INDEX = 'INDEX'
    ON = 'ON'
    RANDOM = 'RANDOM'
    DEFRAGMENT = 'DEFRAGMENT'

    # Constraints
    DEFAULT = 'DEFAULT'
    MAX_SIZE = 'MAX_SIZE'
    PRIMARY_KEY = 'PRIMARY_KEY'

    # Catch-all unknown
    UNKNOWN = 'UNKNOWN'


class Token:
    def __init__(self, token_type: TokenType, value):
        self.token_type = token_type
        self.value = value

    def __repr__(self):
        return f"Token({self.token_type}, {self.value})"
