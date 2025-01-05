from data_structures.hash_table import HashTable
from query_parser_package.tokens import TokenType, Token
from utils.date import Date
from utils.string_utils import custom_isspace, custom_isalnum, custom_isdigit, custom_isalpha


class QueryTokenizer:
    """
        A QueryTokenizer that reads a statement character by character and produces a list of tokens.
    """
    KEYWORD_MAP = HashTable([('CREATE', TokenType.CREATE),
                             ('TABLE', TokenType.TABLE),
                             ('DROP', TokenType.DROP),
                             ('TABLEINFO', TokenType.TABLEINFO),
                             ('INSERT', TokenType.INSERT),
                             ('INTO', TokenType.INTO),
                             ('VALUES', TokenType.VALUES),
                             ('GET', TokenType.GET),
                             ('ROW', TokenType.ROW),
                             ('FROM', TokenType.FROM),
                             ('DELETE', TokenType.DELETE),
                             ('SELECT', TokenType.SELECT),
                             ('WHERE', TokenType.WHERE),
                             ('AND', TokenType.AND),
                             ('OR', TokenType.OR),
                             ('NOT', TokenType.NOT),
                             ('ORDER', TokenType.ORDER),
                             ('BY', TokenType.BY),
                             ('DISTINCT', TokenType.DISTINCT),
                             ('INDEX', TokenType.INDEX),
                             ('ON', TokenType.ON),
                             ('DEFAULT', TokenType.DEFAULT),
                             ('MAX_SIZE', TokenType.MAX_SIZE),
                             ('RANDOM', TokenType.RANDOM),
                             ('DEFRAGMENT', TokenType.DEFRAGMENT),
                             ])

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.current_char = self.text[self.pos] if self.text else None

    def advance(self):
        self.pos += 1
        if self.pos < len(self.text):
            self.current_char = self.text[self.pos]
        else:
            self.current_char = None

    def peek(self):
        peek_pos = self.pos + 1
        if peek_pos < len(self.text):
            return self.text[peek_pos]
        return None

    def skip_whitespace(self):
        while self.current_char and custom_isspace(self.current_char):
            self.advance()

    def collect_identifier_or_keyword(self):
        result = []

        while self.current_char and (custom_isalnum(self.current_char) or self.current_char == '_'):
            result.append(self.current_char)
            self.advance()

        value = ''.join(result)

        if self.KEYWORD_MAP[value]:
            return Token(self.KEYWORD_MAP[value], value)
        else:
            return Token(TokenType.IDENTIFIER, value)

    def collect_number(self):
        result = []
        has_decimal_point = False

        if self.current_char == "-":
            result.append("-")
            self.advance()

        while self.current_char and (custom_isdigit(self.current_char) or (self.current_char == '.')):
            if self.current_char == '.':
                if has_decimal_point:
                    result.append(self.current_char)
                    return Token(TokenType.UNKNOWN, ''.join(result))  # TODO - raise an error?
                has_decimal_point = True

            result.append(self.current_char)
            self.advance()

        if has_decimal_point:
            return Token(TokenType.FLOAT, ''.join(result))
        return Token(TokenType.NUMBER, ''.join(result))

    def collect_string_or_date(self, quote_char):
        self.advance()
        result = []

        while self.current_char and self.current_char != quote_char:
            result.append(self.current_char)
            self.advance()

        if self.current_char == quote_char:
            self.advance()

        string_result = ''.join(result)
        if Date.is_valid_date_string(string_result):
            return Token(TokenType.DATE, string_result)
        return Token(TokenType.STRING, string_result)

    def get_next_token(self):
        """
            Tokenize the input step by step.
        """
        self.skip_whitespace()

        if not self.current_char:
            return Token(TokenType.EOF, '')

        # Check for string or date (single/double quotes)
        if self.current_char in ['"', "'"]:
            return self.collect_string_or_date(self.current_char)

        # Operators and punctuation
        if self.current_char == ',':
            self.advance()
            return Token(TokenType.COMMA, ',')
        if self.current_char == '(':
            self.advance()
            return Token(TokenType.LPAREN, '(')
        if self.current_char == ')':
            self.advance()
            return Token(TokenType.RPAREN, ')')
        if self.current_char == ':':
            self.advance()
            return Token(TokenType.COLON, ':')
        if self.current_char == ';':
            self.advance()
            return Token(TokenType.SECOL, ';')

        # Operators: <=, >=, !=, <, >, =
        if self.current_char == '<':
            if self.peek() == '=':
                self.advance()
                self.advance()
                return Token(TokenType.LEQ, '<=')
            else:
                self.advance()
                return Token(TokenType.LT, '<')

        if self.current_char == '>':
            if self.peek() == '=':
                self.advance()
                self.advance()
                return Token(TokenType.GEQ, '>=')
            else:
                self.advance()
                return Token(TokenType.GT, '>')

        if self.current_char == '=':
            self.advance()
            return Token(TokenType.EQ, '=')

        if self.current_char == '!':
            if self.peek() == '=':  # TODO - raise an error if there is no '='?
                self.advance()
                self.advance()
                return Token(TokenType.NEQ, '!=')

        # Number or Float
        if custom_isdigit(self.current_char) or self.current_char == "-":
            return self.collect_number()

        # Identifier or keyword
        if custom_isalpha(self.current_char) or self.current_char == '_':
            return self.collect_identifier_or_keyword()

        current = self.current_char
        self.advance()
        return Token(TokenType.UNKNOWN, current)

    def tokenize(self):
        tokens = []
        while True:
            token = self.get_next_token()
            tokens.append(token)
            if token.token_type == TokenType.EOF:
                break
        return tokens

