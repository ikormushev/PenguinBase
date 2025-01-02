class BaseDatabaseError(Exception):
    def __init__(self, message):
        self.message = message


class TableError(BaseDatabaseError):
    ...


class ParseError(BaseDatabaseError):
    ...
