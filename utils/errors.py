class BaseDatabaseError(Exception):
    def __init__(self, message):
        self.message = message


class InvalidCommandError(BaseDatabaseError):
    ...


class TableError(BaseDatabaseError):
    ...


class ColumnError(BaseDatabaseError):
    ...
