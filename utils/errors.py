class BaseDatabaseError(Exception):
    def __init__(self, message):
        self.message = message


class InvalidCommandError(BaseDatabaseError):
    ...


class TableAlreadyExistsError(BaseDatabaseError):
    ...


class TableDoesNotExistError(BaseDatabaseError):
    ...


class MandatoryColumnError(BaseDatabaseError):
    ...


class OutOfRangeError(BaseDatabaseError):
    ...