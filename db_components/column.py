from abc import ABC, abstractmethod

from data_structures.hash_table import HashTable
from utils.validators import is_valid_date


class BaseValidator(ABC):
    @abstractmethod
    def validate_value_type(self, value):
        ...

    @abstractmethod
    def validate_value_size(self, value, max_value):
        ...

    @abstractmethod
    def validate_max_value(self, value):
        ...

    @abstractmethod
    def validate_default_value(self, value, max_value):
        pass

    def validate_primary_key(self, value):
        if value == "True" or value == True:
            return True
        elif value == "False" or value == False:
            return False

        raise ValueError("Primary key can be either True or False!")


class NumberValidator(BaseValidator):
    DEFAULT_MAX = 2_147_483_647

    def validate_value_type(self, value):
        return float(value)  # TODO - raising a value by default

    def validate_value_size(self, value, max_value):
        if value > max_value:
            raise ValueError(f"Value cannot exceed {max_value}!")

    def validate_max_value(self, value):
        if value is None:
            return self.DEFAULT_MAX

        value = self.validate_value_type(value)
        self.validate_value_size(value, self.DEFAULT_MAX)

        return value

    def validate_default_value(self, value, max_value):
        if value is None:
            return value

        value = self.validate_value_type(value)
        self.validate_value_size(value, max_value)

        return value


class StringValidator(BaseValidator):
    DEFAULT_MAX = 255

    def validate_value_type(self, value):
        if not isinstance(value, str) or not value.strip():  # TODO - recreate .strip() ?
            raise ValueError(f"Value cannot be empty!")
        return value

    def validate_value_size(self, value, max_value):
        if len(value) > max_value:
            raise ValueError(f"Value has to be less than {max_value} characters!")

    def validate_max_value(self, value):
        if value is None:
            return self.DEFAULT_MAX

        return int(value)

    def validate_default_value(self, value, max_value):
        if value is None:
            return value

        value = self.validate_value_type(value)
        self.validate_value_size(value, max_value)
        return value


class DateValidator(BaseValidator):
    DEFAULT_MAX = 10

    def validate_value_type(self, value):
        if not is_valid_date(value):
            raise ValueError(f"Invalid value!")
        return value

    def validate_value_size(self, value, max_value):
        if len(value) != max_value:
            raise ValueError(f"Value has to be {max_value} characters!")

    def validate_max_value(self, value):
        if value is None:
            return self.DEFAULT_MAX

        value = int(value)
        if value == self.DEFAULT_MAX:
            return value

        raise ValueError(f"Cannot set a max size to 'date' type!")

    def validate_default_value(self, value, max_value):
        if value is None:
            return value

        value = self.validate_value_type(value)

        # TODO - check not really needed, since is_valid_date already checks the size
        self.validate_value_size(value, max_value)

        return value


class Column:
    COLUMN_TYPES = HashTable([("number", NumberValidator()),
                              ("string", StringValidator()),
                              ("date", DateValidator())])

    def __init__(self, column_name: str, column_type: str, max_value=None, default_value=None, is_primary_key=False):
        self.column_name = column_name
        self.column_type = column_type
        self.column_validator = self.COLUMN_TYPES.search(column_type)

        self.is_primary_key = is_primary_key
        self.max_value = max_value
        self.default_value = default_value

    def __str__(self):
        return (f"{self.column_name}|"
                f"{self.column_type}|"
                f"MAX_SIZE:{self.max_value}|"
                f"DEFAULT:{self.default_value if self.default_value else ''}|"
                f"PRIMARY_KEY:{self.is_primary_key}")

    @property
    def column_type(self):
        return self.__column_type

    @column_type.setter
    def column_type(self, value):
        if self.COLUMN_TYPES.search(value) is None:
            raise ValueError(f"{value} is not a valid column type!")

        self.__column_type = value

    @property
    def max_value(self):
        return self.__max_value

    @max_value.setter
    def max_value(self, value):
        self.__max_value = self.column_validator.validate_max_value(value)

    @property
    def default_value(self):
        return self.__default_value

    @default_value.setter
    def default_value(self, value):
        self.__default_value = self.column_validator.validate_default_value(value, self.max_value)

    @property
    def is_primary_key(self):
        return self.__is_primary_key

    @is_primary_key.setter
    def is_primary_key(self, value):
        self.__is_primary_key = self.column_validator.validate_primary_key(value)

    def validate_column_value(self, value):
        self.column_validator.validate_value_type(value)
        self.column_validator.validate_value_size(value, self.max_value)
