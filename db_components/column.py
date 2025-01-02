from abc import ABC, abstractmethod

from data_structures.hash_table import HashTable
from utils.date import Date
from utils.errors import ParseError


class BaseValidator(ABC):
    DEFAULT_MAX = 0

    @abstractmethod
    def validate_value_type(self, value):
        pass

    @abstractmethod
    def validate_value_size(self, value, max_value):
        pass

    @abstractmethod
    def validate_max_size(self, value):
        pass

    def validate_default(self, value, max_value):
        if value is None:
            return value

        value = self.validate_value_type(value)
        if max_value is None:
            max_value = self.DEFAULT_MAX

        self.validate_value_size(value, max_value)

        return value

    def validate_primary_key(self, value):
        if value is None:
            return value

        if value == "True" or value == True:
            return True
        elif value == "False" or value == False:
            return False

        raise ValueError("Primary key can be either True or False!")


class NumberValidator(BaseValidator):
    DEFAULT_MAX = 2_147_483_647

    def validate_value_type(self, value):
        try:
            new_value = float(value)
            return new_value
        except Exception:
            raise ParseError("Unable to parse value!")

    def validate_value_size(self, value, max_value):
        if value > max_value:
            raise ValueError(f"Value cannot exceed {max_value}!")

    def validate_max_size(self, value):
        if value is None:
            return self.DEFAULT_MAX

        value = self.validate_value_type(value)
        self.validate_value_size(value, self.DEFAULT_MAX)

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

    def validate_max_size(self, value):
        if value is None:
            return self.DEFAULT_MAX

        return int(value)


class DateValidator(BaseValidator):
    DEFAULT_MAX = 10

    def validate_value_type(self, value):
        if not Date.is_valid_date_string(value):
            raise ValueError(f"Invalid value!")
        return Date.from_string(value)  # raises a ValueError by default if months and days do not match

    def validate_value_size(self, value, max_value):
        if len(value) != max_value:
            raise ValueError(f"Value has to be {max_value} characters!")

    def validate_max_size(self, value):
        if value is None:
            return self.DEFAULT_MAX

        value = int(value)
        if value == self.DEFAULT_MAX:
            return value

        raise ValueError(f"Cannot set a max size to 'date' type!")


class Column:
    COLUMN_TYPES = HashTable([("number", NumberValidator()),
                              ("string", StringValidator()),
                              ("date", DateValidator())])

    EVALUATION_ORDER = ["MAX_SIZE", "DEFAULT", "PRIMARY_KEY"]

    def __init__(self, column_name: str, column_type: str, given_constraints: HashTable):
        self.column_name = column_name
        self.column_type = column_type
        self.column_validator = self.COLUMN_TYPES.search(column_type)

        self.constraints = HashTable([
            ("DEFAULT", None),
            ("MAX_SIZE", self.column_validator.validate_max_size(None)),
            ("PRIMARY_KEY", False)])

        for constraint in self.EVALUATION_ORDER:
            if constraint in given_constraints.keys():
                self._set_constraint(constraint, given_constraints[constraint])

    def _set_constraint(self, constraint, value):
        validated_value = self._validate_constraint(constraint, value)
        self.constraints[constraint] = validated_value

    def _validate_constraint(self, constraint, value):
        validation_methods = HashTable([("DEFAULT", self.column_validator.validate_default),
                                        ("MAX_SIZE", self.column_validator.validate_max_size),
                                        ("PRIMARY_KEY", self.column_validator.validate_primary_key)])

        if constraint == "DEFAULT":
            return validation_methods[constraint](value, self.constraints["MAX_SIZE"])
        return validation_methods[constraint](value)

    def __str__(self):
        column_result = f"{self.column_name}|{self.column_type}"

        for constraint, value in self.constraints.items():
            if value:
                column_result += f"|{constraint}:{value}"

        return column_result

    @property
    def column_type(self):
        return self.__column_type

    @column_type.setter
    def column_type(self, value):
        if self.COLUMN_TYPES.search(value) is None:
            raise ValueError(f"{value} is not a valid column type!")

        self.__column_type = value

    @property
    def MAX_SIZE(self):
        return self.constraints["MAX_SIZE"]

    @property
    def DEFAULT(self):
        return self.constraints["DEFAULT"]

    @property
    def PRIMARY_KEY(self):
        return self.constraints["PRIMARY_KEY"]

    def validate_column_value(self, value):
        self.column_validator.validate_value_type(value)
        self.column_validator.validate_value_size(value, self.MAX_SIZE)
