class Column:
    COLUMN_TYPES = ["number", "string", "date"]

    def __init__(self, column_name: str, column_type: str, default_value=None, is_primary_key=False):
        self.column_name = column_name
        self.column_type = column_type
        self.default_value = default_value
        self.is_primary_key = is_primary_key

    def __str__(self):
        return f"{self.column_name}:{self.column_type}|DEFAULT:{self.default_value}|PRIMARY_KEY:{self.is_primary_key}"

    @property
    def column_type(self):
        return self.__column_type

    @column_type.setter
    def column_type(self, value):
        is_value_valid = False
        for col_type in self.COLUMN_TYPES:
            if col_type == value:
                is_value_valid = True
                break

        if not is_value_valid:
            raise ValueError(f"{value} is not a valid column type!")
        self.__column_type = value
