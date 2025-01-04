import random


class Date:
    def __init__(self, day: int, month: int, year: int):
        if not (1 <= month <= 12):
            raise ValueError("Month must be between 1 and 12.")

        if not (1 <= day <= self.days_in_month(month, year)):
            raise ValueError(f"Day must be between 1 and {self.days_in_month(month, year)} for month {month}.")

        self.day = day
        self.month = month
        self.year = year

    @staticmethod
    def is_leap_year(year: int) -> bool:
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

    @staticmethod
    def days_in_month(month: int, year: int) -> int:
        if month in (1, 3, 5, 7, 8, 10, 12):
            return 31
        elif month in (4, 6, 9, 11):
            return 30
        elif month == 2:
            return 29 if Date.is_leap_year(year) else 28
        else:
            raise ValueError("Invalid month")

    @classmethod
    def from_string(cls, date_str: str):
        if not cls.is_valid_date_string(date_str):
            raise ValueError("Date must be in the format 'DD.MM.YYYY'")

        parts = date_str.split('.')  # TODO - recreate .split()

        day = int(parts[0])
        month = int(parts[1])
        year = int(parts[2])
        return cls(day, month, year)

    @staticmethod
    def is_valid_date_string(date_str):
        parts = date_str.split('.')
        if len(parts) != 3:
            return False

        if len(parts[0]) != 2 or len(parts[1]) != 2 or len(parts[2]) != 4:
            return False

        try:
            for i in range(len(parts)):
                part = int(parts[i])
            return True
        except ValueError:
            return False

    @staticmethod
    def generate_random_date_string():
        year = random.randint(1900, 2100)
        month = random.randint(1, 12)
        day = random.randint(1, Date.days_in_month(month, year))
        return f"{day:02}.{month:02}.{year}"

    def __repr__(self):
        return f"{self.day:02}.{self.month:02}.{self.year:04}"

    def __eq__(self, other):
        if not isinstance(other, Date):
            raise TypeError(f"'==' not supported between instances of '{type(self).__name__}' and '{type(other).__name__}'")
        return self.day == other.day and self.month == other.month and self.year == other.year

    def __lt__(self, other):
        if not isinstance(other, Date):
            raise TypeError(f"'<' not supported between instances of '{type(self).__name__}' and '{type(other).__name__}'")
        return (self.year, self.month, self.day) < (other.year, other.month, other.day)

    def __le__(self, other):
        return self == other or self < other

    def __len__(self):
        return len(str(self))
