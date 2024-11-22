def is_valid_number(value):
    try:
        value = float(value)
        return True
    except ValueError:
        return False

# TODO - better date validation
def is_valid_date(value):
    def is_leap_year(given_year):
        return (given_year % 4 == 0 and given_year % 100 != 0) or (given_year % 400 == 0)

    try:
        date_parts = value.split(".")  # TODO - create a custom .split()

        if len(date_parts) != 3:
            return False

        day, month, year = date_parts
        if len(day) != 2 or len(month) != 2 or len(year) != 4:
            return False

        day = int(day)
        month = int(month)
        year = int(year)

        if not (1 <= month <= 12):
            return False

        if year < 0:
            return False

        if not (1 <= day <= 31):
            return False

        # if month in [1, 3, 5, 7, 8, 10, 12]:
        #     if not (1 <= day <= 31):
        #         return False
        # elif month in [4, 6, 9, 11]:
        #     if not (1 <= day <= 30):
        #         return False
        # elif month == 2:
        #     if is_leap_year(year):
        #         if not (1 <= day <= 29):
        #             return False
        #     else:
        #         if not (1 <= day <= 28):
        #             return False

        return True
    except ValueError:
        return False
