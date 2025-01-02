def is_valid_number(value):
    try:
        value = float(value)
        return True
    except ValueError:
        return False
