def custom_isspace(ch: str) -> bool:
    whitespace_chars = [' ', '\t', '\n', '\r', '\f', '\v']
    for w in whitespace_chars:
        if ch == w:
            return True
    return False


def custom_isalpha(ch: str) -> bool:
    if ('A' <= ch <= 'Z') or ('a' <= ch <= 'z'):
        return True
    return False


def custom_isdigit(ch: str) -> bool:
    if '0' <= ch <= '9':
        return True
    return False


def custom_isalnum(ch: str) -> bool:
    return custom_isalpha(ch) or custom_isdigit(ch)


def custom_lstrip(s: str, strip_chars: str = None) -> str:
    start = 0
    end = len(s) - 1

    while start <= end:
        ch = s[start]
        if strip_chars is None:
            if custom_isspace(ch):
                start += 1
            else:
                break
        else:
            if ch in strip_chars:
                start += 1
            else:
                break

    result_chars = []
    i = start
    while i <= end:
        result_chars.append(s[i])
        i += 1

    return ''.join(result_chars)


def custom_rstrip(s: str, strip_chars: str = None) -> str:
    start = 0
    end = len(s) - 1

    while end >= start:
        ch = s[end]
        if strip_chars is None:
            if custom_isspace(ch):
                end -= 1
            else:
                break
        else:
            if ch in strip_chars:
                end -= 1
            else:
                break

    result_chars = []
    i = start
    while i <= end:
        result_chars.append(s[i])
        i += 1

    return ''.join(result_chars)


def custom_strip(s: str, strip_chars: str = None) -> str:
    left_stripped = custom_lstrip(s, strip_chars)
    right_stripped = custom_rstrip(left_stripped, strip_chars)
    return right_stripped


def custom_split(s: str, delimiter: str = None) -> list:
    """
        If delimiter is None, it splits by whitespace AND strips the string.
        The built-in .split() works the same way - weird :D.
    """
    result = []
    current_token = []

    if delimiter is None:
        i = 0
        length = len(s)

        while i < length:
            ch = s[i]
            if custom_isspace(ch):
                if len(current_token) > 0:
                    result.append(''.join(current_token))
                    current_token = []

                i += 1
                continue
            else:
                current_token.append(ch)
            i += 1

        if len(current_token) > 0:
            result.append(''.join(current_token))
        return result
    else:
        if len(delimiter) == 0:
            raise ValueError("Empty separator")

        i = 0
        length = len(s)
        while i < length:
            ch = s[i]
            if ch == delimiter:
                result.append(''.join(current_token))
                current_token = []
            else:
                current_token.append(ch)
            i += 1

        result.append(''.join(current_token))
        return result


def custom_upper(s: str) -> str:
    result_chars = []

    for ch in s:
        if 'a' <= ch <= 'z':
            offset = ord(ch) - ord('a')
            upper_ch = chr(ord('A') + offset)
            result_chars.append(upper_ch)
        else:
            result_chars.append(ch)

    return ''.join(result_chars)


def custom_lower(s: str) -> str:
    result_chars = []
    for ch in s:
        if 'A' <= ch <= 'Z':
            offset = ord(ch) - ord('A')
            lower_ch = chr(ord('a') + offset)
            result_chars.append(lower_ch)
        else:
            result_chars.append(ch)

    return ''.join(result_chars)


def custom_startswith(s: str, prefix: str) -> bool:
    if len(prefix) > len(s):
        return False

    i = 0
    while i < len(prefix):
        if s[i] != prefix[i]:
            return False
        i += 1
    return True


def custom_endswith(s: str, suffix: str) -> bool:
    if len(suffix) > len(s):
        return False

    start_idx = len(s) - len(suffix)
    i = 0
    while i < len(suffix):
        if s[start_idx + i] != suffix[i]:
            return False
        i += 1
    return True
