def get_substring_from_string(string, start, end):
    new_string = ""

    for i in range(start, end):
        new_string += string[i]

    return new_string


def custom_split(text, separator=""):
    split_string = []
    word = ""
    index = 0
    separator_length = len(separator)
    text_length = len(text)

    while index < text_length:
        if index + separator_length >= text_length:
            word += text[index]
            index += 1
        else:
            if get_substring_from_string(text, index, index+separator_length) == separator:
                if word:
                    split_string.append(word)
                    word = ""

                index += separator_length
            else:
                word += text[index]
                index += 1

    if word:
        split_string.append(word)

    return split_string

