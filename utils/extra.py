from utils.binary_insertion_sort import binary_insertion_sort


def format_size(size_in_bytes):
    for unit in ['bytes', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024


def reverse_array(arr: list) -> list:
    new_list = []
    for i in range(len(arr) - 1, -1, -1):
        new_list.append(arr[i])
    return new_list


def polynomial_rolling_hash(data: bytes, base=257, mod=2 ** 32):
    hash_val = 0
    for b in data:
        hash_val = (hash_val * base + b) % mod
    return hash_val


def incremental_generator_sort(gen, buffer_size=1000):
    buffer = []
    for item in gen:
        buffer.append(item)
        if len(buffer) >= buffer_size:
            buffer = binary_insertion_sort(buffer)
            for sorted_item in buffer:
                yield sorted_item
            buffer.clear()

    if buffer:
        buffer = binary_insertion_sort(buffer)
        for sorted_item in buffer:
            yield sorted_item


def intersect_offsets(genA, genB):
    a_iter = iter(genA)
    b_iter = iter(genB)
    a_val = next(a_iter, None)
    b_val = next(b_iter, None)
    while a_val is not None and b_val is not None:
        if a_val == b_val:
            yield a_val
            a_val = next(a_iter, None)
            b_val = next(b_iter, None)
        elif a_val < b_val:
            a_val = next(a_iter, None)
        else:
            b_val = next(b_iter, None)


def union_offsets(genA, genB):
    a_iter = iter(genA)
    b_iter = iter(genB)
    a_val = next(a_iter, None)
    b_val = next(b_iter, None)
    last_yielded = None

    while a_val is not None or b_val is not None:
        if b_val is None or (a_val is not None and a_val < b_val):
            if last_yielded != a_val:
                yield a_val
                last_yielded = a_val
            a_val = next(a_iter, None)
        elif a_val is None or (b_val is not None and b_val < a_val):
            if last_yielded != b_val:
                yield b_val
                last_yielded = b_val
            b_val = next(b_iter, None)
        else:
            if last_yielded != a_val:
                yield a_val
                last_yielded = a_val
            a_val = next(a_iter, None)
            b_val = next(b_iter, None)


def difference_offsets(genAll, genSub):
    """
        Return offsets in genAll that are NOT in genSub.
        Both are ascending sets.
    """
    all_iter = iter(genAll)
    sub_iter = iter(genSub)
    a_val = next(all_iter, None)
    s_val = next(sub_iter, None)
    while a_val is not None:
        if s_val is None:
            yield a_val
            a_val = next(all_iter, None)
        elif a_val == s_val:
            a_val = next(all_iter, None)
            s_val = next(sub_iter, None)
        elif a_val < s_val:
            yield a_val
            a_val = next(all_iter, None)
        else:
            s_val = next(sub_iter, None)


def union_unsorted(genA, genB):
    sortedA = incremental_generator_sort(genA)
    sortedB = incremental_generator_sort(genB)
    yield from union_offsets(sortedA, sortedB)


def intersect_unsorted(genA, genB):
    sortedA = incremental_generator_sort(genA)
    sortedB = incremental_generator_sort(genB)
    yield from intersect_offsets(sortedA, sortedB)


def difference_unsorted(genAll, genSub):
    sortedAll = incremental_generator_sort(genAll)
    sortedSub = incremental_generator_sort(genSub)
    yield from difference_offsets(sortedAll, sortedSub)


def just_in_case_date_string(date) -> str:
    day_str = str(date.day)
    if len(day_str) < 2:
        day_str = "0" + day_str

    month_str = str(date.month)
    if len(month_str) < 2:
        month_str = "0" + month_str

    year_str = str(date.year)
    while len(year_str) < 4:
        year_str = "0" + year_str

    return day_str + "." + month_str + "." + year_str
