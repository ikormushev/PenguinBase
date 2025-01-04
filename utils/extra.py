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


def polynomial_rolling_hash(data: bytes, base=257, mod=2**32):
    hash_val = 0
    for b in data:
        hash_val = (hash_val * base + b) % mod
    return hash_val
