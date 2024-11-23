def binary_insertion_sort(keys: list) -> list:
    """
    Perform in-place sorting of a list using Binary Insertion Sort.

    This function sorts the given list by incrementally building a sorted portion
    and inserting each element at its correct position using Binary Search.

    Sorting is stable.

    Complexity:
    - Best Case: O(n log n)
    - Worst Case: O(n^2)
    - Space: O(n)
    """

    for i in range(1, len(keys)):
        key_to_insert = keys[i]

        pos = binary_search(keys, key_to_insert, 0, i - 1)

        # Insert the element at the new position and skip the previous position
        keys = keys[:pos] + [key_to_insert] + keys[pos:i] + keys[i + 1:]
    return keys


def binary_search(keys: list, key_to_insert, low: int, high: int) -> int:
    """
    Perform Binary Search to find the position to insert an
    element in a sorted portion of a list.

    This function searches for the correct position of `key_to_insert` in the
    sorted sublist `keys[low:high+1]`.

    Complexity:
    - Time: O(log n)
    - Space: O(1)

    Returns:
    int: The index where `key_to_insert` should be inserted.

    """

    while low <= high:
        mid = (low + high) // 2

        if keys[mid] < key_to_insert:
            low = mid + 1
        else:
            high = mid - 1

    return low
