from typing import List


class HashNode:
    def __init__(self, key: str, value):
        self.key = key
        self.value = value
        self.next: HashNode | None = None


class HashTable:
    def __init__(self, pairs: list = None, size=10):
        self.table_items: List[HashNode | None] = [None] * size
        self.size = size

        if pairs:
            for key, value in pairs:
                self.insert(key, value)

    def _hash(self, key):
        hash_value = 0
        for char in key:
            hash_value += ord(char)
        return hash_value % self.size

    def __setitem__(self, key: str, value):
        index = self._hash(key)
        new_node = HashNode(key, value)

        if not self.table_items[index]:
            self.table_items[index] = new_node
        else:
            # Collision resolution with chaining
            current = self.table_items[index]

            if current.key == key:
                current.value = value
                return

            while current.next:
                current = current.next
                if current.key == key:
                    current.value = value
                    return
            current.next = new_node

    def __getitem__(self, key):
        index = self._hash(key)
        current = self.table_items[index]

        while current:
            if current.key == key:
                return current.value
            current = current.next

        return None
        # raise KeyError(f"Key '{key}' not found in HashTable.")

    def __delitem__(self, key):
        index = self._hash(key)
        current = self.table_items[index]
        prev = None

        while current:
            if current.key == key:
                if prev:
                    prev.next = current.next
                else:
                    self.table_items[index] = current.next
                return
            prev = current
            current = current.next

    def insert(self, key: str, value):
        self.__setitem__(key, value)

    def search(self, key):
        return self.__getitem__(key)

    def delete(self, key):
        self.__delitem__(key)

    def items(self):
        for bucket in self.table_items:
            current = bucket
            while current:
                yield current.key, current.value
                current = current.next

    def keys(self):
        for bucket in self.table_items:
            current = bucket
            while current:
                yield current.key
                current = current.next

    def __len__(self):
        return len([x for x in self.items()])

    def __repr__(self):
        items = [f"{key}: {value}" for key, value in self.items()]
        return "{" + ', '.join(items) + "}"
