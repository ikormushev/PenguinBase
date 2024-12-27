from datetime import datetime
import struct
from typing import List

from data_structures.hash_table import HashTable
from utils.binary_insertion_sort import binary_insertion_sort
from utils.validators import is_valid_date


class NodeManager:
    def __init__(self, file_path: str):
        self.file_path = file_path

        with open(self.file_path, "rb+") as file:
            file.seek(0)
            header_bytes = file.read(struct.calcsize("iqq1si"))
            self.t, self.root_offset, self.eof, key_type, self.key_max_size = struct.unpack("iqq1si",
                                                                                                 header_bytes)

            self.key_type = key_type.decode()

    def update_header(self):
        with open(self.file_path, "rb+") as file:
            header_data = struct.pack("iqq1si", self.t, self.root_offset, self.eof, self.key_type.encode(), self.key_max_size)
            file.seek(0)
            file.write(header_data)
            file.flush()

    @staticmethod
    def create_node_manager(file_path, t, key_type, key_max_size):
        with open(file_path, "wb+") as file:
            header_bytes_size = struct.calcsize("iqq1si")
            header_data = struct.pack("iqq1si", t, header_bytes_size, header_bytes_size, key_type.encode(), key_max_size)
            file.seek(0)
            file.write(header_data)
            file.flush()

        return NodeManager(file_path)

    def save_node(self, offset: int | None, node_data: bytes) -> int:
        with open(self.file_path, "rb+") as file:
            if offset is None:
                offset = self.eof
            file.seek(offset)
            file.write(node_data)
            self.eof = max(self.eof, offset + len(node_data))
            file.flush()

        self.update_header()

        return offset

    def load_node(self, offset: int) -> bytes:
        with open(self.file_path, 'rb') as file:
            file.seek(offset)
            node_size_bytes = struct.calcsize("i")
            node_size = file.read(node_size_bytes)
            length = struct.unpack("i", node_size)[0]
            data = file.read(length)
            if len(data) < length:
                raise IOError("Not enough data read from file.")
        return data


class PointerListManager:
    def __init__(self, file_path):
        self.file_path = file_path

        with open(self.file_path, "rb+") as file:
            file.seek(0)
            header_bytes = file.read(struct.calcsize("qq"))
            self.free_slot, self.eof = struct.unpack("qq", header_bytes)

    @staticmethod
    def create_pointer_list_manager(file_path):
        with open(file_path, "w+b") as file:
            file.seek(0)
            header_bytes = struct.calcsize("qq")
            header_data = struct.pack("qq", header_bytes, header_bytes)
            file.write(header_data)
            file.flush()

        return PointerListManager(file_path)

    def update_header(self, file):
        header_data = struct.pack("qq", self.free_slot, self.eof)
        file.seek(0)
        file.write(header_data)

    def write_pointer(self, file, position: int, pointer_data: bytes):
        file.seek(position)
        file.write(pointer_data)

    def read_pointer(self, file, position: int):
        file.seek(position)
        return file.read(struct.calcsize("qqq"))

    def create_pointer_list(self, pointer: int):
        pointer_data = struct.pack("qqq", -1, pointer, -1)
        position = -1
        with open(self.file_path, "rb+") as file:
            self.write_pointer(file, self.free_slot, pointer_data)
            position = self.free_slot

            # TODO - fix when delete is available
            self.eof += struct.calcsize("qqq")
            self.free_slot = self.eof
            self.update_header(file)

            file.flush()

        return position

    def add_pointer_to_pointer_list(self, start_pointer: int, new_pointer: int):
        position = start_pointer

        with open(self.file_path, "rb+") as file:
            while position != -1:
                pointer_data = self.read_pointer(file, position)
                prev, current, next_ptr = struct.unpack("qqq", pointer_data)

                if next_ptr == -1:
                    new_node = struct.pack("qqq", position, new_pointer, -1)
                    self.write_pointer(file, self.free_slot, new_node)

                    prev_node = struct.pack("qqq", prev, current, self.free_slot)
                    self.write_pointer(file, position, prev_node)

                    # TODO - fix when delete is available
                    self.eof += struct.calcsize("qqq")
                    self.free_slot = self.eof
                    self.update_header(file)

                position = next_ptr
            file.flush()

    # def delete_pointer_from_pointer_list(self, start_pointer: int, pointer_to_delete: int):
    #     position = start_pointer
    #     new_start_pointer = -1
    #
    #     with open(self.file_path, "rb+") as file:
    #         while position != -1:
    #             file.seek(position)
    #             data = file.read(struct.calcsize("qqq"))
    #             prev, current, next_ptr = struct.unpack("qqq", data)
    #
    #             if current == pointer_to_delete:
    #                 if prev != -1:
    #                     previoous_pointer_data = self.read_pointer(file, prev)
    #                     prev_prev, prev_curr, prev_next = struct.unpack("qqq", previoous_pointer_data)
    #                     previous_pointer_new_data = struct.pack("qqq", prev_prev, prev_curr, next_ptr)
    #                     self.write_pointer(file, prev, previous_pointer_new_data)
    #
    #                 if next_ptr != -1:
    #                     next_pointer_data = self.read_pointer(file, next_ptr)
    #                     next_prev, next_curr, next_next = struct.unpack("qqq", next_pointer_data)
    #                     new_start_pointer = next_ptr
    #                     next_pointer_new_data = struct.pack("qqq", prev, prev_curr, next_next)
    #                     self.write_pointer(file, next_ptr, next_pointer_new_data)
    #
    #                 self.free_slot = position
    #                 break
    #
    #             position = next_ptr
    #
    #     if start_pointer == pointer_to_delete:
    #         return new_start_pointer

    def traverse_pointer_list(self, start_pointer: int):
        position = start_pointer
        pointers = []

        with open(self.file_path, "rb") as file:
            while position != -1:
                curr_pointer_data = self.read_pointer(file, position)
                prev_p, curr_p, next_p = struct.unpack("qqq", curr_pointer_data)
                pointers.append(curr_p)
                position = next_p
        return pointers


class BTreeNodeKey:
    def __init__(self, key, pointers: List[int], key_max_size: int | None = None):
        self.key = key
        self.pointers = pointers
        self.key_max_size = key_max_size

    @property
    def _key_type(self):
        k_type = None

        if isinstance(self.key, int):
            k_type = "I"
        elif isinstance(self.key, float):
            k_type = "F"
        elif isinstance(self.key, str) and is_valid_date(self.key):
            k_type = "D"
        elif isinstance(self.key, str):
            k_type = "S"

        return k_type

    @staticmethod
    def key_size(key_type, key_max_size=0):
        if not key_type:
            raise ValueError(f"Unsupported key type: {key_type}")

        k_size = 1
        if key_type == "N" or key_type == "I" or key_type == "F":
            k_size += 8
        elif key_type == "D":
            k_size += struct.calcsize("10s")
        elif key_type == "S":
            k_size += struct.calcsize("i") + key_max_size

        k_size += struct.calcsize("qq")

        return k_size

    def serialize_key(self) -> bytes:
        key_type = self._key_type
        if not key_type:
            raise ValueError(f"Unsupported key type: {key_type}")

        key_data = b""

        if key_type == "I":
            key_data = b"I" + struct.pack("q", self.key)  # TODO - later change to long long - q
        elif key_type == "F":
            key_data = b"F" + struct.pack("d", self.key)  # TODO - later change to double - d
        elif key_type == "D":
            key_data = b"D" + self.key.encode()
        elif key_type == "S":
            encoded_key = self.key.encode()
            if len(encoded_key) < self.key_max_size:
                encoded_key += b'\x00' * (self.key_max_size - len(encoded_key))
            length = struct.pack("i", self.key_max_size)
            key_data = b"S" + length + encoded_key

        pointers_data = b""

        real_pointer = self.pointers[0]
        pointers_data += struct.pack("q", real_pointer)

        list_pointer = self.pointers[1]
        pointers_data += struct.pack("q", list_pointer)

        return key_data + pointers_data

    @staticmethod
    def deserialize_key(key_data):
        offset = 0

        key_type = key_data[offset:offset + 1]
        offset += 1
        key_max_size = None

        if key_type == b"I":
            key = struct.unpack("q", key_data[offset:offset + 8])[0]  # TODO - later change to long long - q
            offset += 8
        elif key_type == b"F":
            key = struct.unpack("d", key_data[offset:offset + 8])[0]  # TODO - later change to double - d
            offset += 8
        elif key_type == b"D":
            value_bytes = key_data[offset:offset + 10]
            offset += 10
            key = value_bytes.decode()
        elif key_type == b"S":
            length = struct.unpack("i", key_data[offset:offset + 4])[0]
            key_max_size = length
            offset += 4

            value_bytes = key_data[offset:offset + length]
            offset += length

            key = value_bytes.decode().strip("\x00")  # TODO - recreate .strip()
        else:
            raise ValueError("Unsupported key type")

        real_pointer = struct.unpack_from("q", key_data[offset: offset + 8])[0]
        offset += 8
        list_pointer = struct.unpack_from("q", key_data[offset: offset + 8])[0]
        pointers = [real_pointer, list_pointer]

        return BTreeNodeKey(key, pointers, key_max_size)

    def _compare(self, other):
        if not isinstance(other, BTreeNodeKey):
            raise TypeError("Can only compare BTreeNodeKey instances")

        if isinstance(self.key, (int, float)) and isinstance(other.key, (int, float)):
            return (self.key > other.key) - (self.key < other.key)
        elif isinstance(self.key, str) and is_valid_date(self.key) and is_valid_date(other.key):
            date_self = datetime.strptime(self.key, "%d.%m.%Y")
            date_other = datetime.strptime(other.key, "%d.%m.%Y")
            return (date_self > date_other) - (date_self < date_other)
        elif isinstance(self.key, str) and isinstance(other.key, str):
            return (self.key > other.key) - (self.key < other.key)
        else:
            raise TypeError("Unsupported comparison for key types")

    def __eq__(self, other):
        if not isinstance(other, BTreeNodeKey):
            return False
        return self._compare(other) == 0

    def __lt__(self, other):
        return self._compare(other) < 0

    def __repr__(self):
        return f"{self.key}: {self.pointers}"


class BTreeNode:
    def __init__(self, t: int, offset: int | None = None, is_leaf: bool = True, keys=None, children=None):
        """
        Args:
            offset (int | None): Where the node is located in a file.
            is_leaf (bool): Whether this node is a leaf node - the bottom-most node of the B-tree.
        """
        self.t = t
        self.offset = offset
        self.is_leaf = is_leaf
        self.keys: List[BTreeNodeKey] = keys if keys is not None else []
        self.children: List[int] = children if children is not None else []

    @property
    def max_keys(self):
        """
        In a B-tree, the maximum number of keys a node can have is ((2 * t) - 1).
        """
        return 2 * self.t - 1

    @property
    def max_children(self):
        """
        In a B-tree, the maximum number of children per node ca be (2 * t).
        """
        return 2 * self.t

    def sort_node_keys(self):
        """
        Sort the keys in the current B-tree node.

        B-tree requires the nodes to be sorted in ascending order.
        Here, sorting is performed by Binary Insertion Sort
        because the number of keys in a node is typically small.
        """
        self.keys = binary_insertion_sort(self.keys)

    def is_full(self):
        return len(self.keys) == self.max_keys

    def find_key_index(self, key):
        idx = 0
        while idx < len(self.keys) and key > self.keys[idx].key:
            idx += 1
        return idx

    def serialize_node(self, key_type, key_max_size) -> bytes:
        key_base_length = BTreeNodeKey.key_size(key_type, key_max_size)
        metadata = struct.pack("=?ii", self.is_leaf, len(self.keys), len(self.children))

        keys_data = b""
        for i in range(self.max_keys):
            if i < len(self.keys):
                keys_data += self.keys[i].serialize_key()
            else:
                keys_data += b'\x00' * key_base_length

        children_data = b""
        for c in range(self.max_children):
            if c < len(self.children):
                child_off = self.children[c]
                children_data += struct.pack("q", child_off)
            else:
                children_data += struct.pack("q", -1)

        node_data = metadata + keys_data + children_data
        node_size = len(node_data)
        header = struct.pack("i", node_size)
        return header + node_data

    @staticmethod
    def deserialize_node(node_data: bytes, node_offset: int, t: int, key_type, key_max_size):
        key_base_length = BTreeNodeKey.key_size(key_type, key_max_size)
        offset = 0

        is_leaf = struct.unpack("?", node_data[offset:offset + 1])[0]
        offset += 1

        metadata_size = struct.calcsize("ii")
        keys_num, children_num = struct.unpack_from("ii", node_data[offset:offset + metadata_size])
        offset += metadata_size

        keys = []

        for _ in range(keys_num):
            key_data = node_data[offset:offset + key_base_length]
            key = BTreeNodeKey.deserialize_key(key_data)
            keys.append(key)
            offset += key_base_length

        offset += ((t * 2 - 1) - keys_num) * key_base_length

        children = []

        for i in range(children_num):
            child_off = struct.unpack_from("q", node_data[offset:offset + 8])[0]
            if child_off != -1:
                children.append(child_off)
            offset += 8

        return BTreeNode(is_leaf=is_leaf,
                         keys=keys,
                         children=children,
                         t=t,
                         offset=node_offset)


class BTree:
    """
    The B-tree is a self-balancing search tree that maintains sorted data.

    Attributes:
        t (int): The minimum degree of the B-tree.
            Determines the maximum number of children and keys in a node:
            - Maximum keys per node: 2 * t - 1
            - Minimum keys per node (except root): t - 1
            - Maximum children per node: 2 * t
        root (BTreeNode): The root node of the B-tree.
    """

    def __init__(self, node_file_path, pointer_file_path):
        self.manager = NodeManager(node_file_path)
        self.pointer_manager = PointerListManager(pointer_file_path)

    @staticmethod
    def create_tree(t, key_type, key_max_size, node_file_path, pointer_file_path):
        root = BTreeNode(t=t)
        root_bytes = root.serialize_node(key_type, key_max_size)
        manager = NodeManager.create_node_manager(node_file_path, t, key_type, key_max_size)
        pointer_manager = PointerListManager.create_pointer_list_manager(pointer_file_path)
        root_offset = manager.save_node(None, root_bytes)

        return BTree(node_file_path, pointer_file_path)

    def _load_node(self, offset: int) -> BTreeNode:
        node_bytes = self.manager.load_node(offset)
        node_data = BTreeNode.deserialize_node(node_bytes,
                                               offset,
                                               self.manager.t,
                                               self.manager.key_type,
                                               self.manager.key_max_size)
        return node_data

    def _save_node(self, node: BTreeNode) -> int:
        node_data = node.serialize_node(self.manager.key_type, self.manager.key_max_size)
        new_offset = self.manager.save_node(node.offset, node_data)
        node.offset = new_offset
        return node.offset

    @property
    def root(self) -> BTreeNode:
        return self._load_node(self.manager.root_offset)

    def search(self, key) -> HashTable | None:
        return self._search(self.manager.root_offset, key)

    def _search(self, node_offset: int, key) -> HashTable | None:
        i = 0
        node = self._load_node(node_offset)

        # Find the first key greater than or equal to k
        while i < len(node.keys) and key > node.keys[i].key:
            i += 1
        if i < len(node.keys) and node.keys[i].key == key:
            return HashTable([("node", node), ("key_index", i)])

        # If this is a leaf node, then the key is not present
        if node.is_leaf:
            return None

        child_offset = node.children[i]
        return self._search(child_offset, key)

    def insert(self, key, pointer: int):
        root_node = self.root

        existing_key_info = self.search(key)

        # If key already exists, we just add the new pointer
        if existing_key_info:
            existing_key_node = existing_key_info["node"]
            existing_key_index = existing_key_info["key_index"]
            existing_key = existing_key_node.keys[existing_key_index]
            if existing_key.pointers[1] == -1:
                pointer_pos = self.pointer_manager.create_pointer_list(pointer)
                existing_key.pointers[1] = pointer_pos
                existing_key_node.keys[existing_key_index] = existing_key
                self._save_node(existing_key_node)
            else:
                self.pointer_manager.add_pointer_to_pointer_list(existing_key.pointers[1], pointer)

            return

        # If the root is full, the tree grows in height
        if root_node.is_full():
            new_root = BTreeNode(t=self.manager.t, is_leaf=False)
            new_root.children.append(root_node.offset)
            self._split_child(new_root, 0)

            new_root_offset = self._save_node(new_root)
            self.manager.root_offset = new_root_offset
            self.manager.update_header()
            self._insert_non_full(new_root, key, pointer)
            self._save_node(new_root)
        else:
            self._insert_non_full(root_node, key, pointer)
            self._save_node(root_node)

    def _insert_non_full(self, node: BTreeNode, key, pointer: int):
        i = len(node.keys) - 1

        if node.is_leaf:
            new_key = BTreeNodeKey(key, [pointer, -1], self.manager.key_max_size)
            node.keys.append(new_key)
            node.sort_node_keys()  # TODO - not needed to use the built-in .sort() + is it even allowed?
        else:
            while i >= 0 and key < node.keys[i].key:
                i -= 1
            i += 1

            child_offset = node.children[i]
            child_node = self._load_node(child_offset)

            if child_node.is_full():
                self._split_child(node, i)
                if key > node.keys[i].key:
                    i += 1

                child_offset = node.children[i]
                child_node = self._load_node(child_offset)

            self._insert_non_full(child_node, key, pointer)
            self._save_node(child_node)

    def _split_child(self, parent: BTreeNode, i: int):
        """
        Split a full child node into two nodes.
        """
        child_offset = parent.children[i]
        child = self._load_node(child_offset)

        t = self.manager.t
        new_node = BTreeNode(t=t, is_leaf=child.is_leaf)

        parent.keys.insert(i, child.keys[t - 1])
        new_node.keys = child.keys[t:]
        child.keys = child.keys[: t - 1]

        if not child.is_leaf:
            new_node.children = child.children[t:]
            child.children = child.children[:t]

        updated_child_offset = self._save_node(child)
        new_node_offset = self._save_node(new_node)

        parent.children.insert(i + 1, new_node_offset)

        self._save_node(parent)

    def print_tree(self, node_offset: int | None = None, level=0):
        """
        Debugging method to print the structure of the B-tree.

        Args:
            level (int): The current level of the tree (used for indentation).
        """
        if node_offset is None:
            node_offset = self.manager.root_offset

        node = self._load_node(node_offset)

        indent = "  " * level
        print(f"{indent}Level {level} | Keys: {node.keys} (offset={node_offset})")

        if not node.is_leaf:
            for child_off in node.children:
                self.print_tree(child_off, level + 1)

    def find_key_pointers(self, key):
        key_info = self.search(key)

        if key_info:
            key_node = key_info["node"]
            key_index = key_info["key_index"]
            actual_key = key_node.keys[key_index]
            other_pointers_pointer = actual_key.pointers[1]
            pointers = [actual_key.pointers[0]] + self.pointer_manager.traverse_pointer_list(other_pointers_pointer)
            return pointers

        return None
