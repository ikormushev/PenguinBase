import struct
from typing import List

from data_structures.btree.btree_node_manager import BTreeNodeManager
from data_structures.btree.pointer_list_manager import PointerListManager
from data_structures.hash_table import HashTable
from utils.binary_insertion_sort import binary_insertion_sort
from utils.date import Date


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
        elif isinstance(self.key, Date):
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
            key_data = b"I" + struct.pack("q", self.key)
        elif key_type == "F":
            key_data = b"F" + struct.pack("d", self.key)
        elif key_type == "D":
            key_data = b"D" + f"{self.key}".encode()
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
            key = int(struct.unpack("q", key_data[offset:offset + 8])[0])
            offset += 8
        elif key_type == b"F":
            key = float(struct.unpack("d", key_data[offset:offset + 8])[0])
            offset += 8
        elif key_type == b"D":
            value_bytes = key_data[offset:offset + 10]
            offset += 10
            key = Date.from_string(value_bytes.decode())
        elif key_type == b"S":
            length = struct.unpack("i", key_data[offset:offset + 4])[0]
            key_max_size = length
            offset += 4

            value_bytes = key_data[offset:offset + length]
            offset += length

            key = value_bytes.decode().strip("\x00")  # TODO - recreate .strip()

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
        elif isinstance(self.key, Date) and isinstance(other.key, Date):
            return (self.key > other.key) - (self.key < other.key)
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

    def __repr__(self):
        return f"Offset: {self.offset} | Keys: {self.keys} | Children: {self.children}"


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
        self.manager = BTreeNodeManager(node_file_path)
        self.pointer_manager = PointerListManager(pointer_file_path)

    @property
    def t(self):
        return self.manager.t

    @staticmethod
    def create_tree(t, key_type, key_max_size, node_file_path, pointer_file_path):
        root = BTreeNode(t=t)
        root_bytes = root.serialize_node(key_type, key_max_size)
        manager = BTreeNodeManager.create_node_manager(node_file_path, t, key_type, key_max_size)
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
        node_info = self._search(self.manager.root_offset, key)
        if node_info:
            return self.find_key_pointers(node_info)
        return None

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
        if node_offset is None:
            node_offset = self.manager.root_offset

        node = self._load_node(node_offset)

        indent = "  " * level
        print(f"{indent}Level {level} | Keys: {node.keys} (offset={node_offset})")

        if not node.is_leaf:
            for child_off in node.children:
                self.print_tree(child_off, level + 1)

    def find_key_pointers(self, key_info: HashTable):
        key_node = key_info["node"]
        key_index = key_info["key_index"]
        actual_key = key_node.keys[key_index]
        other_pointers_pointer = actual_key.pointers[1]
        pointers = [actual_key.pointers[0]] + self.pointer_manager.traverse_pointer_list(other_pointers_pointer)
        return pointers

    def _delete_from_node(self, node: BTreeNode, key):
        """
        Main points when a node is deleted:
        - a node can have max t children
        - a node should have min (t // 2) children
        - a node can contaion max (t - 1) keys
            - only a leaf node can have max t keys
        - a node (except root) should contain min ((t/2) - 1) keys
        """
        idx = node.find_key_index(key)

        if idx < len(node.keys) and node.keys[idx].key == key:
            if node.is_leaf:
                # Case 1: Key is in the leaf node
                node.keys.pop(idx)  # TODO - .pop() allowed?
            else:
                # Case 2: Key is in an internal node - any node that is not a leaf
                self._delete_internal_node(node, idx)
            self._save_node(node)
        else:
            # Case 3: Key is not in this node
            if node.is_leaf:
                return

            child_offset = node.children[idx]
            child_node = self._load_node(child_offset)

            if len(child_node.keys) < self.t:
                self._fix_child(node, idx)
                # self._save_node(node)  # TODO - probably not needed
                child_node = self._load_node(node.children[idx])

            # Recursively delete from the appropriate child
            self._delete_from_node(child_node, key)
            self._save_node(child_node)

    def _delete_internal_node(self, node: BTreeNode, index: int):
        """
        Handle deletion when the key is in an internal node - any node that is not a leaf.
        """
        key = node.keys[index]
        left_child_offset = node.children[index]
        right_child_offset = node.children[index + 1]

        left_child = self._load_node(left_child_offset)
        right_child = self._load_node(right_child_offset)

        if len(left_child.keys) >= self.t:
            # Case 2.1: Use the predecessor - the largest key on the left child
            pred = self._get_predecessor(node, index)
            node.keys[index] = pred
            self._save_node(node)

            self._delete_from_node(left_child, pred.key)
            # self._save_node(left_child) # TODO - probably not needed
        elif len(right_child.keys) >= self.t:
            # Case 2.2: Use the successor - the smallest key on the right child
            succ = self._get_successor(node, index)
            node.keys[index] = succ
            self._save_node(node)

            self._delete_from_node(right_child, succ.key)
            # self._save_node(right_child) # TODO - probably not needed
        else:
            # Case 2.3: Merge the two children and delete
            self._merge(node, index)
            self._save_node(node)

            merged_child_offset = node.children[index]
            merged_child = self._load_node(merged_child_offset)
            self._delete_from_node(merged_child, key.key)
            self._save_node(merged_child)

    def _fix_child(self, parent_node: BTreeNode, idx: int):
        """
        Ensure the child node has at least t keys by borrowing or merging.
        Args:
            node - the parent node
            idx - the index of the child node
        """
        left_sibling_offset = None
        right_sibling_offset = None

        if idx > 0:
            left_sibling_offset = parent_node.children[idx - 1]
        if idx < len(parent_node.children) - 1:
            right_sibling_offset = parent_node.children[idx + 1]

        if left_sibling_offset is not None:
            left_sibling = self._load_node(left_sibling_offset)

            if len(left_sibling.keys) >= self.t:
                # Borrow from the left sibling
                self._borrow_from_left_sibling(parent_node, idx)
                return

        if right_sibling_offset is not None:
            right_sibling = self._load_node(right_sibling_offset)

            if len(right_sibling.keys) >= self.t:
                # Borrow from the right sibling
                self._borrow_from_right_sibling(parent_node, idx)
                return

        # Merge with a sibling
        if right_sibling_offset is not None:
            self._merge(parent_node, idx)  # left
        else:
            self._merge(parent_node, idx - 1)  # right

    def _merge(self, parent_node: BTreeNode, idx: int):
        """
        Merge two children of a node at the given index.
        """
        left_child_offset = parent_node.children[idx]
        left_child = self._load_node(left_child_offset)

        right_child_offset = parent_node.children[idx + 1]
        right_child = self._load_node(right_child_offset)

        separator_key = parent_node.keys[idx]

        left_child.keys.append(separator_key)
        left_child.keys.extend(right_child.keys)

        if not left_child.is_leaf:
            left_child.children.extend(right_child.children)

        parent_node.keys.pop(idx)
        parent_node.children.pop(idx + 1)

        self._save_node(parent_node)
        self._save_node(left_child)
        self._save_node(right_child)

    def _borrow_from_left_sibling(self, parent_node: BTreeNode, idx: int):
        """
        Borrow a key from the left sibling.
        Args:
            node - the parent node
            idx - the index of the child node
        """
        child_offset = parent_node.children[idx]
        child = self._load_node(child_offset)

        left_sibling_offset = parent_node.children[idx - 1]
        left_sibling = self._load_node(left_sibling_offset)

        # Move a key from the parent to the child
        child.keys.insert(0, parent_node.keys[idx - 1])
        parent_node.keys[idx - 1] = left_sibling.keys.pop()

        if not child.is_leaf:
            child.children.insert(0, left_sibling.children.pop())

        self._save_node(parent_node)
        self._save_node(child)
        self._save_node(left_sibling)

    def _borrow_from_right_sibling(self, parent_node: BTreeNode, idx: int):
        """
        Borrow a key from the right sibling.
        Args:
            node - the parent node
            idx - the index of the child node
        """
        child_offset = parent_node.children[idx]
        child = self._load_node(child_offset)

        right_sibling_offset = parent_node.children[idx + 1]
        right_sibling = self._load_node(right_sibling_offset)

        # Move a key from the parent to the child
        child.keys.append(parent_node.keys[idx])
        parent_node.keys[idx] = right_sibling.keys.pop(0)

        if not child.is_leaf:
            child.children.append(right_sibling.children.pop(0))

        self._save_node(parent_node)
        self._save_node(child)
        self._save_node(right_sibling)

    def _get_predecessor(self, node: BTreeNode, index: int) -> BTreeNodeKey:
        """
        Get the predecessor - the largest key on the left child.
        """
        current = self._load_node(node.children[index])
        while not current.is_leaf:
            child_offset = current.children[-1]
            current = self._load_node(child_offset)
        return current.keys[-1]

    def _get_successor(self, node: BTreeNode, index: int) -> BTreeNodeKey:
        """
        Get the successor - the smallest key on the right child.
        """
        current = self._load_node(node.children[index + 1])
        while not current.is_leaf:
            child_offset = current.children[0]
            current = self._load_node(child_offset)
        return current.keys[0]

    def delete(self, key):
        if self.manager.root_offset == -1:
            return

        root_node = self.root
        self._delete_from_node(root_node, key)
        self._save_node(root_node)

        if not self.root.keys and not self.root.is_leaf:
            if root_node.children:
                self.manager.root_offset = root_node.children[0]
            else:
                self.manager.root_offset = -1
            self.manager.update_header()

    def delete_pointer(self, key, pointer: int):
        searched_node_info = self.search(key)
        if searched_node_info is None:
            return

        searched_node = searched_node_info["node"]
        searched_key_index = searched_node_info["key_index"]

        searched_key = searched_node.keys[searched_key_index]
        main_pointer = searched_key.pointers[0]
        secondary_pointer_to_file = searched_key.pointers[1]

        if main_pointer == pointer:
            if secondary_pointer_to_file == -1:
                self.delete(key)
                return
            else:
                new_main_pointer = self.pointer_manager.get_first_available_pointer(secondary_pointer_to_file)
                searched_key.pointers[0] = new_main_pointer

                new_secondary_pointer = self.pointer_manager.delete_pointer_from_pointer_list(secondary_pointer_to_file,
                                                                                              new_main_pointer)
                searched_key.pointers[1] = new_secondary_pointer
                searched_node.keys[searched_key_index] = searched_key
        else:
            new_secondary_pointer = self.pointer_manager.delete_pointer_from_pointer_list(secondary_pointer_to_file,
                                                                                          pointer)
            searched_key.pointers[1] = new_secondary_pointer
            searched_node.keys[searched_key_index] = searched_key

        self._save_node(searched_node)

    def _range_search_node(self, node_offset: int, lower, upper):
        i = 0
        node = self._load_node(node_offset)

        while i < len(node.keys) and node.keys[i].key < lower:
            if not node.is_leaf:
                yield from self._range_search_node(node.children[i], lower, upper)
            i += 1

        while i < len(node.keys) and node.keys[i].key <= upper:
            if not node.is_leaf:
                yield from self._range_search_node(node.children[i], lower, upper)

            yield self.find_key_pointers(HashTable([("node", node), ("key_index", i)]))
            i += 1

        if not node.is_leaf:
            yield from self._range_search_node(node.children[i], lower, upper)

    def range_search(self, lower, upper):
        yield from self._range_search_node(self.manager.root_offset, lower, upper)