import os.path
import random
import struct
from typing import List

from utils.binary_insertion_sort import binary_insertion_sort
from utils.validators import is_valid_date


class BTreeNodeKey:
    def __init__(self, key, pointers: List[int]):
        self.key = key
        self.pointers = pointers

    def serialize_key(self) -> bytes:
        if isinstance(self.key, int):
            key_data = b"I" + struct.pack("i", self.key)  # TODO - later changed to long long - q
        elif isinstance(self.key, float):
            key_data = b"F" + struct.pack("f", self.key)
        elif isinstance(self.key, str) and is_valid_date(self.key):
            key_type = b"D"
            encoded_key = self.key.encode()
            key_data = key_type + encoded_key
        elif isinstance(self.key, str):
            key_type = b"S"
            encoded_key = self.key.encode()
            key_data = key_type + struct.pack("i", len(encoded_key)) + encoded_key
        else:
            raise ValueError('Invalid key type')

        pointers_data = struct.pack("i", len(self.pointers))
        for pointer in self.pointers:
            pointers_data += struct.pack("i", pointer)  # TODO - later changed to long long - q

        return key_data + pointers_data

    @staticmethod
    def deserialize_key(key_data):
        offset = 0

        key_type = key_data[offset:offset + 1]
        offset += 1

        if key_type == b"I":
            key = struct.unpack("i", key_data[offset:offset + 4])[0]  # TODO - change needed later for long long
            offset += 4
        elif key_type == b"F":
            key = struct.unpack("f", key_data[offset:offset + 4])[0]
            offset += 4
        elif key_type == b"D":
            value_bytes = key_data[offset:offset + 10]
            offset += 10
            key = value_bytes.decode()
        elif key_type == b"S":
            length = struct.unpack_from("i", key_data, offset)[0]
            offset += struct.calcsize("i")
            value_bytes = key_data[offset:offset + length]
            offset += length

            key = value_bytes.decode()
        else:
            raise ValueError("Unsupported key type")

        num_pointers = struct.unpack_from("i", key_data[offset: offset + 4])[0]
        offset += 4
        pointers = []

        for _ in range(num_pointers):
            pointer = struct.unpack_from("i", key_data[offset:offset + 4])[0]
            offset += 4
            pointers.append(pointer)

        return BTreeNodeKey(key, pointers)

    def __lt__(self, other):
        if isinstance(other, BTreeNodeKey):
            return self.key < other.key
        return self.key < other

    def __eq__(self, other):
        if isinstance(other, BTreeNodeKey):
            return self.key == other.key
        return self.key == other

    def __repr__(self):
        return f"{self.key}: {self.pointers}"


class BTreeNode:
    def __init__(self, is_leaf: bool = True, keys=None, children=None):
        """
        Args:
            is_leaf (bool): Whether this node is a leaf node - the bottom-most node of the B-tree.
        """
        self.keys: List[BTreeNodeKey] = keys if keys is not None else []
        self.children = children if children is not None else []  # List[BTreeNode]
        self.is_leaf = is_leaf

    def sort_node_keys(self):
        """
        Sort the keys in the current B-tree node.

        B-tree requires the nodes to be sorted in ascending order.
        Here, sorting is performed by Binary Insertion Sort
        because the number of keys in a node is typically small.
        """
        self.keys = binary_insertion_sort(self.keys)

    def serialize_node(self) -> bytes:
        metadata = struct.pack("?i", self.is_leaf, len(self.keys))

        keys_data = b""
        for key in self.keys:
            key_bytes = key.serialize_key()
            keys_data += struct.pack("i", len(key_bytes)) + key_bytes

        children_data = b""
        if not self.is_leaf:
            for child in self.children:
                child_bytes = child.serialize_node()
                children_data += struct.pack("i", len(child_bytes)) + child_bytes

        return metadata + keys_data + children_data

    @staticmethod
    def deserialize_node(node_data: bytes):
        offset = 0
        metadata_size = struct.calcsize("?i")

        is_leaf, keys_num = struct.unpack_from("?i", node_data[:offset + metadata_size])
        offset += metadata_size

        keys = []

        for _ in range(keys_num):
            key_length = struct.unpack_from("i", node_data[offset: offset + 4])[0]
            offset += 4
            key_data = node_data[offset:offset + key_length]
            key = BTreeNodeKey.deserialize_key(key_data)
            keys.append(key)
            offset += key_length

        children = []
        if not is_leaf:
            for i in range(keys_num + 1):
                child_length = struct.unpack_from("i", node_data[offset:offset + 4])[0]
                offset += 4
                child_data = node_data[offset:offset + child_length]
                child = BTreeNode.deserialize_node(child_data)
                children.append(child)
                offset += child_length

        return BTreeNode(is_leaf=is_leaf, keys=keys, children=children)

    def is_full(self, t):
        return len(self.keys) == self.maximum_number_of_keys(t)

    @staticmethod
    def maximum_number_of_keys(t):
        """
        In a B-tree, the maximum number of keys a node can have is ((2 * t) - 1).
        """
        return (2 * t) - 1

    def find_key_index(self, key):
        idx = 0
        while idx < len(self.keys) and key > self.keys[idx].key:
            idx += 1
        return idx


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

    def __init__(self, t: int):
        self.root = BTreeNode()
        self.t = t

    def search(self, key) -> BTreeNodeKey | None:
        return self._search(self.root, key)

    def _search(self, node: BTreeNode, key) -> BTreeNodeKey | None:
        i = 0

        # Find the first key greater than or equal to k
        while i < len(node.keys) and key > node.keys[i].key:
            i += 1

        if i < len(node.keys) and node.keys[i].key == key:
            return node.keys[i]

        # If this is a leaf node, then the key is not present
        if node.is_leaf:
            return None

        return self._search(node.children[i], key)

    def insert(self, key, pointer: int):
        root = self.root

        existing_key = self.search(key)

        # If key already exists, we just add the new pointer
        if existing_key:
            existing_key.pointers.append(pointer)
            return

        # If the root is full, the tree grows in height
        if root.is_full(self.t):
            new_node = BTreeNode(is_leaf=False)
            new_node.children.append(self.root)
            self._split_child(new_node, 0, self.root)
            self.root = new_node

        self._insert_non_full(self.root, key, pointer)

    def _insert_non_full(self, node: BTreeNode, key, pointer: int):
        i = len(node.keys) - 1

        if node.is_leaf:
            new_key = BTreeNodeKey(key, [pointer])
            node.keys.append(new_key)
            node.sort_node_keys()  # not needed to use the built-in .sort() + is it even allowed?
        else:
            while i >= 0 and key < node.keys[i].key:
                i -= 1
            i += 1

            if node.children[i].is_full(self.t):
                self._split_child(node, i, node.children[i])
                if key > node.keys[i].key:
                    i += 1
            self._insert_non_full(node.children[i], key, pointer)

    def _split_child(self, parent: BTreeNode, i: int, child: BTreeNode):
        """
        Split a full child node into two nodes.
        """

        t = self.t
        new_node = BTreeNode(is_leaf=child.is_leaf)
        parent.children.insert(i + 1, new_node)  # TODO - recreate .insert()?
        parent.keys.insert(i, child.keys[t - 1])  # TODO - recreate .insert()?

        # Move the second half of keys to the new node
        new_node.keys = child.keys[t:new_node.maximum_number_of_keys(t)]
        child.keys = child.keys[0:t-1]

        # If the child is not a leaf, move its children as well
        if not child.is_leaf:
            new_node.children = child.children[t:]
            child.children = child.children[:t]

    def print_tree(self, node=None, level=0):
        """
        Debugging method to print the structure of the B-tree.

        Args:
            node (BTreeNode): The node to start printing from. Defaults to the root.
            level (int): The current level of the tree (used for indentation).
        """
        if node is None:
            node = self.root

        print("  " * level + f"Level {level} | Keys: {node.keys}")

        if not node.is_leaf:
            for child in node.children:
                self.print_tree(child, level + 1)

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
        else:
            # Case 3: Key is not in this node
            if node.is_leaf:
                return

            if len(node.children[idx].keys) < self.t:
                self._fix_child(node, idx)

            # Recursively delete from the appropriate child
            self._delete_from_node(node.children[idx], key)

    def _delete_internal_node(self, node: BTreeNode, index: int):
        """
        Handle deletion when the key is in an internal node - any node that is not a leaf.
        """
        key = node.keys[index]

        if len(node.children[index].keys) >= self.t:
            # Case 2.1: Use the predecessor - the largest key on the left child
            pred = self._get_predecessor(node, index)
            node.keys[index] = pred
            self._delete_from_node(node.children[index], pred.key)
        elif len(node.children[index + 1].keys) >= self.t:
            # Case 2.2: Use the successor - the smallest key on the right child
            succ = self._get_successor(node, index)
            node.keys[index] = succ
            self._delete_from_node(node.children[index], succ.key)
        else:
            # Case 2.3: Merge the two children and delete
            self._merge(node, index)
            self._delete_from_node(node.children[index], key.key)

    def _fix_child(self, node, idx):
        """
        Ensure the child node has at least t keys by borrowing or merging.

        Args:
            node - the parent node
            idx - the index of the child node
        """
        if idx > 0 and len(node.children[idx - 1].keys) >= self.t:
            # Borrow from the left sibling
            self._borrow_from_left_sibling(node, idx)
        elif idx < len(node.children) - 1 and len(node.children[idx + 1].keys) >= self.t:
            # Borrow from the right sibling
            self._borrow_from_right_sibling(node, idx)
        else:
            # Merge with a sibling
            if idx < len(node.children) - 1:
                self._merge(node, idx)  # left
            else:
                self._merge(node, idx - 1)  # right

    def _merge(self, node, idx):
        """
        Merge two children of a node at the given index.
        """
        child = node.children[idx]
        sibling = node.children[idx + 1]

        child.keys.append(node.keys[idx])
        child.keys.extend(sibling.keys)

        if not child.is_leaf:
            child.children.extend(sibling.children)

        node.keys.pop(idx)
        node.children.pop(idx + 1)

    def _borrow_from_left_sibling(self, node, idx):
        """
        Borrow a key from the left sibling.
        Args:
            node - the parent node
            idx - the index of the child node
        """
        child = node.children[idx]
        sibling = node.children[idx - 1]

        # Move a key from the parent to the child
        child.keys.insert(0, node.keys[idx - 1])
        node.keys[idx - 1] = sibling.keys.pop()

        if not child.is_leaf:
            child.children.insert(0, sibling.children.pop())

    def _borrow_from_right_sibling(self, node, idx):
        """
        Borrow a key from the right sibling.
        Args:
            node - the parent node
            idx - the index of the child node
        """

        child = node.children[idx]
        sibling = node.children[idx + 1]

        # Move a key from the parent to the child
        child.keys.append(node.keys[idx])
        node.keys[idx] = sibling.keys.pop(0)

        # Move the sibling's first child to the child
        if not child.is_leaf:
            child.children.append(sibling.children.pop(0))

    def _get_predecessor(self, node, index) -> BTreeNodeKey:
        """
        Get the predecessor - the largest key on the left child.
        """
        current = node.children[index]
        while not current.is_leaf:
            current = current.children[-1]
        return current.keys[-1]

    def _get_successor(self, node, index) -> BTreeNodeKey:
        """
        Get the successor - the smallest key on the right child.
        """
        current = node.children[index + 1]
        while not current.is_leaf:
            current = current.children[0]
        return current.keys[0]

    def delete(self, key):
        if not self.root:
            return

        self._delete_from_node(self.root, key)

        if not self.root.keys and not self.root.is_leaf:
            self.root = self.root.children[0]

    def serialize_tree(self, file_path):
        with open(file_path, 'wb') as file:
            file.seek(0)
            minimun_degree = b"T" + struct.pack("i", self.t)
            file.write(minimun_degree)

            serialized_root = self.root.serialize_node()
            file.write(serialized_root)

    @staticmethod
    def deserialize_tree(file_path):
        with open(file_path, 'rb') as file:
            file.read(1)  # skipping the minimum_degree_key
            minimum_degree_size = struct.calcsize("i")
            t = struct.unpack("i", file.read(minimum_degree_size))[0]

            root = BTreeNode.deserialize_node(file.read())

        btree = BTree(t)
        btree.root = root
        return btree
