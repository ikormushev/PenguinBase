from typing import List

from utils.binary_insertion_sort import binary_insertion_sort


class BTreeNodeKey:
    def __init__(self, key, pointers: List[int]):
        self.key = key
        self.pointers = pointers

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
    def __init__(self, t: int, is_leaf: bool = True):
        """
        Args:
            t (int): Minimum degree of the B-tree. Defines the range for the number of keys.
            is_leaf (bool): Whether this node is a leaf node - the bottom-most node of the B-tree.
        """
        self.t = t
        self.keys: List[BTreeNodeKey] = []
        self.children = []
        self.is_leaf = is_leaf

    def maximum_number_of_keys(self):
        """
        In a B-tree, the maximum number of keys a node can have is ((2 * t) - 1).
        """
        return (2 * self.t) - 1

    def is_full(self) -> bool:
        return len(self.keys) == self.maximum_number_of_keys()

    def sort_node_keys(self):
        """
        Sort the keys in the current B-tree node.

        B-tree requires the nodes to be sorted in ascending order.
        Here, sorting is performed by Binary Insertion Sort because the number of keys in a node is typically small.
        """
        self.keys = binary_insertion_sort(self.keys)


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
        self.root = BTreeNode(t)
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
        if root.is_full():
            new_node = BTreeNode(self.t, is_leaf=False)
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

            if node.children[i].is_full():
                self._split_child(node, i, node.children[i])
                if key > node.keys[i].key:
                    i += 1
            self._insert_non_full(node.children[i], key, pointer)

    def _split_child(self, parent: BTreeNode, i: int, child: BTreeNode):
        """
        Split a full child node into two nodes.
        """

        t = self.t
        new_node = BTreeNode(t, is_leaf=child.is_leaf)
        parent.children.insert(i + 1, new_node)  # TODO - recreate .insert()?
        parent.keys.insert(i, child.keys[t - 1])  # TODO - recreate .insert()?

        # Move the second half of keys to the new node
        new_node.keys = child.keys[t:child.maximum_number_of_keys()]
        child.keys = child.keys[:(t - 1)]

        # If the child is not a leaf, move its children as well
        if not child.is_leaf:
            new_node.children = child.children[t:(2 * t)]
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
