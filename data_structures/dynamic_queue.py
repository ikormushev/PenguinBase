from utils.binary_insertion_sort import binary_insertion_sort


class QueueNode:
    def __init__(self, value, next_node=None):
        self.value = value
        self.next = next_node


class DynamicQueue:
    def __init__(self):
        self.front = None
        self.rear = None
        self.length = 0

    def enqueue(self, value):
        node = QueueNode(value)

        if self.rear is None:
            self.front = node
            self.rear = node
        else:
            self.rear.next = node
            self.rear = node
        self.length += 1

    def dequeue(self):
        if self.front is None:
            return

        node = self.front
        self.front = self.front.next

        if self.front is None:
            self.rear = None

        self.length -= 1
        return node

    def peek(self):
        if self.front is not None:
            return self.front.value
        return None

    @staticmethod
    def from_list_sorted(arr: list):
        arr = binary_insertion_sort(arr)
        return DynamicQueue.from_list(arr)

    @staticmethod
    def from_list(arr: list):
        queue = DynamicQueue()
        for el in arr:
            queue.enqueue(el)
        return queue
