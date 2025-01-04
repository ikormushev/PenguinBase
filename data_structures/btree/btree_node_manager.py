import struct

from utils.errors import TableError
from utils.extra import polynomial_rolling_hash


class BTreeNodeManager:
    def __init__(self, file_path: str):
        self.file_path = file_path

        with open(self.file_path, "rb+") as file:
            file.seek(0)
            stored_hash_bytes = file.read(4)  # -> struct.calcsize("I") == 4
            if len(stored_hash_bytes) != 4:
                raise TableError("Corrupted file: BTree header mismatch")
            stored_hash_val = struct.unpack("I", stored_hash_bytes)[0]

            header_bytes = file.read(struct.calcsize("iqq1si"))
            if len(header_bytes) != struct.calcsize("iqq1si"):
                raise TableError("Corrupted file: BTree header mismatch")

            computed_hash_val = polynomial_rolling_hash(header_bytes)
            if computed_hash_val != stored_hash_val:
                raise TableError("Corrupted file: BTree header mismatch")

            self.t, self.root_offset, self.eof, key_type, self.key_max_size = struct.unpack("iqq1si",
                                                                                                 header_bytes)

            self.key_type = key_type.decode()

    def update_header(self):
        header_data = struct.pack("iqq1si",
                                  self.t, self.root_offset, self.eof,
                                  self.key_type.encode(), self.key_max_size)
        header_hash_val = polynomial_rolling_hash(header_data)
        header_hash_bytes = struct.pack("I", header_hash_val)

        with open(self.file_path, "rb+") as file:
            file.seek(0)
            file.write(header_hash_bytes)
            file.write(header_data)
            file.flush()

    @staticmethod
    def create_node_manager(file_path, t, key_type, key_max_size):
        with open(file_path, "wb+") as file:
            header_bytes_size = struct.calcsize("iqq1si")
            header_data = struct.pack("iqq1si", t,
                                      header_bytes_size + 4, header_bytes_size + 4,
                                      key_type.encode(), key_max_size)
            file.seek(0)

            header_hash_val = polynomial_rolling_hash(header_data)
            header_hash_bytes = struct.pack("I", header_hash_val)
            file.write(header_hash_bytes)
            file.write(header_data)
            file.flush()

        return BTreeNodeManager(file_path)

    def save_node(self, offset: int | None, node_data: bytes) -> int:
        node_hash_val = polynomial_rolling_hash(node_data)
        node_hash_bytes = struct.pack("I", node_hash_val)

        with open(self.file_path, "rb+") as file:
            if offset is None:
                offset = self.eof

            file.seek(offset)
            file.write(node_hash_bytes)
            file.write(node_data)

            if self.eof < offset + len(node_data):
                self.eof = offset + len(node_data) + 4  # -> struct.calcsize("I") == 4
            file.flush()

        self.update_header()

        return offset

    def load_node(self, offset: int) -> bytes:
        with open(self.file_path, 'rb') as file:
            file.seek(offset)

            stored_hash_bytes = file.read(4)  # -> struct.calcsize("I") == 4
            if len(stored_hash_bytes) != 4:
                raise TableError(f"Corrupted file: cannot read the node hash")
            stored_hash_val = struct.unpack("I", stored_hash_bytes)[0]

            node_size = file.read(4)  # -> struct.calcsize("i") == 4
            if len(node_size) != 4:
                raise TableError(f"Corrupted file: BTree cannot load node with offset {offset}")

            length = struct.unpack("i", node_size)[0]
            data = file.read(length)
            if len(data) != length:
                raise TableError(f"Corrupted file: BTree cannot load node with offset {offset}")

        computed_hash_val = polynomial_rolling_hash(node_size + data)

        if computed_hash_val != stored_hash_val:
            raise TableError(f"Corrupted file: BTree cannot load node with offset {offset}")

        return data

