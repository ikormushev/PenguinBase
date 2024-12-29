import struct


class BTreeNodeManager:
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

        return BTreeNodeManager(file_path)

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

