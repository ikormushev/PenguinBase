import struct

from utils.errors import TableError
from utils.extra import polynomial_rolling_hash


class PointerListManager:
    def __init__(self, file_path):
        self.file_path = file_path

        with open(self.file_path, "rb+") as file:
            file.seek(0)
            stored_hash_bytes = file.read(4)  # -> struct.calcsize("I") == 4
            if len(stored_hash_bytes) != 4:
                raise TableError("Corrupted file: PointerList header mismatch")
            stored_hash_val = struct.unpack("I", stored_hash_bytes)[0]

            header_bytes = file.read(struct.calcsize("qq"))
            if len(header_bytes) != struct.calcsize("qq"):
                raise TableError("Corrupted file: PointerList header mismatch")

            computed_hash_val = polynomial_rolling_hash(header_bytes)
            if computed_hash_val != stored_hash_val:
                raise TableError("Corrupted file: PointerList header mismatch")

            self.free_slot, self.eof = struct.unpack("qq", header_bytes)

    @staticmethod
    def create_pointer_list_manager(file_path):
        header_bytes = struct.calcsize("qq")
        header_data = struct.pack("qq", header_bytes + 4, header_bytes + 4)   # -> struct.calcsize("I") == 4
        header_hash_val = polynomial_rolling_hash(header_data)
        header_hash_bytes = struct.pack("I", header_hash_val)

        with open(file_path, "w+b") as file:
            file.seek(0)
            file.write(header_hash_bytes)
            file.write(header_data)
            file.flush()

        return PointerListManager(file_path)

    def update_header(self):
        header_data = struct.pack("qq", self.free_slot, self.eof)
        header_hash_val = polynomial_rolling_hash(header_data)
        header_hash_bytes = struct.pack("I", header_hash_val)

        with open(self.file_path, "rb+") as file:
            file.seek(0)
            file.write(header_hash_bytes)
            file.write(header_data)
            file.flush()

    def write_pointer(self, position: int, pointer_data: bytes):
        pointer_hash_val = polynomial_rolling_hash(pointer_data)
        pointer_hash_bytes = struct.pack("I", pointer_hash_val)

        with open(self.file_path, "rb+") as file:
            file.seek(position)
            file.write(pointer_hash_bytes)
            file.write(pointer_data)
            file.flush()

    def read_pointer(self, position: int) -> bytes:
        with open(self.file_path, "rb") as file:
            file.seek(position)
            stored_hash_bytes = file.read(4)  # -> struct.calcsize("I") == 4
            if len(stored_hash_bytes) != 4:
                raise TableError(f"Corrupted file: PointerList cannot load pointer at position {position}")
            stored_hash_val = struct.unpack("I", stored_hash_bytes)[0]

            pointer_data_size = struct.calcsize("qqq")
            pointer_data = file.read(pointer_data_size)
            if len(pointer_data) != pointer_data_size:
                raise TableError(f"Corrupted file: PointerList cannot load pointer at position {position}")

            computed_hash_val = polynomial_rolling_hash(pointer_data)

            if computed_hash_val != stored_hash_val:
                raise TableError(f"Corrupted file: PointerList cannot load pointer at position {position}")

            return pointer_data

    def allocate_space(self, position: int):
        if position == self.eof:
            self.eof += struct.calcsize("qqq") + 4  # -> struct.calcsize("I") == 4
            self.free_slot = self.eof
        else:
            self.free_slot = self.eof

        self.update_header()

    def create_pointer_list(self, pointer: int):
        pointer_data = struct.pack("qqq", -1, pointer, -1)
        position = self.free_slot

        self.write_pointer(position, pointer_data)

        self.allocate_space(position)
        return position

    def add_pointer_to_pointer_list(self, start_pointer: int, new_pointer: int):
        curr_position = start_pointer

        while curr_position != -1:
            pointer_data = self.read_pointer(curr_position)
            prev, current, next_ptr = struct.unpack("qqq", pointer_data)

            if next_ptr == -1:
                new_node = struct.pack("qqq", curr_position, new_pointer, -1)
                next_position = self.free_slot
                self.write_pointer(next_position, new_node)

                prev_node = struct.pack("qqq", prev, current, next_position)
                self.write_pointer(curr_position, prev_node)

                self.allocate_space(next_position)
                break

            curr_position = next_ptr

    def get_first_available_pointer(self, start_pos: int):
        pointer_data = self.read_pointer(start_pos)
        prev, current, next_ptr = struct.unpack("qqq", pointer_data)
        return current

    def delete_pointer_from_pointer_list(self, start_pointer: int, pointer_to_delete: int):
        curr_position = start_pointer
        new_start_pointer = -1

        while curr_position != -1:
            curr_pointer_data = self.read_pointer(curr_position)
            curr_prev, curr_curr, curr_next = struct.unpack("qqq", curr_pointer_data)

            if curr_curr == pointer_to_delete:
                if curr_prev != -1:
                    previous_pointer_data = self.read_pointer(curr_prev)
                    prev_prev, prev_curr, prev_next = struct.unpack("qqq", previous_pointer_data)
                    previous_pointer_new_data = struct.pack("qqq", prev_prev, prev_curr, curr_next)
                    self.write_pointer(curr_prev, previous_pointer_new_data)

                if curr_next != -1:
                    next_pointer_data = self.read_pointer(curr_next)
                    next_prev, next_curr, next_next = struct.unpack("qqq", next_pointer_data)
                    next_pointer_new_data = struct.pack("qqq", curr_prev, next_curr, next_next)
                    self.write_pointer(curr_next, next_pointer_new_data)

                    new_start_pointer = curr_next

                self.free_slot = curr_position
                break

            curr_position = curr_next

        if start_pointer == curr_position:
            return new_start_pointer
        return start_pointer

    def traverse_pointer_list(self, start_pointer: int):
        position = start_pointer

        while position != -1:
            curr_pointer_data = self.read_pointer(position)
            prev_p, curr_p, next_p = struct.unpack("qqq", curr_pointer_data)
            yield curr_p

            position = next_p
