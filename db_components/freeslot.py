class FreeSlot:
    def __init__(self, slot_position: int, slot_length: int):
        self.slot_position = slot_position
        self.slot_length = slot_length

    def __str__(self):
        return f"{self.slot_position}|{self.slot_length}"
