"""
It contains custom error used for datafeed
"""


class ListEnumError(ValueError):
    def __init__(self, list_type, value):
        self.value = value
        self.list_type = list_type
        super().__init__(f"EnumError for{self.list_type}: Details {self.value}")


class SubscibeListError(ValueError, value):
    self.value = value
    super.__init__(f"""{self.value}""")
