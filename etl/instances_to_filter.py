from enum import Enum


class InstancesToFilter(Enum):
    YEAR = "Q577"
    CALENDARYEAR = "Q3186692"
    DISAMBIGUATION = "Q4167410"
    LIST = "Q13406463"

    @classmethod
    def list(cls):
        return list(map(lambda p: p.value, InstancesToFilter))
