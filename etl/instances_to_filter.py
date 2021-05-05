from enum import Enum


class InstancesToFilter(Enum):
    YEAR = "Q577"
    CALENDARYEAR = "Q3186692"
    RECURRENTTIMEFRAME = "Q14795564"
    CENTURYLEAPYEAR = "Q3311614"
    FAMILYNAME = "Q101352"
    NAME = "Q82799"
    DISAMBIGUATION = "Q4167410"
    LIST = "Q13406463"

    @classmethod
    def list(cls):
        return [p.value for p in InstancesToFilter]
