from typing import Protocol


class Hi(Protocol):
    def hello():
        return "HI"


class By():
    def hi():
        return "By"
