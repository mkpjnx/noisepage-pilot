from abc import ABC, abstractmethod
from typing import Dict


class Configuration(ABC):
    @property
    @abstractmethod
    def identifier(self):
        raise NotImplementedError("Should be implemented by child classes")

    def __repr__(self) -> str:
        return self.identifier

    def __eq__(self, other):
        if not isinstance(other, Configuration):
            return False
        return self.__repr__() == other.__repr__()

    def __hash__(self) -> int:
        return hash(self.identifier)


class Action(ABC):
    def __init__(self, target: Configuration):
        self._target = target

    @property
    def target(self):
        return self._target

    @abstractmethod
    def to_sql(self) -> str:
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        pass

    def __repr__(self):
        return self.to_sql()


class ActionGenerator(ABC):
    def __init__(self):
        self.generator = self.get_action()

    @abstractmethod
    def get_action(self):
        pass

    def get_n(self, n=1) -> int:
        i = 0
        actions = []
        while i < n:
            try:
                actions.append(next(self.generator))
                i += 1
            except StopIteration:
                return actions
        return actions
