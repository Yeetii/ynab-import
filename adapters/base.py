from abc import ABC, abstractmethod
from pathlib import Path

from models import Transaction


class BaseAdapter(ABC):
    @abstractmethod
    def parse(self, filepath: Path) -> list[Transaction]: ...
