from pathlib import Path
from typing import Iterator

import ijson


class ProductFileReader:
    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)

    def read_products(self) -> Iterator[dict]:
        if not self.file_path.exists():
            raise FileNotFoundError(f"Product file not found: {self.file_path}")

        with self.file_path.open("r", encoding="utf-8") as file_obj:
            for item in ijson.items(file_obj, "item"):
                if not isinstance(item, dict):
                    raise ValueError("Each product must be a JSON object")
                yield item
