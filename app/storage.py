import json
from pathlib import Path


class KeyValueStore:
    def __init__(self, file_path="data/store.json"):
        self.file_path = Path(file_path)
        self.data = {}
        self._load()

    def _load(self):
        if self.file_path.exists():
            with open(self.file_path, "r") as file:
                self.data = json.load(file)

    def _save(self):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.file_path, "w") as file:
            json.dump(self.data, file, indent=4)

    def put(self, key, value):
        self.data[key] = value
        self._save()

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        value = self.data.pop(key, None)
        self._save()
        return value