from pathlib import Path


class Unavailable(Exception):
    pass


class PayloadPort:
    def __init__(self):
        self._offline = set()
        self._accesses = []

    def set_available(self, file_id, column, available):
        if type(available) is not bool:
            raise ValueError("availability must be boolean")
        key = (file_id, column)
        if available:
            self._offline.discard(key)
        else:
            self._offline.add(key)

    def read_bytes(self, file_id, column, path):
        key = (file_id, column)
        self._accesses.append(key)
        if key in self._offline:
            raise Unavailable(f"offline payload: {file_id}/{column}")
        try:
            return Path(path).read_bytes()
        except FileNotFoundError as exc:
            raise Unavailable(f"missing payload: {file_id}/{column}") from exc

    @property
    def access_log(self):
        return list(self._accesses)

    def clear_access_log(self):
        self._accesses.clear()
