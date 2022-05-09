# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Loadind data from sqlite3 to postgres."""

from storage import BaseStorage, JsonFileStorage
from typing import Any, Optional


class State:
    """
    Класс для хранения состояния при работе с данными,
    чтобы постоянно не перечитывать данные с начала.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.data = self.storage.retrieve_state()
        if self.data is None:
            self.data = {}

    def set(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.data[key] = value
        self.storage.save_state(self.data)

    def get(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        self.data = self.storage.retrieve_state()
        if key in self.data:
            return self.data[key]
        return None

    def is_empty(self):
        return len(self.data) == 0

    def __repr__(self):
        self.data = self.storage.retrieve_state()
        return str(",".join([f"{k}: {v}" for k, v in self.data.items()]))

    def clear(self):
        self.data = {}
        self.storage.save_state(self.data)
