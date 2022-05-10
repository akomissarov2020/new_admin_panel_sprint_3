# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Permanent storage."""

import abc
import json
import os
from typing import Any, NoReturn, Optional

import redis


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        with open(self.file_path, "w") as fh:
            json.dump(state, fh)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        if not os.path.isfile(self.file_path):
            return {}
        with open(self.file_path) as fh:
            return json.load(fh)


class RedisStorage(BaseStorage):

    def __init__(self, config: Any) -> NoReturn:
        self.redis_adapter = redis.Redis(
            **config.get_redis_dict(), decode_responses=True
        )

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        if not state:
            self.redis_adapter.delete("state")
        else:
            self.redis_adapter.hset("state", mapping=state)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        return self.redis_adapter.hgetall("state")
