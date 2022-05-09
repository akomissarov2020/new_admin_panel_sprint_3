# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Dataclasses for loadind data from:
    1) sqlite3 to postgres;
    2) postgres to elasticsearch.
"""
import abc
import datetime
import uuid
from dataclasses import _MISSING_TYPE, dataclass, field, fields
from uuid import UUID
from pydantic.schema import Optional, List
from pydantic import BaseModel, validator


class PersonModel(BaseModel):
    id: str
    name: str


class MovieModel(BaseModel):
    id: UUID
    imdb_rating: Optional[float]
    genre: Optional[List]

    title: str
    description: Optional[str]

    director: Optional[List]
    actors: Optional[List]
    actors_names: Optional[List]
    writers_names: Optional[List]

    writers: Optional[List]

    @validator("imdb_rating")
    def valid_imdb_rating(cls, value):
        if value is None:
            return 0.0
        return value

    @validator("description")
    def valid_description(cls, value):
        if value is None:
            return ""
        return value

    @validator("actors")
    def valid_actors(cls, value):
        if value is None:
            return []
        return value

    @validator("director")
    def valid_director(cls, value):
        if value is None:
            return []
        return value

    @validator("writers")
    def valid_writers(cls, value):
        if value is None:
            return []
        return value

    @validator("actors_names")
    def valid_actors_names(cls, value):
        if value is None:
            return []
        return value

    @validator("writers_names")
    def valid_writers_names(cls, value):
        if value is None:
            return []
        return value
