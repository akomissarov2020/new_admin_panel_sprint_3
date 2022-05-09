# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Dataclasses for loadind data from sqlite3 to postgres."""
import abc
import datetime
import uuid
from dataclasses import _MISSING_TYPE, dataclass, field, fields


@dataclass
class BaseImportDataclass:

    def __post_init__(self) -> None:
        """Replace None values with defaults"""
        for field in fields(self):
            value = getattr(self, field.name)
            if not isinstance(field.default, _MISSING_TYPE) and value is None:
                setattr(self, field.name, field.default)

    def test_the_same(self, item->BaseImportDataclass) -> None:
        """Test the same with given item."""
        for field in fields(self):
            value_a = getattr(self, field.name)
            value_b = getattr(item, field.name)
            assert value_a == value_b

    @abc.abstractmethod
    def get_data(self) -> None:
        """Return data tuple for fastq SQL export"""
        pass

    @abc.abstractmethod
    def get_fields(self) -> None:
        """Return fields names"""
        pass


@dataclass
class Filmwork(BaseImportDataclass):
    title: str
    file_path: str
    creation_date: datetime.datetime
    created_at: datetime.datetime
    updated_at: datetime.datetime
    description: str = field(default="")
    rating: float = field(default=0.0)
    type: str = field(default="")
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def get_data(self):
        return (
            self.id,
            self.title,
            self.description,
            self.creation_date,
            self.file_path,
            self.rating,
            self.type,
            self.created_at,
            self.updated_at,
        )

    def get_fields():
        return (
            "id",
            "title",
            "description",
            "creation_date",
            "file_path",
            "rating",
            "type",
            "created",
            "modified",
        )


@dataclass
class Genre(BaseImportDataclass):
    name: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    description: str = field(default="")
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def get_data(self):
        return (self.id, self.name, self.description, self.created_at, self.updated_at)

    def get_fields():
        return "id", "name", "description", "created", "modified"


@dataclass
class Person(BaseImportDataclass):

    full_name: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def get_data(self):
        return (self.id, self.full_name, self.created_at, self.updated_at)

    def get_fields():
        return "id", "full_name", "created", "modified"


@dataclass
class GenreFilmwork(BaseImportDataclass):
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    created_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def get_data(self):
        return (
            self.id,
            self.film_work_id,
            self.genre_id,
            self.created_at,
        )

    def get_fields():
        return "id", "film_work_id", "genre_id", "created"


@dataclass
class PersonFilmwork(BaseImportDataclass):
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def get_data(self):
        return (
            self.id,
            self.film_work_id,
            self.person_id,
            self.role,
            self.created_at,
        )

    def get_fields():
        return "id", "film_work_id", "person_id", "role", "created"
