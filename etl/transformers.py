# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

from models import MovieModel
from models import PersonModel
from models import GenreModel
from config import Settings

settings = Settings()

def transformer_films(row: dict) -> list:
    """Data transformer."""
    data = MovieModel(**row)
    index_template = {
        "index": {
            "_index": settings.es_scheme_films,
            "_id": str(data.id),
        }
    }
    data_template = {
        "id": str(data.id),
        "imdb_rating": data.imdb_rating,
        "genre": data.genre,
        "title": data.title,
        "description": data.description,
        "director": data.director,
        "actors_names": data.actors_names,
        "writers_names": data.writers_names,
        "actors": data.actors,
        "writers": data.writers,
    }
    return [index_template, data_template]


def transformer_genres(row: dict) -> list:
    """Data transformer."""
    data = GenreModel(**row)
    index_template = {
        "index": {
            "_index": settings.es_scheme_genres,
            "_id": str(data.id),
        }
    }
    data_template = {
        "id": str(data.id),
        "name": data.name,
        "description": data.description,
    }
    return [index_template, data_template]


def transformer_persons(row: dict) -> list:
    """Data transformer."""
    data = PersonModel(**row)
    index_template = {
        "index": {
            "_index": settings.es_scheme_persons,
            "_id": str(data.id),
        }
    }
    data_template = {
        "id": str(data.id),
        "full_name": data.full_name,
        "is_actor": False,
        "is_director": False,
        "is_writer": False,
    }
    return [index_template, data_template]