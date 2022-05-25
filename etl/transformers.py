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
        "genres": data.genres,
        "title": data.title,
        "description": data.description,
        "directors": data.directors,
        "directors_names": data.directors_names,
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


def transformer_people_roles(row: dict) -> list:
    """Data transformer from film to people roles."""
    data = MovieModel(**row)
    result = []
    for someone in data.actors:
        index_template = {
            "update": {
                "_index": settings.es_scheme_persons,
                "_id": str(someone["id"]),
            }
        }
        data_template = {
            "doc" : {
                "is_actor": True,
            }
        }
        result.append(index_template)
        result.append(data_template)
    for someone in data.writers:
        index_template = {
            "update": {
                "_index": settings.es_scheme_persons,
                "_id": str(someone["id"]),
            }
        }
        data_template = {
            "doc" : {
                "is_writer": True,
            }
        }
        result.append(index_template)
        result.append(data_template)
    for someone in data.directors:
        index_template = {
            "update": {
                "_index": settings.es_scheme_persons,
                "_id": str(someone["id"]),
            }
        }
        data_template = {
            "doc" : {
                "is_director": True,
            }
        }
        result.append(index_template)
        result.append(data_template)
    return result
