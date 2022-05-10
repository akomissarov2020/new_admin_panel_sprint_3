# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import datetime
import os
import sys
import time
from typing import Any, Iterator, NoReturn, Optional

import elasticsearch
from backoff import backoff_decorator
from config import Settings, check_pid, logger
from extractors import iter_bulk_extractor
from initiation import create_index
from models import MovieModel
from state import State
from storage import RedisStorage


class SingletonError(Exception):
    pass


@backoff_decorator
def extractor(config, state) -> Iterator:
    """Extractor from source database."""
    yield from iter_bulk_extractor(config, state)


def transformer(row: dict) -> list:
    """Data transformer."""
    data = MovieModel(**row)
    index_template = {
        "index": {
            "_index": "movies",
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


@backoff_decorator
def loader(es: Any, data: list, index_name: str) -> dict:
    """Data loader to the target database."""
    response = es.bulk(index=index_name, body=data, refresh=True)
    return response


@backoff_decorator
def get_instance(state, config) -> Any:
    """Get elastic search instance and init it of required."""
    es = elasticsearch.Elasticsearch([config.es_address])

    if state.get("innitiated") != "1":
        status = create_index(es, config)
        logger.info("last_bulk_extractor set to the default value")
        state.set("last_bulk_extractor", "1900-01-01 01:00:00")
        if status["acknowledged"]:
            state.set("innitiated", 1)
    return es


def check_singleton(state: Any) -> NoReturn:
    """Check that app is singleton."""

    mypid = os.getpid()
    if state.is_empty():
        state.set("pid", mypid)
    else:
        pid = int(state.get("pid"))
        if pid != mypid and check_pid(pid):
            raise SingletonError(f"Other process is running with pid={pid}")
        else:
            logger.info(f"Starting new process instead of pid={pid}")
            state.set("pid", os.getpid())


@backoff_decorator
def get_storage() -> RedisStorage:
    """Get storage."""
    config = Settings()
    storage = RedisStorage(config)
    return storage, config


def configuration(force: bool) -> tuple:
    """Configure ETL."""
    storage, config = get_storage()
    state = State(storage)

    check_singleton(state)

    if force:
        state.clear()
    es = get_instance(state, config)
    logger.info(f"Current state: {state}")

    return config, state, es

@backoff_decorator
def main(force=False) -> NoReturn:
    """The entrypoint function."""
    config, state, es = configuration(force)

    datasource = extractor(config, state)

    for row_bulk in extractor(config, state):
        transformed_data = sum(map(transformer, row_bulk), [])
        logger.info(f"Uploading {len(transformed_data)/2} items to ES")
        if transformed_data:
            print(transformed_data)
            loader(es, strings_to_es, config.es_scheme)
    logger.info("Done.")


if __name__ == "__main__":
    while True:
        main(force=False)
        time.sleep(10)
