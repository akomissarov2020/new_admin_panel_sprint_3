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
from state import State
from storage import RedisStorage
from transformers import transformer_films
from transformers import transformer_persons
from transformers import transformer_genres
from queries import query_films, query_genres, query_persons

class SingletonError(Exception):
    pass


@backoff_decorator
def extractor_films(config, state) -> Iterator:
    """Extractor from source database."""
    yield from iter_bulk_extractor("films", config, query_films, config.batch_size, state)

@backoff_decorator
def extractor_genres(config, state) -> Iterator:
    """Extractor from source database."""
    yield from iter_bulk_extractor("genres", config, query_genres, config.batch_size, state)

@backoff_decorator
def extractor_persons(config, state) -> Iterator:
    """Extractor from source database."""
    yield from iter_bulk_extractor("persons", config, query_persons, config.batch_size, state)


@backoff_decorator
def loader_data_to_es(es: Any, data: list, index_name: str) -> dict:
    """Data loader to the target database."""
    response = es.bulk(index=index_name, body=data, refresh=True)
    return response


@backoff_decorator
def get_instance(state, config) -> Any:
    """Get elastic search instance and init it of required."""
    es = elasticsearch.Elasticsearch([config.es_address])

    if state.get("innitiated") != "1":
        json_file_name = config.es_json_file_films
        index_name = config.es_scheme_films
        status = create_index(es, json_file_name, index_name)

        json_file_name = config.es_json_file_persons
        index_name = config.es_scheme_persons
        status = create_index(es, json_file_name, index_name)

        json_file_name = config.es_json_file_genres
        index_name = config.es_scheme_genres
        status = create_index(es, json_file_name, index_name)

        logger.info("last_bulk_extractors set to the default value")
        state.set("last_bulk_extractor_films", "1900-01-01 01:00:00")
        state.set("last_bulk_extractor_persons", "1900-01-01 01:00:00")
        state.set("last_bulk_extractor_genres", "1900-01-01 01:00:00")
        if status["acknowledged"]:
            state.set("innitiated", 1)
    return es


def check_singleton(state: Any) -> NoReturn:
    """Check that app is singleton."""

    mypid = os.getpid()
    if state.is_empty() or not state.get("pid") :
        state.set("pid", mypid)
    else:
        pid = int(state.get("pid"))
        if pid != mypid and check_pid(pid):
            raise SingletonError(f"Other process is running with pid={pid}")
        else:
            logger.info(f"Starting new process instead of pid={pid}")
            state.set("pid", os.getpid())


# @backoff_decorator
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

    total = 0
    for row_bulk in extractor_genres(config, state):
        transformed_data = sum(map(transformer_genres, row_bulk), [])
        logger.info(f"Uploading {len(transformed_data)/2} items to ES")
        if transformed_data:
            print(transformed_data)
            loader_data_to_es(es, transformed_data, config.es_scheme_genres)
            total += len(transformed_data)/2
    
    logger.info(f"Done with genres. ({total})")

    total = 0
    for row_bulk in extractor_persons(config, state):
        transformed_data = sum(map(transformer_persons, row_bulk), [])
        logger.info(f"Uploading {len(transformed_data)/2} items to ES")
        if transformed_data:
            print(transformed_data)
            loader_data_to_es(es, transformed_data, config.es_scheme_persons)
            total += len(transformed_data)/2
    
    logger.info(f"Done with persons: ({total})")

    total = 0
    for row_bulk in extractor_films(config, state):
        transformed_data = sum(map(transformer_films, row_bulk), [])
        logger.info(f"Uploading {len(transformed_data)/2} items to ES")
        if transformed_data:
            print(transformed_data)
            loader_data_to_es(es, transformed_data, config.es_scheme_films)
            total += len(transformed_data)/2

    logger.info(f"Done with films: ({total})")


if __name__ == "__main__":
    while True:
        main(force=True)
        time.sleep(1000)
