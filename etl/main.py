# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import os
import sys
import datetime
import elasticsearch
from backoff import backoff_decorator
from initiation import create_index
from storage import RedisStorage
from state import State
from config import logger, Settings, check_pid
from extractors import bulk_extractor
from models import MovieModel
import time


class SingletonError(Exception):
    pass


@backoff_decorator
def bulk_upload_to_es(es, data, index_name):
    response = es.bulk(index=index_name, body=data, refresh=True)
    return response


@backoff_decorator
def main(force=False):

    config = Settings()

    storage = RedisStorage(config)
    state = State(storage)

    if force:
        state.clear()

    logger.info(f"Current state: {state}")

    es = elasticsearch.Elasticsearch([config.es_address])

    ### Singleton pattern by unique pid
    mypid = os.getpid()
    if state.is_empty():
        state.set("pid", mypid)
        status = create_index(es, config)
        logger.info("last_bulk_extractor set to the default value")
        state.set("last_bulk_extractor", "1900-01-01 01:00:00")
        if status["acknowledged"]:
            state.set("innitiated", 1)
    else:
        pid = int(state.get("pid"))
        if pid != mypid and check_pid(pid):
            raise SingletonError(f"Other process is running with pid={pid}")
        else:
            logger.info(f"Starting new process instead of pid={pid}")
            state.set("pid", os.getpid())

    strings_to_es = []
    for row in bulk_extractor(config, state):
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
        strings_to_es.append(index_template)
        strings_to_es.append(data_template)

    logger.info(f"Uploading {len(strings_to_es)/2} items to ES")
    if strings_to_es:
        bulk_upload_to_es(es, strings_to_es, config.es_scheme)
    logger.info("Done.")


if __name__ == "__main__":
    while True:
        main(force=False)
        time.sleep(10)
