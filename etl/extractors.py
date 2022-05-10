# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import datetime
import sys
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, NoReturn, Tuple

import psycopg2
from config import logger
from psycopg2.extras import DictCursor


def handle_psycopg2_errors(err: Exception) -> NoReturn:
    """Handle errors for psycopg2."""
    logger.error("psycopg2 error: %s" % (" ".join(err.args)))
    logger.error("Exception class is: ", err.__class__)
    logger.error("psycopg2 traceback: ")
    exc_type, exc_value, exc_tb = sys.exc_info()
    logger.error(traceback.format_exception(exc_type, exc_value, exc_tb))
    sys.exit(11)


@contextmanager
def conn_context(config: dataclass) -> Iterator:
    """Context manager that connect to both databases."""
    try:
        conn = psycopg2.connect(**config.get_psycopg_dict(), cursor_factory=DictCursor)
    except psycopg2.OperationalError as err:
        handle_psycopg2_errors(err)
    yield conn
    conn.close()


def iter_bulk_extractor(config: dataclass, state: Any) -> Iterator:
    """Get all data from DB."""
    with conn_context(config) as conn:
        cursor = conn.cursor()

        query = """
        SELECT content.film_work.id,
        content.film_work.rating AS imdb_rating,
        ARRAY_AGG(DISTINCT content.genre.name) AS genre,
        content.film_work.title,
        content.film_work.description,
        ARRAY_AGG(DISTINCT content.person.full_name)
        FILTER(WHERE content.person_film_work.role = 'director') AS director,
        ARRAY_AGG(DISTINCT content.person.full_name)
        FILTER(WHERE content.person_film_work.role = 'actor') AS actors_names,
        ARRAY_AGG(DISTINCT content.person.full_name)
        FILTER(WHERE content.person_film_work.role = 'writer') AS writers_names,
        JSON_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name))
        FILTER(WHERE content.person_film_work.role = 'actor') AS actors,
        JSON_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name))
        FILTER(WHERE content.person_film_work.role = 'writer') AS writers
        FROM content.film_work
        LEFT OUTER JOIN content.genre_film_work ON (content.film_work.id = content.genre_film_work.film_work_id)
        LEFT OUTER JOIN content.genre ON (content.genre_film_work.genre_id = content.genre.id)
        LEFT OUTER JOIN content.person_film_work ON (content.film_work.id = content.person_film_work.film_work_id)
        LEFT OUTER JOIN content.person ON (content.person_film_work.person_id = content.person.id)
        WHERE greatest(content.film_work.modified, content.person.modified, content.genre.modified) > '%s'
        GROUP BY content.film_work.id, content.film_work.title, content.film_work.description, content.film_work.rating
        """ % state.get(
            "last_bulk_extractor"
        )

        cursor.execute(query)
        state.set(
            "last_bulk_extractor", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        while True:
            rows_batch = cursor.fetchmany(config.batch_size)
            if not rows_batch:
                break
            yield rows_batch
