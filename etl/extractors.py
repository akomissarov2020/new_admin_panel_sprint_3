# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import psycopg2
from config import logger
from backoff import backoff_decorator
from typing import Iterator, NoReturn, Tuple
from contextlib import contextmanager
import datetime
from psycopg2.extras import DictCursor


def handle_psycopg2_errors(err: Exception) -> NoReturn:
    """Handle errors for psycopg2."""
    logging.error("psycopg2 error: %s" % (" ".join(err.args)))
    logging.error("Exception class is: ", err.__class__)
    logging.error("psycopg2 traceback: ")
    exc_type, exc_value, exc_tb = sys.exc_info()
    logging.error(traceback.format_exception(exc_type, exc_value, exc_tb))
    sys.exit(11)


@contextmanager
def conn_context(config):
    """Context manager that connect to both databases."""
    try:
        conn = psycopg2.connect(**config.get_psycopg_dict(), cursor_factory=DictCursor)
    except psycopg2.OperationalError as err:
        handle_psycopg2_errors(err)
    yield conn
    conn.close()


@backoff_decorator
def bulk_extractor(config, state):
    """Get all data from DB."""
    with conn_context(config) as conn:
        cursor = conn.cursor()
        cursor.execute("""
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
        """ % state.get("last_bulk_extractor"))
        rows = cursor.fetchall()
        state.set("last_bulk_extractor", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S:"))
        return rows
