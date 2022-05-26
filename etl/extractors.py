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
from psycopg2.extras import DictCursor

from config import logger


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


def iter_bulk_extractor(
    name: str, config: Any, query: str, batch_size: int, state: Any
) -> Iterator:
    """Get all data from DB."""
    logger.info("Connection...")
    with conn_context(config) as conn:

        cursor = conn.cursor()
        query = query % state.get(f"last_bulk_extractor_{name}")

        cursor.execute(query)
        state.set(
            f"last_bulk_extractor_{name}",
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        while True:
            rows_batch = cursor.fetchmany(batch_size)
            if not rows_batch:
                break
            yield rows_batch
