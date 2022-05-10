# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Loadind data from sqlite3 to postgres."""
import logging
import os
import sqlite3
import sys
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Iterator, NoReturn, Tuple

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Identifier, Placeholder

from models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)


table2dataclass = {
    "genre": Genre,
    "genre_film_work": GenreFilmwork,
    "person_film_work": PersonFilmwork,
    "person": Person,
    "film_work": Filmwork,
}


@dataclass
class UploadSettings:
    """Dataclass for settings."""

    localdb: os.PathLike = os.environ.get("DB_SQLITE")
    dbname: str = os.environ.get("DB_NAME")
    output_dbname: str = os.environ.get("DB_PREFIX")
    user: str = os.environ.get("POSTGRES_USER")
    password: str = os.environ.get("POSTGRES_PASSWORD")
    host: str = os.environ.get("SQL_HOST")
    port: int = os.environ.get("SQL_PORT")
    batch_size: int = 100

    def get_psycopg_dict(self) -> dict:
        """Get subset of settings for psycopg connection."""
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }


def handle_sqlite3_errors(err: Exception) -> NoReturn:
    """Handle errors for sqlite3."""
    logging.error("SQLite error: %s" % (" ".join(err.args)))
    logging.error("Exception class is: ", err.__class__)
    logging.error("SQLite traceback: ")
    exc_type, exc_value, exc_tb = sys.exc_info()
    logging.error(traceback.format_exception(exc_type, exc_value, exc_tb))
    sys.exit(10)


def handle_psycopg2_errors(err: Exception) -> NoReturn:
    """Handle errors for psycopg2."""
    logging.error("psycopg2 error: %s" % (" ".join(err.args)))
    logging.error("Exception class is: ", err.__class__)
    logging.error("psycopg2 traceback: ")
    exc_type, exc_value, exc_tb = sys.exc_info()
    logging.error(traceback.format_exception(exc_type, exc_value, exc_tb))
    sys.exit(11)


@contextmanager
def conn_context(settings: UploadSettings) -> Iterator[Tuple[sqlite3.Row, _connection]]:
    """Context manager that connect to both databases."""
    try:
        conn = sqlite3.connect(settings.localdb)
    except sqlite3.Error as err:
        handle_sqlite3_errors(err)

    conn.row_factory = sqlite3.Row

    pg_conn = None

    try:
        pg_conn = psycopg2.connect(**settings.get_psycopg_dict(), cursor_factory=DictCursor)
    except psycopg2.OperationalError as err:
        handle_psycopg2_errors(err)

    yield conn, pg_conn

    conn.close()
    pg_conn.close()


def iter_sqlite_db(
    cursor: sqlite3.Cursor, table_name: str, batch_size: int
) -> Iterator:
    """Iterate over sqllite db"""
    try:
        cursor.execute(f"SELECT * FROM {table_name};")
    except sqlite3.Error as err:
        handle_sqlite3_errors(err)
    while True:
        rows = cursor.fetchmany(size=batch_size)
        if not rows:
            break
        yield rows


def upload_table(
    curs: sqlite3.Cursor,
    pg_cur: psycopg2.extras.DictCursor,
    model: dataclass,
    table_name: str,
    db_name: str,
    batch_size: int,
) -> NoReturn:
    """Upload data from sqlite3 to postgres database."""

    fields_ = model.get_fields()
    col_names = SQL(", ").join(Identifier(name) for name in fields_)
    place_holders = SQL(", ").join(Placeholder() * len(fields_))
    sql = SQL(
        "INSERT INTO {db_name}.{table_name} ({col_names}) VALUES ({values}) ON CONFLICT DO NOTHING;"
    ).format(
        db_name=Identifier(db_name),
        table_name=Identifier(table_name),
        col_names=col_names,
        values=place_holders,
    )

    for batch in iter_sqlite_db(curs, table_name, batch_size):
        try:
            pg_cur.executemany(sql, batch)
        except Exception as err:
            handle_psycopg2_errors(err)


def load_from_sqlite(settings: UploadSettings) -> NoReturn:
    """Основной метод загрузки данных из SQLite в Postgres"""

    with conn_context(settings) as (conn, pg_conn):
        cur = conn.cursor()
        pg_cur = pg_conn.cursor()
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        except sqlite3.Error as err:
            handle_sqlite3_errors(err)

        for i, item in enumerate(cur.fetchall()):
            table_name = item["name"]
            if not table_name in table2dataclass:
                continue
            db_name = settings.output_dbname
            logging.info(table_name)
            dataclass_ = table2dataclass[table_name]
            upload_table(cur, pg_cur, dataclass_, table_name, db_name, settings.batch_size)
        try:
            pg_conn.commit()
        except Exception as err:
            handle_psycopg2_errors(err)


if __name__ == "__main__":

    logging.info("Starting data export...")

    settings = UploadSettings(
        batch_size=100,
    )

    load_from_sqlite(settings)
