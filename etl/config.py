# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import logging
import os
import sys
import traceback
from dataclasses import dataclass
from typing import Iterator, NoReturn, Tuple

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """Dataclass for settings."""

    localdb: os.PathLike = os.environ.get("DB_SQLITE")
    dbname: str = os.environ.get("DB_NAME")
    output_dbname: str = os.environ.get("DB_PREFIX")
    user: str = os.environ.get("DB_USER")
    password: str = os.environ.get("DB_PASSWORD")
    host: str = "127.0.0.1"
    port: int = 5432
    batch_size: int = 100

    es_scheme: str = os.getenv("ELASTIC_SCHEME")
    es_address: str = os.getenv("ELASTIC_ADDRESS")
    es_json_file: os.PathLike = os.environ.get("ELASTIC_JSON_FILE")

    redis_db: str = os.getenv("REDIS_DB")
    redis_port: int = os.getenv("REDIS_PORT")
    redis_host: str = os.getenv("REDIS_HOST")

    def get_psycopg_dict(self) -> dict:
        """Get subset of settings for psycopg connection."""
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }

    def get_redis_dict(self) -> dict:
        """Get redis connection dict."""
        return {
            "host": self.redis_host,
            "port": self.redis_port,
            "db": self.redis_db,
        }


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger()


def handle_errors(err: Exception) -> NoReturn:
    """Handle errors for sqlite3."""
    logger.error("Error: %s" % (" ".join(err.args)))
    logger.error("Exception class is: %s" % err.__class__)
    logger.error("Traceback: ")
    exc_type, exc_value, exc_tb = sys.exc_info()
    logger.error(traceback.format_exception(exc_type, exc_value, exc_tb))
    sys.exit(10)


def check_pid(pid: int) -> bool:
    """Check For the existence of a unix pid."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True
