# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import logging
import time
from datetime import datetime
from functools import wraps
from config import handle_errors

class BackoffFailException(Exception):
    pass


def backoff_decorator(func, initial=0.1, factor=2, max_timeout=3, max_tries=10):
    """Backoff functional decorator"""
     
    @wraps(func)       
    def inner(*args, **kwargs):
        timeout = initial
        attempts = 0
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as err:
                time.sleep(timeout)
                timeout = min(initial * (attempts ** factor), max_timeout)
                logging.error(f"Backoff attempt fail with timeout {timeout}, {err}")
                attempts += 1
                continue
            finally:
                if attempts == max_tries:
                    logging.error("Too many attempts in a backoff decorator")
                    raise BackoffFailException("Too many attempts in a backoff decorator") 
    return inner
