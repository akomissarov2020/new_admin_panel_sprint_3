# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

import os
import elasticsearch
from backoff import backoff_decorator
from config import Settings, logger
import json

@backoff_decorator
def create_index(es, config):
    """Create index according to given json file"""

    json_file_name = config.es_json_file
    index_name = config.es_scheme
    if es.indices.exists([index_name]):
        logger.info(f"Deleting existing index ({index_name}) in ES")
        es.indices.delete(index=index_name, ignore=[400, 404])

    with open(json_file_name) as fh:
        data = json.load(fh)

    logger.info(f"Creating index ({index_name}) in ES")
    response = es.indices.create(index=index_name, body=data)
    logger.info(f"Done.")
    return response
