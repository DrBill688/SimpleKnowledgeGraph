# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""
import logging
logger = logging.getLogger(__package__)
logger.setLevel(logging.DEBUG)
from sqlalchemy import create_engine, text, Engine

LABEL_MAX_LENGTH=32
VALUE_MAX_LENGTH=4096
METADATA_MAX_LENGTH=128
NUMBER_OF_EXAMPLES=1
METADATA_EXAMPLE_MAX_LENGTH=16
MODEL_TABLE='SKG_MODEL'
MODEL_TABLE_STRUCT={'parent': f'VARCHAR({LABEL_MAX_LENGTH})',
                    'child': f'VARCHAR({LABEL_MAX_LENGTH})'}
MODEL_METADATA_TABLE='SKG_METADATA'
MODEL_METADATA_TABLE_STRUCT={'label': f'VARCHAR({LABEL_MAX_LENGTH})',
                             'description': f'VARCHAR({METADATA_MAX_LENGTH})',
                             }
MODEL_METADATA_TABLE_STRUCT.update(dict([(f'example_{i}',f'VARCHAR({METADATA_EXAMPLE_MAX_LENGTH})') for i in range(1,NUMBER_OF_EXAMPLES+1)]))
CORPUS_TABLE='SKG_CORPUS'
CORPUS_TABLE_STRUCT={'parent_label': f'VARCHAR({LABEL_MAX_LENGTH})',
                     'parent_value': f'VARCHAR({VALUE_MAX_LENGTH})',
                     'child_label': f'VARCHAR({LABEL_MAX_LENGTH})',
                     'child_value': f'VARCHAR({VALUE_MAX_LENGTH})'}

def create_inmemory_database() -> Engine:
    engine = create_engine('sqlite+pysqlite:///')
    engine.connect().execute(text("CREATE TABLE {} ({})".format(MODEL_TABLE, ",".join([f'{m} {MODEL_TABLE_STRUCT[m]}' for m in MODEL_TABLE_STRUCT.keys()]))))
    engine.connect().execute(text("CREATE TABLE {} ({})".format(MODEL_METADATA_TABLE, ",".join([f'{m} {MODEL_METADATA_TABLE_STRUCT[m]}' for m in MODEL_METADATA_TABLE_STRUCT.keys()]))))
    engine.connect().execute(text("CREATE TABLE {} ({})".format(CORPUS_TABLE, ",".join([f'{m} {CORPUS_TABLE_STRUCT[m]}' for m in CORPUS_TABLE_STRUCT.keys()]))))
    return engine


