# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""
import logging
logger = logging.getLogger(__package__)
logger.setLevel(logging.DEBUG)
from typing import Self
from .schema import create_inmemory_database
from .sql import validate_schema, reload_model, add_edge, dump_database
from .verbalize import verbalize_graph
from .Query import Query


class SimpleKnowledgeGraph:
    def __init__(self, engine=None):
        if engine is None:
            self.engine = create_inmemory_database()
        else:
            self.engine = engine
        validate_schema(self.engine)
        self.model = reload_model(self.engine)
        return
    def __str__(self):
        return dump_database(self.engine)
    def reloadModel(self) -> Self:
        self.model = reload_model(self.engine)
        return self
    def verbalizeModel(self)-> str:
        return verbalize_graph(self.model)
    def addEdge(self, parent: tuple, child: tuple) -> Self:
        if type(parent) is not tuple or type(child) is not tuple or len(parent) != 2 or len(child) != 3:
            raise ValueError('Incorrect parameters sent to addEdge: {parent} should be tuple(2) and {child} should be tuple(3)')
        else:
            add_edge(self.engine, *parent, *child)
        return self
    def query(self) -> Query:
        return Query(self)
