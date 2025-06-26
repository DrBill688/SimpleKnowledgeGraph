# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""
import logging
logger = logging.getLogger(__package__)
logger.setLevel(logging.DEBUG)
from typing import Self
from .enumerations import Restriction
from .Projection import Projection

class Query:

    def __init__(self, G):
        self.skg = G
        self.root_type = None
        self.include_labels = []
        self.restrictions = []
        return
    def perspective(self, label: str) -> Self:
        self.root_type = label
        return self
    def include(self, label_list: list) -> Self:
        self.include_labels.extend(label_list)
        self.include_labels = list(set(self.include_labels))
        return self
    def add_restriction(self, sop: str, l:str, eo:str, rv:str) -> Self:
        assert(l in self.skg.model['metadata'].keys())
        self.restrictions.append(Restriction(sop.upper(),l,eo.upper(),rv))
        return self
    def result(self) -> dict:
        #run the query
        projection = Projection(self.skg, self.root_type)
        for restriction in self.restrictions:
            projection += restriction
        return projection.fill(self.include_labels).as_dict()