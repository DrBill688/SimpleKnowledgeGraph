# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""
import logging
logger = logging.getLogger(__package__)
logger.setLevel(logging.DEBUG)


class SKGDeclaration:
    def __init__(self, graph, record):
        from SimpleKnowledgeGraph import SimpleKnowledgeGraph
        self.graph = graph
        if type(self.graph) != SimpleKnowledgeGraph:
            raise TypeError(f'Graph is not a SimpleKnowledgeGraph, it is a {str(type(self.graph))}')
        self.record = record
        self.key = []
        self.non_key_children = []
        self.non_key_parents = []
        return
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            constructedKey = None
            if self.graph is None:
                raise ValueError("No graph")
            if len(self.key) == 0:
                raise ValueError("No primary key defined")
            else:
                if len(self.key) > 1:
                    constructedKey = ( '***({})'.format('|'.join([m[0] for m in self.key])),
                                       '***({})'.format('|'.join([str(m[1]) for m in self.key])),
                                       '***This is an n-dimensional constructed linkage.')
                    for parent in self.key:
                        self.graph.addEdge((parent[0],parent[1]), constructedKey)
                else:
                    constructedKey = self.key[0]
            if len(self.non_key_parents) == 0:
                self.graph.addEdge((None,None), constructedKey)
            for edge in self.non_key_parents:
                self.graph.addEdge((None,None), edge)
                self.graph.addEdge((edge[0], edge[1]), constructedKey)
            for edge in self.non_key_children:
                self.graph.addEdge((constructedKey[0], constructedKey[1]), edge)
        return True
    def node(self, label, column, primary_key = False, description=None, transform_function = None, reverse=False):
        if self.record.get(column) is not None and str(self.record[column]) != 'nan':
            value = self.record[column]
            if transform_function is not None:
                value = transform_function(value)
            if primary_key:
                self.key.append((label, value, description))
            else:
                if not reverse:
                    self.non_key_children.append((label, value, description))
                else:
                    self.non_key_parents.append((label, value, description))
        return self
