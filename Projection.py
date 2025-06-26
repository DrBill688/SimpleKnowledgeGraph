# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""
from typing import Self
import re, datetime, networkx
from .sql import run_query
from .enumerations import SetOperator, Restriction, EvalOperator

class Projection:
    def __init__(self, G, root_type):
        self.skg = G
        self.root_type = root_type
        self.result_graph = networkx.DiGraph()
        self.current_root_list = []
        return
    def __add__(self, restriction) -> Self:
        datatype = str
        if len(self.skg.model['metadata'][restriction.label]['examples']) > 0:
            datatype = self.determine_type(self.skg.model['metadata'][restriction.label]['examples'][0])
        result = run_query(self.skg.engine, datatype, restriction, list(networkx.all_shortest_paths(self.skg.model['graph'], self.root_type, restriction.label)))
        result_root_list = []
        for record in result:
            result_root_list.extend(self.insert_edges(restriction.set_operator, record))
        if restriction.set_operator == SetOperator.AND:
            [self.result_graph.remove_node(m) for m in self.current_root_list if m not in result_root_list]
        self.current_root_list = list(self.result_graph.successors((None,None)))
        return self
    def insert_edges(self, set_op, record) -> list:
        print(f'INSERTING EDGES FROM: {record}')
        result_root_list = []
        for i in range(1, int(len(record)/4)+1):
            if record[0+(i-1)*4] == self.root_type:
                result_root_list.append((record[0+(i-1)*4], record[1+(i-1)*4]))
                if  (set_op == SetOperator.OR) or \
                    ((set_op == SetOperator.AND) and 
                     ((record[0+(i-1)*4], record[1+(i-1)*4]) in self.current_root_list)):
                    self.result_graph.add_edge((None, None), (record[0+(i-1)*4], record[1+(i-1)*4]))
                    self.result_graph.add_edge((record[0+(i-1)*4], record[1+(i-1)*4]), (record[2+(i-1)*4], record[3+(i-1)*4]))
            else:
                self.result_graph.add_edge((record[0+(i-1)*4], record[1+(i-1)*4]), (record[2+(i-1)*4], record[3+(i-1)*4]))
        return result_root_list
    def fill(self, field_list: list) -> Self:
        for field in field_list:
            result = run_query(self.skg.engine, list, Restriction(SetOperator.OR, self.root_type, EvalOperator.IN, [m[1] for m in self.current_root_list]), list(networkx.all_shortest_paths(self.skg.model['graph'].reverse(), field, self.root_type, field)), direction="reverse")
            for record in result:
                self.insert_edges(SetOperator.OR, record)
        return self
    def as_dict(self, start=(None,None)) -> dict:
        result = {}
        for child in self.result_graph.successors(start):
            if child[0] != self.root_type:
                if child[0] in result.keys():
                    print(f'{child[0]} exists: {result[child[0]]} of type: {type(result[child[0]])}')
                    if type(result[child[0]]) == str:
                        print('Converting to a list')
                        result[child[0]] = [result[child[0]]]
                        print(f'{child[0]} is now: {result[child[0]]} of type: {type(result[child[0]])}')
                    result[child[0]].append(child[1])
                    print(f'{child[0]} appended resulting in : {result[child[0]]} of type: {type(result[child[0]])}')
                else:
                    result[child[0]] = child[1] 
            childres = self.as_dict(child)
            if len(list(childres.keys())) > 0:
                result[child[1]] = childres
        return result
    def determine_type(self, example_str):
        float_re = r'^([0-9]*[.])?[0-9]+'
        bool_re = r'^(TRUE|FALSE)'
        date_re1 = r'^(\d{4})-(\d{2})-(\d{2})'
        date_re2 = r'^(\d{2})-(\S{3})-(\d{4})'
        var = example_str
        if re.fullmatch(float_re, var) is not None:
            var = float(var)
        elif re.fullmatch(bool_re, var.upper()) is not None:
            var = var.upper() == 'TRUE'
        elif re.fullmatch(date_re1, var.upper()) is not None:
            var = datetime.date(var)
        elif re.fullmatch(date_re2, var.upper()) is not None:
            var = datetime.datetime.strptime(var, '%d-%b-%Y').date()
        return type(var)
        