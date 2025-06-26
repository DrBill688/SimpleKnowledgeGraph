# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""

from enum import Enum
class SetOperator(str, Enum):
    OR='OR'
    AND='AND'
class EvalOperator(str, Enum):
    IN="IN"
    NOTIN="NI"
    EQUALS='EQ'
    NOTEQUALS='NE'
    LESSTHAN='LT'
    GREATERTHAN='GT'
class Restriction():
    def __init__(self, sop: str, l:str, eo:str, rv:str|list):
        self.set_operator = SetOperator(sop)
        self.label = l
        self.eval_operator = EvalOperator(eo)
        self.restriction_value = rv
        return
