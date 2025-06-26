# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""
from sqlalchemy import text, inspect, Engine
import networkx as nx
import datetime
from .schema import NUMBER_OF_EXAMPLES, METADATA_EXAMPLE_MAX_LENGTH
from .schema import MODEL_TABLE, MODEL_METADATA_TABLE, CORPUS_TABLE
from .schema import MODEL_TABLE_STRUCT, MODEL_METADATA_TABLE_STRUCT, CORPUS_TABLE_STRUCT
from .enumerations import Restriction, EvalOperator


def validate_schema(engine: Engine) -> bool:
    isValid = True
    iengine = inspect(engine)
    if len(set(iengine.get_table_names()) & set([MODEL_TABLE, MODEL_METADATA_TABLE, CORPUS_TABLE])) != 3:
        isValid = False
    else:
        for table_decl in [(MODEL_TABLE, MODEL_TABLE_STRUCT),
                           (MODEL_METADATA_TABLE, MODEL_METADATA_TABLE_STRUCT),
                           (CORPUS_TABLE, CORPUS_TABLE_STRUCT)]:
            actual_columns = [c['name'] for c in iengine.get_columns(table_decl[0])]
            for column in table_decl[1].keys():
                if column not in actual_columns:
                    raise ValueError(f'{column} is missing from {table_decl[0]} table')
    if isValid == False:
        raise ValueError('Invalid schema.')
    return

def dump_database(engine: Engine) -> str:
    rcModel = list(engine.connect().execute(text(f'SELECT COUNT(*) from {MODEL_TABLE}')))[0][0]
    edgesModel = list(engine.connect().execute(text(f'SELECT * from {MODEL_TABLE}')))
    rcMeta = list(engine.connect().execute(text(f'SELECT COUNT(*) from {MODEL_METADATA_TABLE}')))[0][0]
    rcCorpus = list(engine.connect().execute(text(f'SELECT COUNT(*) from {CORPUS_TABLE}')))[0][0]
    return f'Model has {rcModel} edges, Metadata exists for {rcMeta} node types, and the Corpus holds {rcCorpus} edges.\n{edgesModel}'

def reload_model(engine: Engine) -> dict:
    model = {"graph": nx.DiGraph(),
             "metadata": {}
             }
    result = engine.connect().execute(text("SELECT {} from {} WHERE {} IS NOT NULL".format(",".join(MODEL_TABLE_STRUCT.keys()), MODEL_TABLE, list(MODEL_TABLE_STRUCT.keys())[0])))
    for row in result:
        model["graph"].add_edge(row[0], row[1])
    result = engine.connect().execute(text("SELECT {} from {}".format(",".join(MODEL_METADATA_TABLE_STRUCT.keys()), MODEL_METADATA_TABLE)))
    for row in result:
        model["metadata"][row[0]] = {'description':row[1],
                                     'examples': [m for m in row[-NUMBER_OF_EXAMPLES:] if m is not None]}
    return model    

def add_edge(engine: Engine, parent_label, parent_value, child_label, child_value, child_label_description) -> None:
    add_model(engine, parent_label, child_label, child_label_description, child_value)
    add_corpus(engine, parent_label, parent_value, child_label, child_value)
    return

def add_model(engine: Engine, parent, child, child_description, child_example) -> None:
    try:
        existing = list(engine.connect().execute(text(f'SELECT COUNT(*) from {MODEL_TABLE} where parent{"=:parent" if parent is not None else " is NULL"} and child=:child'), {'parent': parent, 'child': child}))[0][0]
        if existing == 0:
            engine.connect().execute(text('INSERT INTO {}({}) VALUES(:parent, :child)'.format(MODEL_TABLE, ",".join(list(MODEL_TABLE_STRUCT.keys())))), {'parent':parent, 'child':child})
    except:
        pass 
    add_model_metadata(engine, child, child_description, child_example)
    return

def add_model_metadata(engine: Engine, child, child_description, child_example) -> None:
    def adjusted_example(ex):
        if type(ex) == str:
            return ex if len(ex) < METADATA_EXAMPLE_MAX_LENGTH else '{}...'.format(ex[:METADATA_EXAMPLE_MAX_LENGTH])
        else:
            return ex
    result = list(engine.connect().execute(text(f'SELECT * from {MODEL_METADATA_TABLE} where {list(MODEL_METADATA_TABLE_STRUCT.keys())[0]} = :child'), {'child':child}))
    if len(result) == 0:
        fields = list(MODEL_METADATA_TABLE_STRUCT.keys())[:2]
        engine.connect().execute(text('INSERT INTO {}({}, example_1) VALUES(:label, :desc, :example)'.format(MODEL_METADATA_TABLE, ",".join(fields))), {'label':child, 'desc':child_description, 'example': adjusted_example(child_example)})
    else:
        existing = list(result[0]) #update the first null example
        if None in existing[-NUMBER_OF_EXAMPLES:] and child_example not in existing[-NUMBER_OF_EXAMPLES:]:
            engine.connect().execute(text('UPDATE {} SET {}=:example WHERE {}=:label'.format(MODEL_METADATA_TABLE, f'example_{existing.index(None, len(existing)-NUMBER_OF_EXAMPLES) - (len(existing)-NUMBER_OF_EXAMPLES) + 1}', list(MODEL_METADATA_TABLE_STRUCT.keys())[0])),{'label':child, 'example':adjusted_example(child_example)})
    return

def add_corpus(engine: Engine, parent_label, parent_value, child_label, child_value) -> None:
    if parent_label is not None:
        engine.connect().execute(text('INSERT INTO {}({}) VALUES(:parent_label, :parent_value, :child_label, :child_value)'.format(CORPUS_TABLE, ",".join(list(CORPUS_TABLE_STRUCT.keys())))), {'parent_label': parent_label,'parent_value': parent_value, 'child_label': child_label, 'child_value':child_value})
    return

def run_query(engine: Engine, datatype: type, restriction: Restriction, list_of_paths: list, direction = "forward"):
    print('RUNNING QUERY')
    def operator(op:EvalOperator) -> str:
        if op.lower() == 'eq':
            return '=='
        elif  op.lower() == 'ne':
            return '!='
        elif  op.lower() == 'gt':
            return '>'
        elif  op.lower() == 'lt':
            return '<'
        elif  op.lower() == 'in':
            return 'IN'
        elif  op.lower() == 'ni':
            return 'NOT IN'
        else:
            raise ValueError(f'Unknown operator: {op}')
    def typeCastColumn(tbl: str, dt: type, s: str) -> str:
        if dt == str or dt == list:
            return f'{tbl}.{s}'
        elif dt == float:
            return f'CAST({tbl}.{s} AS NUMERIC)'
        elif dt == bool:
            return f'{tbl}.{s}'
        elif dt == datetime.date:
            return f'DATE({tbl}.{s})'
        else:
            raise ValueError(f'Unknown type for casting: {dt}')
    def typeCastValue(dt: type, s: str|list) -> str:
        if dt == str:
            return f'"{s}"'
        elif dt == float:
            return f'CAST("{s}" AS NUMERIC)'
        elif dt == bool:
            return f'"{s}"'
        elif dt == datetime.date:
            return f'DATE("{s}")'
        elif dt == list:
            return '({})'.format(','.join([f'"{m}"' for m in s ]))
        else:
            raise ValueError(f'Unknown type for casting: {dt}')
    query = ""
    if direction == "forward":
        for path in list_of_paths:
            joincount = len(path) - 1
            selectClause ='{}'.format(','.join([f'{chr(ord("a")+m)}.*' for m in range(0, joincount)])) 
            fromClause = '{}'.format(','.join([f'{CORPUS_TABLE} {chr(ord("a")+m)}' for m in range(0, joincount)]))
            whereClause = 'a.{} == "{}" AND {} {} {}'.format(list(CORPUS_TABLE_STRUCT.keys())[2], restriction.label, typeCastColumn('a', datatype, list(CORPUS_TABLE_STRUCT.keys())[3]), operator(restriction.eval_operator), typeCastValue(datatype, restriction.restriction_value))
            tableLinks = ' AND '.join(['{} == {} and {} == {}'.format(f'{chr(ord("a")+m+1)}.child_label', f'{chr(ord("a")+m)}.parent_label',f'{chr(ord("a")+m+1)}.child_value', f'{chr(ord("a")+m)}.parent_value') for m in range(0,joincount-1)])
            if joincount > 1:
                tableLinks += f' AND {chr(ord("a")+joincount-1)}.parent_label == "{path[0]}"'
            query='select {}\nfrom {}\nwhere {} {} {}\n'.format(selectClause, fromClause, whereClause, 'AND' if joincount > 1 else '', tableLinks)      
            #print(query)
    else: # assume "reverse"
        for path in list_of_paths:
            joincount = len(path)-1
            selectClause ='{}'.format(','.join([f'{chr(ord("a")+m)}.*' for m in range(0, joincount)])) 
            fromClause = '{}'.format(','.join([f'{CORPUS_TABLE} {chr(ord("a")+m)}' for m in range(0, joincount)]))
            whereClause = 'a.{} == "{}" AND {} {} {}'.format(list(CORPUS_TABLE_STRUCT.keys())[0], restriction.label, typeCastColumn('a', datatype, list(CORPUS_TABLE_STRUCT.keys())[1]), operator(restriction.eval_operator), typeCastValue(datatype, restriction.restriction_value))
            tableLinks = ' AND '.join(['{} == {} and {} == {}'.format(f'{chr(ord("a")+m+1)}.parent_label', f'{chr(ord("a")+m)}.child_label',f'{chr(ord("a")+m+1)}.parent_value', f'{chr(ord("a")+m)}.child_value') for m in range(0,joincount-1)])
            tableLinks += f' AND {chr(ord("a")+joincount-1)}.child_label == "{path[0]}"'
            query='select {}\nfrom {}\nwhere {} {} {}\n'.format(selectClause, fromClause, whereClause, 'AND' if joincount > 1 else '', tableLinks)      
    return engine.connect().execute(text(query))
