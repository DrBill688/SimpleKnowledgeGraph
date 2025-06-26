# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""

def verbalize_subtree(G: dict, thisnode:str, indent_level:int = 0) -> str:
    result = '{}{}{}'.format("--".join(['' for n in range(0, indent_level+1)]),' ' if indent_level > 0 else '', thisnode)
    metastring = ""
    metadata = G['metadata'].get(thisnode)
    metastring += ' ('
    if metadata is not None:
        if metadata.get('description'):
            metastring += f"{metadata['description']}"
            examples = metadata.get('examples')
            if examples is not None and len(examples) > 0:
                metastring += " Ex. {}".format(', '.join([f'"{m}"' for m in examples]))
    metastring += ')'
    result += '{}\n'.format(metastring if metastring != ' ()' else '')
    for child in G['graph'].successors(thisnode):
        result += verbalize_subtree(G, child, indent_level+1)
    return result

def verbalize_graph(G: dict) -> str:
    res = ""
    for rootnode in {n for n, d in G['graph'].in_degree() if d == 0}:
        res += verbalize_subtree(G, rootnode)
    return res
