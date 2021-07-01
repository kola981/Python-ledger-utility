'''
Created on Jan 15, 2021

@author: ak981
'''

def mergeColumnNames(columns, _dict):
    l = []
    for val in _dict.values():
        l.extend(val)
    new_columns = set(l)
    new_columns.discard('')
    new_columns = sorted(new_columns, key=str.lower, reverse=False)
    columns.extend(new_columns)
    return columns

