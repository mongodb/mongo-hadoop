#!/usr/bin/env python

import sys
sys.path.append(".")

from pymongo_hadoop import KeyValueBSONReducer, KeyValueBSONInput

def reducer(key, values):
    print >> sys.stderr, "Processing Key: %s" % key
    _count = _sum = 0
    for v in values:
        _count += 1
        _sum += v['value']
    return (key, _sum / _count)


KeyValueBSONReducer(reducer, input_fh=KeyValueBSONInput())
