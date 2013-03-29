#!/usr/bin/env python

import sys
import os
sys.path.append(".")

try:
    from pymongo_hadoop import BSONReducer
    import pymongo_hadoop
except:
    here = os.path.abspath(__file__)
    module_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(here))),
                    'language_support',
                    'python')
    sys.path.append(module_dir)
    from pymongo_hadoop import BSONReducer

def reducer(key, values):
    print >> sys.stderr, "Processing Key: %s" % key
    _count = _sum = 0
    for v in values:
        _count += 1
        _sum += v['bc10Year']
    return {'_id': key, 'avg': _sum / _count,
            'count': _count, 'sum': _sum }

BSONReducer(reducer)
