#!/usr/bin/env python

import sys
sys.path.append(".")

from pymongo_hadoop import BSONReducer

def reducer(key, values):
    print >> sys.stderr, "Processing Key: %s" % key
    _count = _sum = 0
    for v in values:
        _count += 1
        _sum += v['bc10Year']
    return {'_id': key, 'average': _sum / _count}

BSONReducer(reducer)
