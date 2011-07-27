#!/usr/bin/env python

import sys

from pymongo_hadoop import BSONReducerInput, KeyValueBSONOutput

def reducer(data):
    for key, values in data:
        print >> sys.stderr, "Processing Key: %s" % key
        _count = _sum = 0
        for v in values:
            _count += 1
            _sum += v['bc10Year']
        yield (key, _sum / _count)

output = KeyValueBSONOutput()
input  = BSONReducerInput(reducer)

        
output.writes(input)
