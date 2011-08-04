#!/usr/bin/env python

import sys

from pymongo_hadoop import KeyValueBSONMapper

def mapper(keys_and_values):
    for (k, v) in keys_and_values:
        yield (k.year, v['bc10Year'])

KeyValueBSONMapper(mapper)
print >> sys.stderr, "Done Mapping."
