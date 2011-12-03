#!/usr/bin/env python

import sys

from pymongo_hadoop import KeyValueBSONMapper

def mapper(entries):
    for (k, v) in entries:
        yield (k.year, v['bc10Year'])

KeyValueBSONMapper(mapper)
print >> sys.stderr, "Done Mapping."
