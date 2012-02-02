#!/usr/bin/env python
import sys

from pymongo_hadoop import BSONMapper

def mapper(documents):
    for doc in documents:
        yield {'_id': doc['user']['time_zone'], 'count': 1}

BSONMapper(mapper)
print >> sys.stderr, "Done Mapping."
