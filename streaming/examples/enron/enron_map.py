#!/usr/bin/env python

import sys
sys.path.append(".")

from pymongo_hadoop import BSONMapper

def mapper(documents):
    i = 0
    for doc in documents:
        i = i + 1
        if 'headers' in doc and 'To' in doc['headers'] and 'From' in doc['headers']:
            from_field  = doc['headers']['From']
            to_field = doc['headers']['To']
            recips = [x.strip() for x in to_field.split(',')]
            for r in recips:
                yield {'_id': {'f':from_field, 't':r}, 'count': 1}

BSONMapper(mapper)
print >> sys.stderr, "Done Mapping."
