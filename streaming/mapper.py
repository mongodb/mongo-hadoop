#!/usr/bin/env python

import sys

from pymongo_hadoop import BSONMapper

def mapper(documents):
    for doc in documents:
        yield {'_id': doc['_id'].year, 'bc10Year': doc['bc10Year']}

BSONMapper(mapper)
print >> sys.stderr, "Done Mapping."
