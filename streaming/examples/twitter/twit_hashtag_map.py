#!/usr/bin/env python

import sys
sys.path.append(".")

from pymongo_hadoop import BSONMapper

def mapper(documents):
    for doc in documents:
        for hashtag in doc['entities']['hashtags']:
            yield {'_id': hashtag['text'], 'count': 1}

BSONMapper(mapper)
print >> sys.stderr, "Done Mapping."
