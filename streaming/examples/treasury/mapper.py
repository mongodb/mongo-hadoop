#!/usr/bin/env python

import sys
import os
sys.path.append(".")

try:
    from pymongo_hadoop import BSONMapper
    import pymongo_hadoop
    print >> sys.stderr, "pymongo_hadoop is not installed or in path - will try to import from source tree."
except:
    here = os.path.abspath(__file__)
    module_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(here))),
                    'language_support',
                    'python')
    sys.path.append(module_dir)
    print >> sys.stderr, sys.path
    from pymongo_hadoop import BSONMapper

def mapper(documents):
    print >> sys.stderr, "Running python mapper."

    for doc in documents:
        yield {'_id': doc['_id'].year, 'bc10Year': doc['bc10Year']}

    print >> sys.stderr, "Python mapper finished."

BSONMapper(mapper)
