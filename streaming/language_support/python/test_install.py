#!/usr/bin/env python

try:
    import pymongo
    from bson import _elements_to_dict, InvalidBSON
except:
    raise Exception("Cannot find a valid pymongo installation.")

try:
    from pymongo_hadoop import BSONInput
except:
    raise Exception("Cannot find a valid pymongo_hadoop installation.")

print "*** Everything looks OK. All required modules were found."
