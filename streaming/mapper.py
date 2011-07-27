#!/usr/bin/env python

import sys

import bson
from bson import BSON, _elements_to_dict, _dict_to_bson
import struct

def handle_row(item):
    x =  _dict_to_bson({'_id': item['_id'].year,
                         'bc10Year': item['bc10Year']}, False)
    print >> sys.stderr, "Writing Out: %s" % x
    print x

data = []
buf = ""
size = 0
for line in sys.stdin:
    buf += line
#    if not size:
        #size = struct.unpack("<i", buf[:4])[0]
        #print >> sys.stderr, "Size: %d" % size

    #if len(buf) >= size:
        #print >> sys.stderr, "[ %d ] Attempting decode of %d bytes expected %d" % (len(data), len(buf), size)
        #chunk = buf[4:size - 1] # slice out the data section
        ##print >> sys.stderr, "[%d] Chunk: %s" % (len(chunk), chunk)
        #try:
            #row = _elements_to_dict(chunk, dict, True)
        #except Exception, e:
            #print >> sys.stderr, "Failed parsing: %s" % e
        #data += row
        #handle_row(row)
        ##print >> sys.stderr, "Data: %s" % data
        #buf = buf[size - 1:]
        ##print >> sys.stderr, "[%d] Buf: %s" % (len(buf), buf)
        #size = 0

    #print >> sys.stderr, "Size: %d" % struct.unpack("<i", data[:4])[0]
    #print >> sys.stderr, bson.decode_all(data)

#print >> sys.stderr, bson.decode_all(buf)
#print >> sys.stderr, "Size: %d / Len: %d" % (struct.unpack("<i", buf[:4])[0], len(buf))
data =  bson.decode_all(buf)
for row in data:
    handle_row(row)
