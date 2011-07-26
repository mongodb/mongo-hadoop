#!/usr/bin/env python

import sys

import bson
from bson import _elements_to_dict, _dict_to_bson, _get_int
import struct

def handle_row(item):
    data =  {'_id': item['_id'].year, 'bc10Year': item['bc10Year']}
    x = _dict_to_bson(data, False)
    length = len(x)
    print >> sys.stderr, "Output Length: %s" % length
    l = struct.pack("<i", length)
    if not sys.stdout.closed:
        print, l + x
    else:
        raise Exception("STDOUT closed. Ack!")

buf = ""
size = 0
for line in sys.stdin:
    #print >> sys.stderr, "Decode. Size? %s" % size
    buf += line
    if size == 0:
        n = []
        if len(buf) > 0:
            n = buf
            size = struct.unpack("<i", n[:4])[0]
        print >> sys.stderr, "Size: %d"  % (size)

    if len(buf) >= size:
        chunk = buf[4:size - 1] # slice out the data section
        row = _elements_to_dict(chunk, dict, True)
        handle_row(row)
        buf = buf[size:]
        size = 0

print >> sys.stderr, "Done Mapping?"
