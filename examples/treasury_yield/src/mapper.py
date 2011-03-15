#!/usr/bin/env python

import sys

import bson
import struct

buf = ""
print >> sys.stderr, sys.stdin
   #print >> sys.stderr, buf
    #print >> sys.stderr, "Size: %d" % struct.unpack("<i", data[:4])[0]
    #print >> sys.stderr, bson.decode_all(data)

#print >> sys.stderr, buf
#print >> sys.stderr, "Size: %d / Len: %d" % (struct.unpack("<i", buf[:4])[0], len(buf))
#print >> sys.stderr, bson.decode_all(buf)
