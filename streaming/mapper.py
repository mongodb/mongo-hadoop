#!/usr/bin/env python

import sys

import bson
from bson import _elements_to_dict, _dict_to_bson, _get_int, InvalidBSON
import struct

def handle_row(item):
    data =  {'_id': item['_id'].year, 'bc10Year': item['bc10Year']}
    x = _dict_to_bson(data, False)
    #length = len(x)
    #l = struct.pack("<i", length)
    if not sys.stdout.closed:
        sys.stdout.write(x)
    else:
        raise Exception("STDOUT closed. Ack!")

buf = ""
size = 0

def dump_bits(bits):
    for bit in bits:
        print >> sys.stderr, "\t * Bit: %s Ord: %d" % (hex(ord(bit)), ord(bit))

class BSONInput(object):
    """Custom file class for decoding streaming BSON,
    based upon the Dumbo "typedbytes" module at 
    https://github.com/klbostee/typedbytes
    """

    def __init__(self, fh, unicode_errors='strict'):
        self.fh = fh
        self.unicode_errors = unicode_errors
        self.eof = False

    def _read(self):
        try:
            size_bits = self.fh.read(4)
            size = struct.unpack("<i", size_bits)[0] - 4 # BSON size byte includes itself 
            data = self.fh.read(size) 
            if len(data) != size:
                raise struct.error("Unable to cleanly read expected BSON Chunk; EOF, underful buffer or invalid object size.")
            if data[size - 1] != "\x00":
                raise InvalidBSON("Bad EOO in BSON Data")
            chunk = data[:size - 1]
            doc = _elements_to_dict(chunk, dict, True)
            return doc
        except struct.error, e:
            #print >> sys.stderr, "Parsing Length record failed: %s" % e
            self.eof = True
            raise StopIteration(e)

    def read(self):
        try:
            return self._read()
        except StopIteration:
            #print >> sys.stderr, "Iteration Failure: %s" % e
            return None

    def _reads(self):
        r = self._read
        while 1:
            yield r()
            
    def close(self):
        self.fh.close()

    __iter__ = reads = _reads
        

x = 0
for doc in BSONInput(sys.stdin):
    #print >> sys.stderr, "[INPUT DOC %s] %s" % (x, doc)
    x += 1
    handle_row(doc)

#for line in sys.stdin:
    ##print >> sys.stderr, "Decode. Size? %s" % size
    #buf += line
    #if size == 0:
        #n = []
        #if len(buf) > 0:
            #n = buf
            #size = struct.unpack("<i", n[:4])[0]
        ##print >> sys.stderr, "Size: %d"  % (size)

    #if len(buf) >= size:
        #chunk = buf[4:size - 1] # slice out the data section
        #row = _elements_to_dict(chunk, dict, True)
        #handle_row(row)
        #buf = buf[size:]
        #size = 0

print >> sys.stderr, "Done Mapping?"
