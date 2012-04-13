from bson import _elements_to_dict, InvalidBSON

import sys
import struct

class BSONInput(object):
    """Custom file class for decoding streaming BSON,
<<<<<<< HEAD
    based upon the Dumbo & "typedbytes" modules at
    https://github.com/klbostee/dumbo &
=======
    based upon the Dumbo "typedbytes" module at
>>>>>>> develop/multipleinputs
    https://github.com/klbostee/typedbytes
    """

    def __init__(self, fh=sys.stdin, unicode_errors='strict'):
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
        except StopIteration, e:
            print >> sys.stderr, "Iteration Failure: %s" % e
            return None

    def _reads(self):
        r = self._read
        while 1:
            yield r()

    def close(self):
        self.fh.close()

    __iter__ = reads = _reads

class KeyValueBSONInput(BSONInput):
    def read(self):
        try:
            doc = self._read()
        except StopIteration, e:
            print >> sys.stderr, "Key/Value Input iteration failed/stopped: %s" % e
            return None
        if '_id' in doc:
            return doc['_id'], doc
        else:
            raise struct.error("Cannot read Key '_id' from Input Doc '%s'" % doc)

    def reads(self):
        it = self._reads()
        n = it.next
        while 1:
            doc = n()
            if '_id' in doc:
                yield doc['_id'], doc
            else:
               raise struct.error("Cannot read Key '_id' from Input Doc '%s'" % doc)

    __iter__ = reads
