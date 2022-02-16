from bson import InvalidBSON, BSON
from bson.codec_options import CodecOptions

import sys
import struct

STREAMING_CODEC_OPTIONS = CodecOptions(tz_aware=True)


class BSONInput(object):
    """Custom file class for decoding streaming BSON,
    based upon the Dumbo & "typedbytes" modules at
    https://github.com/klbostee/dumbo &
    https://github.com/klbostee/typedbytes
    """

    def __init__(self, fh=sys.stdin.buffer, unicode_errors='strict'):
        self.fh = fh
        self.unicode_errors = unicode_errors
        self.eof = False

    def _read(self):
        try:
            size_bits = self.fh.read(4)
            size = struct.unpack("<i", size_bits)[0] - 4 # BSON size byte includes itself 
            data = size_bits + self.fh.read(size)
            if len(data) != size + 4:
                raise struct.error("Unable to cleanly read expected BSON Chunk; EOF, underful buffer or invalid object size.")
            if data[size + 4 - 1] != 0:
                raise InvalidBSON("Bad EOO in BSON Data")
            doc = BSON(data).decode(codec_options=STREAMING_CODEC_OPTIONS)
            return doc
        except struct.error as e:
            #print >> sys.stderr, "Parsing Length record failed: %s" % e
            self.eof = True
            raise StopIteration(e)

    def read(self):
        try:
            return self._read()
        except StopIteration as e:
            print("Iteration Failure: %s" % e, file=sys.stderr)
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
        except StopIteration as e:
            print("Key/Value Input iteration failed/stopped: %s" % e, file=sys.stderr)
            return None
        if '_id' in doc:
            return doc['_id'], doc
        else:
            raise struct.error("Cannot read Key '_id' from Input Doc '%s'" % doc)

    def reads(self):
        it = self._reads()
        n = it.__next__
        while 1:
            doc = n()
            if '_id' in doc:
                yield doc['_id'], doc
            else:
               raise struct.error("Cannot read Key '_id' from Input Doc '%s'" % doc)

    __iter__ = reads
