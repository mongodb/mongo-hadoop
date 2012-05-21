import collections
import sys
from bson import BSON


class BSONOutput(object):
    """Custom file class for decoding streaming BSON,
    based upon the Dumbo & "typedbytes" modules at
    https://github.com/klbostee/dumbo &
    https://github.com/klbostee/typedbytes
    """

    def __init__(self, fh=sys.stdout, unicode_errors='strict'):
        self.fh = fh
        self.unicode_errors = unicode_errors

    def __del__(self):
        if not self.fh.closed:
            self.fh.flush()

    def _write(self, obj):
        if isinstance(obj, dict):
            self._validate_write(obj)
            self.fh.write(BSON.encode(obj, False))
        else:
            #raise Exception("Can only write a Dict. No support for direct BSON Serialization of '%s'" % type(obj))
            #print >> sys.stderr, "Bare (or non-dict) output value '%s' found.  Wrapping in a BSON object 'value' field." % obj
            self._write({'value': obj})

    write = _write

    def _writes(self, iterable):
        w = self.write
        for obj in iterable:
            if isinstance(obj, dict) or (isinstance(obj, tuple) and len(obj) == 2):
                w(obj)
            elif isinstance(obj, collections.Iterable):
                for o in obj:
                    w(o)

    writes = _writes

    def _validate_write(self, obj):
        if '_id' not in obj:
            raise KeyError("Dictionary output for BSON Streaming must contain '_id', set to the key of the job.")

    def flush(self):
        self.fh.flush()

    def close(self):
        self.fh.close()

class KeyValueBSONOutput(BSONOutput):

    def _validate_write(self, obj):
        if isinstance(obj, tuple):
            if len(obj) != 2:
                raise ValueError("Key/Value Tuples can only contain 2 elements.")
            if isinstance(obj[1], dict) and '_id' in obj[1]:
                #print >> sys.stderr, "WARNING: Value contains an '_id', which will be overwritten by the contents of the key in KeyValueBSONOutput Mode."
                pass



    def write(self, pair):
        if isinstance(pair, tuple):
            self._validate_write(pair)
            k = pair[0]
            v = pair[1]
            if not isinstance(v, dict):
                #print >> sys.stderr, "Bare (or non-dict) output value of type '%s' found.  Wrapping in a BSON object 'value' field." % type(v)
                v = {'value': v}
            v['_id'] = k
            super(KeyValueBSONOutput, self)._write(v)
        else:
            raise ValueError("Can only write a Tuple of (<key>, <value as a dict>). No support for direct BSON Serialization of '%s'" % type(pair))

    def writes(self, iterable):
        self._writes([j for j in [i for i in iterable]])
