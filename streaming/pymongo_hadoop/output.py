import sys
from bson import _dict_to_bson


class BSONOutput(object):
    """Custom file class for encoding streaming BSON,
    based upon the Dumbo "typedbytes" module at 
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
            self.fh.write(_dict_to_bson(obj, False))
        else:
            raise Exception("Can only write a Dict. No support for direct BSON Serialization of '%s'" % type(obj))

    write = _write

    def _writes(self, iterable):
        w = self._write
        for obj in iterable:
            w(obj)

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
            if len(tuple) != 2:
                raise ValueError("Key/Value Tuples can only contain 2 elements.")
        if not isinstance(obj[1], dict):
            raise ValueError("Values for KeyValueBSONOutput must be a python 'dict', not a '%s'" % type(obj[1]))

        if '_id' in obj[1]:
            print >> sys.stderr, "WARNING: Value contains an '_id', which will be overwritten by the contents of the key in KeyValueBSONOutput Mode."



    def write(self, pair):
        if isinstance(pair, tuple):
            self._validate_write(pair)
            pair[1]['_id'] = pair[0]
            self._writes(self, pair[1])
        else:
            raise ValueError("Can only write a Tuple of (<key>, <value as a dict>). No support for direct BSON Serialization of '%s'" % type(pair))

    def writes(self, iterable):
        self._writes([j for j in [i for i in iterable]])
