import sys

from input import BSONInput, KeyValueBSONInput
from output import BSONOutput, KeyValueBSONOutput
from reducer import BSONReducer, KeyValueBSONReducer
from reducer import BSONReducerInput, KeyValueBSONReducerInput

__all__ = ['BSONInput', 'BSONOutput',
           'KeyValueBSONOutput', 'KeyValueBSONInput',
           'BSONReducerInput', 'KeyValueBSONReducerInput',
           'BSONReducer', 'KeyValueBSONReducer']

def dump_bits(bits):
    for bit in bits:
        print >> sys.stderr, "\t * Bit: %s Ord: %d" % (hex(ord(bit)), ord(bit))

