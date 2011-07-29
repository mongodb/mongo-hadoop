import sys

from input import BSONInput, KeyValueBSONInput
from output import BSONOutput, KeyValueBSONOutput
from reducer import BSONReducer, BSONReducerInput

__all__ = ['BSONInput', 'BSONOutput',
           'KeyValueBSONOutput', 'KeyValueBSONInput',
           'BSONReducerInput', 'BSONReducer']

def dump_bits(bits):
    for bit in bits:
        print >> sys.stderr, "\t * Bit: %s Ord: %d" % (hex(ord(bit)), ord(bit))

