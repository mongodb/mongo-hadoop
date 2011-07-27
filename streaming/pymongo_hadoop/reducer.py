"""Custom file class for reducing streaming BSON,
based upon the Dumbo module at 
https://github.com/klbostee/dumbo
"""
from itertools import groupby
from operator import itemgetter
from input import KeyValueBSONInput, BSONInput
import struct

import sys

class BSONReducer(object):
    def __init__(self, factory=None):
        if factory:
            self.factory = factory
        
    def factory(self):
        """Processor factory used to consume reducer input (one per path on multiple outputs)

        Must return a callable (aka processor) that accepts two parameters
        "key" and "values", and returns an iterable of strings or None.

        The processor may have a close() method that returns an iterable of
        strings or None. This method is called when the last key-values pair
        for a path is seen.

        """
        return lambda key, values: values

class KeyValueBSONReducer(BSONReducer):
    pass

class BSONReducerInput(BSONInput):
    """Wrapper to 'roll up' the reduce data down to just
    key and values, simplifying the streaming API as much
    as humanly possible."""

    # TODO - Combiner/KeyFunc
    def __init__(self, reducefunc):
        """ 
        reducer must be a function which takes a series of 
        pairs of Key, Values to process
        """
        self.reducer = reducefunc
        super(BSONReducerInput, self).__init__()
            
    def iter_reduce(self):
        data = groupby(self._reads(), lambda doc: doc['_id'])
        data = ((key, (v for v in values)) for key, values in data)
        return self.reducer(data)
    
    __iter__ = iter_reduce

class KeyValueBSONReducerInput(KeyValueBSONInput):
    """Wrapper to 'roll up' the reduce data down to just
    key and values, simplifying the streaming API as much
    as humanly possible."""
       
    #__iter__ = reads = _reads
    pass
