"""Custom file class for reducing streaming BSON,
based upon the Dumbo module at
https://github.com/klbostee/dumbo
"""
from itertools import groupby
from input import BSONInput, KeyValueBSONInput
from output import BSONOutput, KeyValueBSONOutput

import sys

import inspect # HACKY!

class BSONReducer(object):

    output = BSONOutput()

    def __init__(self, factory=None, input_fh=None):
        if factory:
            # Try to figure out if they gave us a factory
            # or a straight function
            spec = inspect.getargspec(factory)
            if len(spec.args) == 2:
                self.factory = lambda: factory
            elif len(spec.args) == 0:
                self.factory = factory
            else:
                raise ValueError("Invalid Factory.  Must return a function expecting 2 arguments or be a function expecting 2 arguments.")

        if not input_fh:
            self.input = BSONReducerInput(self)
        else:
            self.input = input_fh


        self.output.writes(self.input)

    def __call__(self, data):
        for key, values in data:
            yield self.factory()(key, values)

    def factory(self):
        """Processor factory used to consume reducer input
        Must return a callable (aka processor) that accepts two parameters
        "key" and "values", and returns an iterable of strings or None.
        """
        return lambda key, values: {'values': [v for v in values]}

class KeyValueBSONReducer(BSONReducer):

    output = KeyValueBSONOutput()

    def __init__(self, factory=None, input_fh=None):
        super(KeyValueBSONReducer, self).__init__(factory, input_fh)



def default_reducer(data):
    print >> sys.stderr, "*** Invoking default reducer function, this is unoptimized for your data and may be very slow."

class BSONReducerInput(BSONInput):
    """Wrapper to 'roll up' the reduce data down to just
    key and values, simplifying the streaming API as much
    as humanly possible."""


    # TODO - Combiner/KeyFunc
    def __init__(self, reducefunc):
        """
        reducer must be a function which takes a series of
        pairs of Key, Values to process and returns an iterable
        of (key, value) tuples OR a dict with key in _id
        (recommended as a generator)
        """
        self.reducer = reducefunc
        super(BSONReducerInput, self).__init__()


    def iter_reduce(self):
        data = groupby(self._reads(), lambda doc: doc['_id'])
        data = ((key, (v for v in values)) for key, values in data)
        return self.reducer(data)

    __iter__ = iter_reduce

class KeyValueBSONReducerInput(BSONReducerInput, KeyValueBSONInput):
    def __init__(self, reducefunc):
        self.reducer = reducefunc
        super(KeyValueBSONInput, self).__init__()

    def iter_reduce(self):
        data = groupby(self._reads(), lambda k, v: k)
        data = ((key, (v for v in values)) for key, values in data)
        return self.reducer(data)

    __iter__ = iter_reduce

