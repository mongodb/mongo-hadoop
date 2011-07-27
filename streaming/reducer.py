#!/usr/bin/env python

import sys

from pymongo_hadoop import BSONReducerInput, KeyValueBSONOutput

output = KeyValueBSONOutput()
input  = BSONReducerInput()

for entry in input:
    print >> sys.stderr, "Value: %s" % entry

