#!/usr/bin/env python

import sys

from pymongo_hadoop import KeyValueBSONInput, KeyValueBSONOutput

output = KeyValueBSONOutput()
input  = KeyValueBSONInput()

for (k, v) in input:
    output.write((k.year, v['bc10Year']))

print >> sys.stderr, "Done Mapping."
