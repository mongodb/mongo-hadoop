#!/usr/bin/env python

import sys

from pymongo_hadoop import BSONInput, BSONOutput

output = BSONOutput()
input  = BSONInput()

for doc in input:
    output.write({'_id': doc['_id'].year, 'bc10Year': doc['bc10Year']})


print >> sys.stderr, "Done Mapping."
