#!/usr/bin/env python

import sys

from operator import itemgetter

for line in sys.stdin:
    print >> sys.stderr, type(line)

