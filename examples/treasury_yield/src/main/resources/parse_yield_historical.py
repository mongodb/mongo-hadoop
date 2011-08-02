#!/usr/bin/env python

from xml.dom.minidom import parse
from pymongo import Connection
from datetime import datetime

conn = Connection()
db = conn.test
mongo = db['yield_historical.in']

dom = parse('yield_historical_Jan90_Sep10.xml')

data = dom.firstChild.firstChild

def parseDecimal(input):
    try:
        return float(input.strip())
    except Exception, e:
        return None

for entry in [item for item in data.childNodes if not item.getElementsByTagName("BOND_MKT_UNAVAIL")[0].hasChildNodes()]:
    t_item = {}
    try:
        t_item['_id'] = datetime.strptime(entry.getElementsByTagName("NEW_DATE")[0].firstChild.wholeText.strip(), "%m-%d-%Y")
    except:
        print "Bad date ... '%s'" %  entry.getElementsByTagName("NEW_DATE")[0].firstChild.wholeText
    t_item['dayOfWeek'] = entry.getElementsByTagName("DAY_OF_WEEK")[0].firstChild.wholeText
    t_item['bc1Month'] = parseDecimal(entry.getElementsByTagName("BC_1MONTH")[0].firstChild.wholeText)
    t_item['bc3Month'] = parseDecimal(entry.getElementsByTagName("BC_3MONTH")[0].firstChild.wholeText)
    t_item['bc6Month'] = parseDecimal(entry.getElementsByTagName("BC_6MONTH")[0].firstChild.wholeText)
    t_item['bc1Year'] = parseDecimal(entry.getElementsByTagName("BC_1YEAR")[0].firstChild.wholeText)
    t_item['bc2Year'] = parseDecimal(entry.getElementsByTagName("BC_2YEAR")[0].firstChild.wholeText)
    t_item['bc3Year'] = parseDecimal(entry.getElementsByTagName("BC_3YEAR")[0].firstChild.wholeText)
    t_item['bc5Year'] = parseDecimal(entry.getElementsByTagName("BC_5YEAR")[0].firstChild.wholeText)
    t_item['bc7Year'] = parseDecimal(entry.getElementsByTagName("BC_7YEAR")[0].firstChild.wholeText)
    t_item['bc10Year'] = parseDecimal(entry.getElementsByTagName("BC_10YEAR")[0].firstChild.wholeText)
    t_item['bc20Year'] = parseDecimal(entry.getElementsByTagName("BC_20YEAR")[0].firstChild.wholeText)
    t_item['bc30Year'] = parseDecimal(entry.getElementsByTagName("BC_30YEAR")[0].firstChild.wholeText)
    mongo.save(t_item)

