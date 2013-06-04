import sys
import struct
import pymongo
from bson import BSON
import os

SPLIT_SIZE = 64 * 1024 * 1024

def main(argv):
    split_bson(argv[0])

def split_bson(path):
    bsonfile_path = os.path.abspath(path)
    splitsfile_out = os.path.join(os.path.dirname(bsonfile_path),  "." + os.path.basename(bsonfile_path) + ".splits")
    bsonfile = open(bsonfile_path,'r')
    splitsfile = open(splitsfile_out,'w')

    file_position = 0
    cur_split_start = 0
    cur_split_size = 0
    while True:
        size_bits = bsonfile.read(4)
        if len(size_bits) < 4:
            if cur_split_size > 0:
                #print {"start":cur_split_start, "length": bsonfile.tell() - cur_split_start}
                splitsfile.write(BSON.encode({"s":long(cur_split_start), "l": long(bsonfile.tell() - cur_split_start)}))
            break
        size = struct.unpack("<i", size_bits)[0] - 4 # BSON size byte includes itself 
        file_position += 4
        if cur_split_size + 4 + size > SPLIT_SIZE:
            #print {"start":cur_split_start, "length": bsonfile.tell() - 4 - cur_split_start}
            splitsfile.write(BSON.encode({"s":long(cur_split_start), "l": long(bsonfile.tell() - 4 - cur_split_start)}))
            cur_split_start = bsonfile.tell() - 4
            cur_split_size = 0
        else:
            pass

        bsonfile.seek(file_position + size)
        file_position += size
        cur_split_size += 4 + size
    
if __name__ == '__main__':
    main(sys.argv[1:])
