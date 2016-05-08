#!/usr/bin/env python
import sys, time

def parseRecords():
    for line in sys.stdin:
        line = line.strip('\n')
        cell = line.split('\t')
        yield (cell[0], cell[1])

def mapper():
    for c1, c2 in parseRecords():
        print '%s\t%s' % (c1, c2)

if __name__=='__main__':
    mapper()
