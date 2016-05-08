#!/usr/bin/env python
import sys, time

def parseRecords():
    for line in sys.stdin:
        line = line.strip('\n')
        cell = line.split(',')
        yield int(int(cell[2])/60)

def mapper():
    for words in parseRecords():
        print '%s\t%s' % (words, 1)

if __name__=='__main__':
    mapper()
