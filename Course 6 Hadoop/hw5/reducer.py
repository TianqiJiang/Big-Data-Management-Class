#!/usr/bin/env python
import itertools, operator, sys

def parsePairs():
    for line in sys.stdin:
        yield tuple(line.strip('\n').split('\t'))

def reducer():
    for key, pairs in itertools.groupby(parsePairs(),
                                        operator.itemgetter(0)):
        count = sum(int(i[1]) for i in pairs)
        print '%s\t%s' % (key, count)

if __name__=='__main__':
    reducer()
