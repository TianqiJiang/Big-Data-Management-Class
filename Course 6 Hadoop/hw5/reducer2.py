#!/usr/bin/env python
import itertools, operator, sys

def parsePairs():
    for line in sys.stdin:
        yield tuple(line.strip('\n').split('\t'))

def reducer():
    a = []
    b = 0

    for key, pairs in (parsePairs()):
        a.append([int(key), pairs])
        b = b + int(pairs)

    b = b / 2
    a = sorted(a)
    c = 0
   
    for i in a:
        c = c + int(i[1])
        if c>=b:
            print "The median of trip duration is", i[0], "min"
            break
    
if __name__=='__main__':
    reducer()
