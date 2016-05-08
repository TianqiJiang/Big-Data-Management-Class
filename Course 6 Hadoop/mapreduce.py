###############################################################################
##
## Copyright (C) 2016, NYU Center for Urban Science and Progress (NYU CUSP). 
## All rights reserved.
## Contact: huy.vo@nyu.edu
##
## This file is part of the Big Data Management & Analysis class, CUSP-GX-5008.
##
## "Redistribution and use in source and binary forms, with or without 
## modification, are permitted provided that the following conditions are met:
##
##  - Redistributions of source code must retain the above copyright notice, 
##    this list of conditions and the following disclaimer.
##  - Redistributions in binary form must reproduce the above copyright 
##    notice, this list of conditions and the following disclaimer in the 
##    documentation and/or other materials provided with the distribution.
##  - Neither the name of NYU-Poly nor the names of its 
##    contributors may be used to endorse or promote products derived from 
##    this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
## THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
## PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR 
## CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
## EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
## PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
## OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
## WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
## OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF 
## ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."
##
###############################################################################

import itertools
import operator

def run(k1v1, mapper, reducer=None):
    k2v2 = (r for g in k1v1 for r in mapper(g))
    k3v3 = ((reducer((k2, map(operator.itemgetter(1), v2s)))
            for k2, v2s in itertools.groupby(sorted(k2v2), key=operator.itemgetter(0)))
            if reducer else k2v2)
    return k3v3

def partition(stream, chunk_size):
    i = iter(stream)
    while True:
        l = list(itertools.islice(i, 0, chunk_size))
        if not l: return
        yield l

def runPartition(k1v1, mapper, reducer=None, chunk_size=10):
    k2v2 = (r
            for g in partition(k1v1, chunk_size)
            for r in mapper(g))
    k3v3 = ((reducer((k2, map(operator.itemgetter(1), v2s)))
             for k2, v2s in itertools.groupby(sorted(k2v2), key=operator.itemgetter(0)))
            if reducer else k2v2)
    return k3v3
