from pyspark import SparkContext
from csv_parser import csvRDD
import csv
import sys

def extractScores(index, lines):
    if index==0:
        lines.next()
    reader = csv.reader(lines)
    for row in reader:
        if row[2]!='s':
            (dbn, takers, score) = (row[0], int(row[2]), int(row[4]))
            yield (dbn, (score*takers, takers))

def extractSchools(lines):
    for row in lines:
        if row[17].isdigit():
            (dbn, boro, total_students) = (row[0], row[2], int(row[17]))
            if total_students>500:
                yield (dbn, boro)
            
if __name__=='__main__':
    sc = SparkContext()

    SAT_FN = 'SAT_Results.csv'
    HSD_FN = 'DOE_High_School_Directory_2014-2015.csv'

    sat = sc.textFile(SAT_FN, use_unicode=False).cache()
    schools = sc.textFile(HSD_FN, use_unicode=False).cache()

    
    satScores = sat.mapPartitionsWithIndex(extractScores)
    largeSchools = csvRDD(schools).mapPartitions(extractSchools)

    largeSchools.join(satScores) \
        .values() \
        .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
        .mapValues(lambda x: x[0]/x[1]) \
        .sortBy(lambda x: -x[1]) \
        .saveAsTextFile(sys.argv[-1])
