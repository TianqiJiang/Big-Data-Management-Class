import csv
import sys
import dateutil
import seaborn 


def citibike2hod(filename):	
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            birth = dateutil.parser.parse(row['birth_year'])
            age = 2016- birth.year
            yield age

count = {}
for hod in citibike2hod(sys.argv[0]):
    count[hod] = count.get(hod, 0)+1

current = 0
total = sum(count.itervalues())
for k,v in sorted(count.iteritems()):
    current += v
    if current*2>=total:
        print k
        break