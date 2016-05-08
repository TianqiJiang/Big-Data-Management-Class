import csv
import sys

if __name__=='__main__':
    if len(sys.argv)<2:
        sys.stderr.write('USAGE: python %s <INPUT_CSV>\n' % sys.argv[0])
        sys.exit(1)

    ageDistribution = {}
    totalCount = 0
    
    with open(sys.argv[1], 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            if row['usertype']=='Subscriber':
                age = 2016-int(row['birth_year'])
                ageDistribution[age] = ageDistribution.get(age, 0) + 1
                totalCount += 1

    medianCount = 0
    for age,count in sorted(ageDistribution.iteritems()):
        medianCount += count
        if medianCount>=totalCount/2:
            print age
            break

