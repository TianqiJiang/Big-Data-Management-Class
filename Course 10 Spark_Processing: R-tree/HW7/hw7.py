import datetime
import operator
import os
import sys
import time
import pyspark

def indexZones(shapeFilename):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), zones.geometry[idx])):
            return idx
    return -1

def mapToZone(parts):
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = indexZones('neighborhoods.geojson')

    for line in parts:        
        fields = line.strip().split(',')
        if len(fields) == 18:
            if all((fields[5],fields[6],fields[9],fields[10])):
                try:
                    for i in [fields[5],fields[6],fields[9],fields[10]]:
                        float(i)
                    pickup_location  = geom.Point(proj(float(fields[5]), float(fields[6])))
                    dropoff_location = geom.Point(proj(float(fields[9]), float(fields[10])))
                    pickup_zone = findZone(pickup_location, index, zones)
                    dropoff_zone = findZone(dropoff_location, index, zones)
                    if pickup_zone>=0 and dropoff_zone>=0:
                        yield ((str(zones.borough[dropoff_zone]), str(zones.neighborhood[pickup_zone])), 1)
                except:
                    pass

if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()

    trips = sc.textFile(','.join(sys.argv[1:-1]))

    output = trips \
        .mapPartitions(mapToZone) \
        .reduceByKey(lambda a, b: (a+b))\
        .sortBy(lambda x: -x[1]) \
        .map(lambda x: x[0]) \
        .reduceByKey(lambda a, b: (a+','+b)) \
        .mapValues(lambda x: x.split(",")[:3]) \
    
    output.saveAsTextFile(sys.argv[-1])
