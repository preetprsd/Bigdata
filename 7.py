# Program3: Write a MapReduce program to analyze the given Earthquake Data and generate statistics with region and magnitude/ region and depth/ region and latitude/ region and longitude.

from mrjob.job import MRJob
import csv
import sys

class MREarthquake(MRJob):
    def mapper(self,_,line):
        row = next(csv.reader([line]))
        
        if row[9] == "Region":
            return
        
        region = row[9]
        magnitude = row[6]
        depth = row[7]
        latitude = row[4]
        longitude = row[5]
        
        # sys.stderr.write((region + '\n').encode())

        yield region, (magnitude, depth, latitude, longitude)
        
    def reducer(self, key, values):
        total_mag = 0
        total_depth = 0
        total_lat = 0
        total_long = 0
        
        cnt = 0
        
        for value in values:
            total_mag += float(value[0])
            total_depth += float(value[1])
            total_lat += float(value[2])
            total_long += float(value[3])
            cnt += 1
            
        avg_mag = total_mag/cnt
        avg_dpt = total_depth/cnt
        avg_lat = total_lat/cnt
        avg_long = total_long/cnt
        
        yield key, (avg_mag, avg_dpt, avg_lat, avg_long)
        
if _name_ == '_main_':
    MREarthquake.run()