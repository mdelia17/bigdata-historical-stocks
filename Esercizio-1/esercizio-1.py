#!/usr/bin/env python3
"""spark application"""
import argparse

from pyspark.sql import SparkSession
from datetime import datetime
TIMESTAMP_FORMAT = "%Y-%m-%d"

def percentuale(a, b):
    return float("{:.5f}".format(float((b-a)/a*100)))
    
def aggregate(line1, line2):
    firstclose, lastclose, min, max, firstdate, lastdate, open, close, date, curr_streak, max_streak, max_year_streak = tuple(line1)
    if min > line2[2]:
        min = line2[2]
    if max < line2[3]:
        max = line2[3]
    if firstdate > line2[4]:
        firstdate = line2[4]
        firstclose = line2[0]
    if lastdate < line2[5]:
        lastdate = line2[5]
        lastclose = line2[1]

    if close<=open and curr_streak == 1: 
        curr_streak = 0
        max_year_streak = 0

    if line2[7]>line2[6]:
        if (line2[8]-date).days == 1:
            curr_streak = curr_streak + 1
        else: 
            curr_streak = 1
        if curr_streak >= max_streak:
            max_streak = curr_streak
            max_year_streak = line2[8].year
    else:   
        curr_streak = 0 

    return [firstclose, lastclose, min, max, firstdate, lastdate, line2[6], line2[7], line2[8], curr_streak, max_streak, max_year_streak]

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Esercizio-1") \
    .getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()

lines_RDD = input_RDD.map(lambda line: line.strip().split(","))

sorted_lines_RDD = lines_RDD.sortBy(lambda line: (line[0], line[7]), ascending=True)

stock_prices_RDD = sorted_lines_RDD.map(lambda line: [line[0], [float("{:.5f}".format(float(line[2]))), float("{:.5f}".format(float(line[2]))), float("{:.5f}".format(float(line[4]))), float("{:.5f}".format(float(line[5]))), line[7], line[7], float("{:.5f}".format(float(line[1]))), float("{:.5f}".format(float(line[2]))), datetime.strptime(line[7], TIMESTAMP_FORMAT), 1, 0, 0]])

stock_prices_RDD = stock_prices_RDD.reduceByKey(aggregate)     

stock_date_first_last_percent_RDD = stock_prices_RDD.map(lambda line: [line[0], [line[1][4], line[1][5], percentuale(line[1][0],line[1][1]), line[1][3], line[1][2], line[1][10], line[1][11]]])

sorted_output_RDD = stock_date_first_last_percent_RDD.sortBy(lambda line: line[1][1], ascending=False)

sorted_output_RDD.coalesce(1,True).saveAsTextFile(output_filepath)                  
