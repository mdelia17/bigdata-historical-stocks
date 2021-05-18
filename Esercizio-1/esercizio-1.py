#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession
from datetime import datetime


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Esercizio-1") \
    .getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()

TIMESTAMP_FORMAT = "%Y-%m-%d"
lines_RDD = input_RDD.map(lambda line: line.strip().split(","))
stock_prices_RDD = lines_RDD.map(lambda line: [line[0], [float(line[2]), float(line[2]), float(line[4]), float(line[5]), line[7], line[7]]])

# stock_date_RDD = stock_prices_RDD.map(lambda fields: [fields[0], fields[5]])

# stock_min_max_RDD = stock_prices_RDD.map(lambda fields: [fields[0], [fields[3], fields[4]]])

def aggregate(line1, line2):
    firstclose, lastclose, min, max, firstdate, lastdate = tuple(line1)
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
    return [firstclose, lastclose, min, max, firstdate, lastdate]

stock_prices_RDD = stock_prices_RDD.reduceByKey(aggregate)    
stock_date_first_last_percent_RDD = stock_prices_RDD.map(lambda line: [line[0], [line[1][4], line[1][5], (float(line[1][1])-float(line[1][0]))/float(line[1][0])*100, line[1][3], line[1][2]]])

sorted_output_RDD = stock_date_first_last_percent_RDD.sortBy(lambda line: line[1][1], ascending=False)
                  
sorted_output_RDD.coalesce(1,True).saveAsTextFile(output_filepath)
# stock_date_first_last_percent_RDD.saveAsTextFile(output_filepath)
