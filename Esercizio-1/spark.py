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
stock_prices_RDD = lines_RDD.map(lambda line: [line[0], line[1], line[2], line[4], line[5], line[7]])

stock_date_RDD = stock_prices_RDD.map(lambda fields: [fields[0], fields[5]])

stock_min_max_RDD = stock_prices_RDD.map(lambda fields: [fields[0], [fields[3], fields[4]]])

stock_min_max_RDD = stock_min_max_RDD.reduceByKey(lambda a, b: [min(float(a[0]),float(b[0])), max(float(a[1]),float(b[1]))])
stock_min_max_RDD = stock_min_max_RDD.map(lambda fields: [fields[0], (fields[1][0], fields[1][1])])

stock_date_first_RDD = stock_date_RDD.reduceByKey(func=lambda a, b: min(a,b))
stock_date_last_RDD = stock_date_RDD.reduceByKey(func=lambda a, b: max(a,b))

stock_date_first_RDD = stock_date_first_RDD.map(f=lambda fields: [(fields[0], fields[1]),1])
stock_date_last_RDD = stock_date_last_RDD.map(f=lambda fields: [(fields[0], fields[1]),1])

stock_prices_RDD = stock_prices_RDD.map(lambda fields: [(fields[0], fields[5]), fields[2]])
stock_date_first_closed_RDD = stock_prices_RDD.join(stock_date_first_RDD)
stock_date_last_closed_RDD = stock_prices_RDD.join(stock_date_last_RDD)

stock_date_first_closed_RDD = stock_date_first_closed_RDD.map(lambda fields: [fields[0][0], (fields[0][1], fields[1][0])])
stock_date_last_closed_RDD = stock_date_last_closed_RDD.map(lambda fields: [fields[0][0], (fields[0][1], fields[1][0])])

stock_date_first_last_closed_RDD = stock_date_last_closed_RDD.join(stock_date_first_closed_RDD)
stock_date_first_last_percent_RDD = stock_date_first_last_closed_RDD.map(f=lambda word: [word[0], (word[1][0][0], word[1][1][0], ((float(word[1][0][1])-float(word[1][1][1]))/float(word[1][1][1]))*100)])

output_RDD = stock_date_first_last_percent_RDD.join(stock_min_max_RDD)
sorted_output_RDD = output_RDD.sortBy(lambda word: word[1][0][0], ascending=False)
sorted_output_RDD = sorted_output_RDD.map(lambda word: [word[0], word[1][0][1], word[1][0][0], word[1][0][2], word[1][1][1], word[1][1][0]])

sorted_output_RDD.coalesce(1,True).saveAsTextFile(output_filepath)
