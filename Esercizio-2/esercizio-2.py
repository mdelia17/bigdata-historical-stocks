#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"

# sector_year_reduction
def sector_year_reduction(line1, line2): 
    #print(line1, line2)
    datamin, datamax, closemin, closemax = tuple(line1)
    if datamin == line2[0]:
        closemin += line2[2]
    if datamin > line2[0]:
        datamin = line2[0]
        closemin = line2[2]
    if datamax == line2[1]:
        closemax += line2[3]
    if datamax < line2[1]:
        datamax = line2[1]
        closemax = line2[3]
    return [datamin, datamax, closemin, closemax]

# ticker_year_reduction
def ticker_year_reduction(line1, line2): 
    #print(line1, line2)
    datamin, datamax, closemin, closemax, volume = tuple(line1)
    # if line1[0] == line2[0]:
    #     closemin += line2[2]
    if datamin > line2[0]:
        datamin = line2[0]
        closemin = line2[2]
    # if line1[1] == line2[1]:
    #     closemin += line2[3]
    if datamax < line2[1]:
        datamax = line2[1]
        closemax = line2[3] 
    somma_vol = volume + line2[4]
    return [datamin, datamax, closemin, closemax, somma_vol]

# best_ticker_reduction
def best_ticker_reduction(line1, line2): 
    ticker_perc, percent_max, ticker_vol, vol_max = tuple(line1)
    # vol_max = int(vol_max)
    # vol2 = int(line2[3])
    if percent_max < line2[1]:
        percent_max = line2[1]
        ticker_perc = line2[0]
    if vol_max < line2[3]:
        vol_max = line2[3]
        ticker_vol = line2[2]
    return [ticker_perc, percent_max, ticker_vol, vol_max]

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--input_path2", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, input_filepath2, output_filepath = args.input_path, args.input_path2, args.output_path

spark = SparkSession \
    .builder \
    .appName("Esercizio-2") \
    .getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()
input_RDD2 = spark.sparkContext.textFile(input_filepath2).cache()

lines_RDD = input_RDD.map(lambda line: line.strip().split(","))
lines_RDD2 = input_RDD2.map(lambda line: line.strip().split(","))

prices_RDD = lines_RDD.map(lambda line: [line[0], [ float(line[2]), int(line[6]), line[7]]])
stocks_RDD = lines_RDD2.map(lambda line: [line[0], line[3]])

prices_filtered_RDD = prices_RDD.filter(lambda line: datetime.strptime(line[1][2], TIMESTAMP_FORMAT).year >= 2009 and datetime.strptime(line[1][2], TIMESTAMP_FORMAT).year <= 2018)

join_RDD = prices_filtered_RDD.join(stocks_RDD)

sector_year_RDD = join_RDD.map(lambda line: [(line[1][1], datetime.strptime(line[1][0][2], TIMESTAMP_FORMAT).year), [line[1][0][2], line[1][0][2], float(line[1][0][0]), float(line[1][0][0])] ])

sector_year_min_max_RDD = sector_year_RDD.reduceByKey(sector_year_reduction)

sector_year_percent_RDD = sector_year_min_max_RDD.map(lambda line: [line[0], (float(line[1][3])-float(line[1][2]))/float(line[1][2]) * 100])

ticker_year_RDD = join_RDD.map(lambda line: [(line[0], datetime.strptime(line[1][0][2], TIMESTAMP_FORMAT).year, line[1][1]), [line[1][0][2], line[1][0][2], line[1][0][0], line[1][0][0], line[1][0][1]]])

ticker_year_RDD = ticker_year_RDD.reduceByKey(ticker_year_reduction)

sector_percent_vol_RDD = ticker_year_RDD.map(lambda line: [(line[0][2], line[0][1]), [line[0][0], (float(line[1][3])-float(line[1][2]))/float(line[1][2]) * 100 , line[0][0], line[1][4]]])
sector_percent_vol_RDD = sector_percent_vol_RDD.reduceByKey(best_ticker_reduction)

output_RDD = sector_percent_vol_RDD.join(sector_year_percent_RDD)

output_RDD = output_RDD.map(lambda line: [line[0][0], line[0][1], line[1][1], line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3]] )

sorted_output_RDD = output_RDD.sortBy(lambda line: line[0], ascending=True)
                  
sorted_output_RDD.coalesce(1,True).saveAsTextFile(output_filepath)
