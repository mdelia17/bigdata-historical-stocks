#!/usr/bin/env python3
"""spark application"""
import argparse

from pyspark.sql import SparkSession
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"

def perc(a, b):
    return float("{:.5f}".format(float((b-a)/a*100)))

def ticker_month_reduction(line1, line2): 
    datamin, datamax, closemin, closemax = tuple(line1)
    if datamin == line2[0]:
        closemin += line2[2]
    if datamin > line2[0]:
        datamin = line2[0]
        closemin = line2[2]
    if datamax == line2[1]:
        closemin += line2[3]
    if datamax < line2[1]:
        datamax = line2[1]
        closemax = line2[3]
    return [datamin, datamax, closemin, closemax]

def partFunc(aggr, line):
    aggr[line[0]-1] = line[1]
    return aggr
 
def combFunc(aggr1, aggr2):
    for i in range(len(aggr2)): 
        if aggr2[i] != "-":
            aggr1[i] = aggr2[i]
    return aggr1
    
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--input_path2", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, input_filepath2, output_filepath = args.input_path, args.input_path2, args.output_path

spark = SparkSession \
    .builder \
    .appName("Esercizio-3") \
    .getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()
input_RDD2 = spark.sparkContext.textFile(input_filepath2).cache()

lines_RDD = input_RDD.map(lambda line: line.strip().split(","))
lines_RDD2 = input_RDD2.map(lambda line: line.strip().split(","))

prices_RDD = lines_RDD.map(lambda line: [ (line[0], datetime.strptime(line[7], TIMESTAMP_FORMAT).month), [ line[7], line[7], float("{:.5f}".format(float(line[2]))), float("{:.5f}".format(float(line[2])))] ])
stocks_RDD = lines_RDD2.map(lambda line: [ line[0], line[2] ])

prices_RDD = prices_RDD.filter(lambda line: datetime.strptime(line[1][0], TIMESTAMP_FORMAT).year == 2017)

ticker_month_min_max_RDD = prices_RDD.reduceByKey(ticker_month_reduction)

sticker_month_perc_RDD = ticker_month_min_max_RDD.map(lambda line: [line[0][0], [ line[0][1], perc(line[1][2],line[1][3])] ])

join_RDD = sticker_month_perc_RDD.join(stocks_RDD)

name_month_percent_RDD = join_RDD.map(lambda line: [ (line[1][1], line[1][0][0]), [line[1][0][1], 1]])

name_month_percent_RDD = name_month_percent_RDD.reduceByKey(lambda a, b: [a[0]+b[0], a[1]+1])

name_month_average_RDD = name_month_percent_RDD.map(lambda line: [ line[0][0], [line[0][1], float("{:.5f}".format(float(line[1][0]/line[1][1])))] ])

name_trend_RDD = name_month_average_RDD.aggregateByKey(["-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"], partFunc, combFunc)

all_pairs_RDD = name_trend_RDD.cartesian(name_trend_RDD)

SOGLIA = 1
SOGLIA_MESI_COMUNE = 6

def check_pairs(line):
    mesi_comuni = 0
    if line[0][0] != line[1][0]:
        for i in range(12):
            if line[0][1][i] == "-" and line[1][1][i] != "-" or line[0][1][i] != "-" and line[1][1][i] == "-":
                return False
            if line[0][1][i] == "-" and line[1][1][i] == "-":
                continue
            diff = abs(float(line[0][1][i]) - float(line[1][1][i])) 
            if diff <= SOGLIA:
                mesi_comuni +=1 
                continue
            else:
                return False
        return mesi_comuni >= SOGLIA_MESI_COMUNE
    else: 
        return False

all_similar_pairs_RDD = all_pairs_RDD.filter(check_pairs)

all_similar_pairs_RDD.coalesce(1,True).saveAsTextFile(output_filepath)