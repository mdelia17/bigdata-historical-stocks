#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession
from datetime import datetime


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--input_path2", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, input_filepath2, output_filepath = args.input_path, args.input_path2, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Esercizio-2") \
    .getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()
input_RDD2 = spark.sparkContext.textFile(input_filepath2).cache()

TIMESTAMP_FORMAT = "%Y-%m-%d"
def sum_min(line1, line2): 
    #print(line1, line2)
    datamin, datamax, closemin, closemax = tuple(line1)
    if line1[0] == line2[0]:
        closemin += line2[2]
    if line1[0] > line2[0]:
        datamin = line2[0]
        closemin = line2[2]
    if line1[1] == line2[1]:
        closemin += line2[3]
    if line1[1] < line2[1]:
        datamax = line2[1]
        closemax = line2[3]
    return [datamin, datamax, closemin, closemax]

def sum_min2(line1, line2): 
    #print(line1, line2)
    datamin, datamax, closemin, closemax, volume = tuple(line1)
    if line1[0] == line2[0]:
        closemin += line2[2]
    if line1[0] > line2[0]:
        datamin = line2[0]
        closemin = line2[2]
    if line1[1] == line2[1]:
        closemin += line2[3]
    if line1[1] < line2[1]:
        datamax = line2[1]
        closemax = line2[3] 
    return [datamin, datamax, closemin, closemax, int(volume)+int(line2[4])]

def ticker_percent_ticker_vol(line1, line2): 
    ticker_perc, percent_max, ticker_vol, vol_max = tuple(line1)
    if percent_max < line2[1]:
        percent_max = line2[1]
        ticker_perc = line2[0]
    if vol_max < line2[3]:
        vol_max = line2[3]
        ticker_vol = line2[2]
    return [ticker_perc, percent_max, ticker_vol, vol_max]
    
# ogni elemento del RDD è una lista di valori
lines_RDD = input_RDD.map(lambda line: line.strip().split(","))
lines_RDD2 = input_RDD2.map(lambda line: line.strip().split(","))

# tolgo i campi che non servono
prices_RDD = lines_RDD.map(lambda line: [line[0], [line[2], line[6], line[7]]])
stocks_RDD = lines_RDD2.map(lambda line: [line[0], line[3]])

# teniamo solo i record tra il 2009 e il 2018
prices_RDD = prices_RDD.filter(lambda line: datetime.strptime(line[1][2], TIMESTAMP_FORMAT).year >= 2009 and datetime.strptime(line[1][2], TIMESTAMP_FORMAT).year <= 2018)

# facciamo il join
join_RDD = prices_RDD.join(stocks_RDD)

join_RDD2 = join_RDD.map(lambda line: [(line[1][1], datetime.strptime(line[1][0][2], TIMESTAMP_FORMAT).year), [line[1][0][2], line[1][0][2], line[1][0][0], line[1][0][0]] ])

stock_min_max_RDD = join_RDD2.reduceByKey(sum_min)

stock_min_max_RDD = stock_min_max_RDD.map(lambda line: [line[0], (float(line[1][3])-float(line[1][2]))/float(line[1][2]) * 100])

# azione del settore con maggior incremento percentuale e volume
ticker_percent_vol_RDD = join_RDD.map(lambda line: [(line[0], datetime.strptime(line[1][0][2], TIMESTAMP_FORMAT).year, line[1][1]), [line[1][0][2], line[1][0][2], line[1][0][0], line[1][0][0], line[1][0][1]]])
# output_RDD = stock_date_first_last_percent_RDD.join(stock_min_max_RDD)
# sorted_output_RDD = output_RDD.sortBy(lambda word: word[1][0][0], ascending=False)
# sorted_output_RDD = sorted_output_RDD.map(lambda word: [word[0], word[1][0][1], word[1][0][0], word[1][0][2], word[1][1][1], word[1][1][0]])
ticker_percent_vol_RDD = ticker_percent_vol_RDD.reduceByKey(sum_min2)

ticker_percent_vol_RDD = ticker_percent_vol_RDD.map(lambda line: [(line[0][2], line[0][1]), [line[0][0], (float(line[1][3])-float(line[1][2]))/float(line[1][2]) * 100 , line[0][0], line[1][4]]])
ticker_percent_vol_RDD = ticker_percent_vol_RDD.reduceByKey(ticker_percent_ticker_vol)

final_RDD = ticker_percent_vol_RDD.join(stock_min_max_RDD)

final_RDD = final_RDD.map(lambda line: [line[0][0], line[0][1], line[1][1], line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3]] )

sorted_final_RDD = final_RDD.sortBy(lambda line: line[0], ascending=True)

# lines = final_RDD.collect()

# spark.sparkContext.parallelize([lines]) \
#                    .saveAsTextFile(output_filepath)
                  
sorted_final_RDD.coalesce(1,True).saveAsTextFile(output_filepath)
