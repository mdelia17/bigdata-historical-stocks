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
    .appName("Esercizio-3") \
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
    
# ogni elemento del RDD è una lista di valori
lines_RDD = input_RDD.map(lambda line: line.strip().split(","))
lines_RDD2 = input_RDD2.map(lambda line: line.strip().split(","))

# tolgo i campi che non servono
prices_RDD = lines_RDD.map(lambda line: [ (line[0], datetime.strptime(line[7], TIMESTAMP_FORMAT).month), [ line[7], line[7], line[2], line[2] ] ])
stocks_RDD = lines_RDD2.map(lambda line: [ line[0], line[2] ])

# teniamo solo i record nel 2017
prices_RDD = prices_RDD.filter(lambda line: datetime.strptime(line[1][0], TIMESTAMP_FORMAT).year == 2017)

# facciamo il join
# join_RDD = prices_RDD.join(stocks_RDD)

stock_min_max_RDD = prices_RDD.reduceByKey(sum_min)

stock_min_max_RDD = stock_min_max_RDD.map(lambda line: [line[0][0], [ line[0][1], (float(line[1][3])-float(line[1][2]))/float(line[1][2]) * 100] ])

join_RDD = stock_min_max_RDD.join(stocks_RDD)

join_RDD = join_RDD.map(lambda line: [ (line[1][1], line[1][0][0]), [line[1][0][1], 1]])

join_RDD = join_RDD.reduceByKey(lambda a, b: [a[0]+b[0], a[1]+1])

join_RDD = join_RDD.map(lambda line: [ line[0][0], [line[0][1], line[1][0]/line[1][1]] ])

def seqFunc(acc, teams):
    acc[teams[0]-1] = teams[1]
    return acc
 
def combFunc(acc1, acc2):
    for i in range(len(acc2)): 
        if acc2[i] != "-":
            acc1[i] = acc2[i]
    return acc1

join_RDD = join_RDD.aggregateByKey(["-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"], seqFunc, combFunc)

join_RDD = join_RDD.cartesian(join_RDD)

# # azione del settore con maggior incremento percentuale e volume
# ticker_percent_vol_RDD = join_RDD.map(lambda line: [(line[0], datetime.strptime(line[1][0][2], TIMESTAMP_FORMAT).year, line[1][1]), [line[1][0][2], line[1][0][2], line[1][0][0], line[1][0][0], line[1][0][1]]])
# # output_RDD = stock_date_first_last_percent_RDD.join(stock_min_max_RDD)
# # sorted_output_RDD = output_RDD.sortBy(lambda word: word[1][0][0], ascending=False)
# # sorted_output_RDD = sorted_output_RDD.map(lambda word: [word[0], word[1][0][1], word[1][0][0], word[1][0][2], word[1][1][1], word[1][1][0]])
# ticker_percent_vol_RDD = ticker_percent_vol_RDD.reduceByKey(sum_min2)

# ticker_percent_vol_RDD = ticker_percent_vol_RDD.map(lambda line: [(line[0][2], line[0][1]), [line[0][0], (float(line[1][3])-float(line[1][2]))/float(line[1][2]) * 100 , line[0][0], line[1][4]]])
# ticker_percent_vol_RDD = ticker_percent_vol_RDD.reduceByKey(ticker_percent_ticker_vol)

# final_RDD = ticker_percent_vol_RDD.join(stock_min_max_RDD)

# final_RDD = final_RDD.map(lambda line: [line[0][0], line[0][1], line[1][1], line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3]] )

# sorted_final_RDD = final_RDD.sortBy(lambda line: line[0], ascending=True)

record = join_RDD.first()
app_RDD = spark.sparkContext.parallelize([record])
cart_RDD = join_RDD.cartesian(app_RDD)

lines2 = cart_RDD.collect()

spark.sparkContext.parallelize([lines2]) \
                   .saveAsTextFile(output_filepath)

# def rispetta_soglia(): 

# for record in lines:
#     app_RDD = spark.sparkContext.parallelize([record])
#     cart_RDD = join_RDD.cartesian(app_RDD)
#     cart_filtered_RDD = cart_RDD.filter(rispetta_soglia)
                  
# sorted_final_RDD.coalesce(1,True).saveAsTextFile(output_filepath)
