#!/usr/bin/env python3
"""reducer.py"""

import sys

name_2_set ={}
keyList = [1,2,3,4,5,6,7,8,9,10,11,12]
soglia = 1
soglia_mesi_comune = 6
lista_mesi = ["GEN", "FEB", "MAR", "APR", "MAG", "GIU", "LUG", "AGO", "SETT", "OTT", "NOV", "DIC"]
for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    name = line[0]

    name_2_set[name] = line[1:]

similarity_dict_2_set = {}
def update (name1, name2, similarity): 
    if name1 not in similarity_dict_2_set: 
        similarity_dict_2_set[name1] = [(name2,similarity)]
    else: 
        similarity_dict_2_set[name1].append((name2,similarity))
    if name2 not in similarity_dict_2_set: 
        similarity_dict_2_set[name2] = [(name1,similarity)]
    else: 
        similarity_dict_2_set[name2].append((name1,similarity))

keys = list(name_2_set.keys())
for x in range(len(keys)-1):
    i = keys[x]
    for y in range(x+1,len(keys)):
        j = keys[y]
        delta = 0
        mesi_comuni = 0
        flag = False
        for k in range(12):
            if name_2_set[i][k] == "-" and name_2_set[j][k] != "-" or name_2_set[i][k] != "-" and name_2_set[j][k] == "-":
                flag = True
                break
            if name_2_set[i][k] == "-" and name_2_set[j][k] == "-":
                continue
            diff = abs(float(name_2_set[i][k]) - float(name_2_set[j][k])) 
            if diff < soglia: 
                delta += diff
                mesi_comuni +=1
            else:
                flag = True
                break
        if flag==False and mesi_comuni >= soglia_mesi_comune:
            # dobbiamo fare la divisioni di delta per i mesi_comuni
            update(i, j, delta)

for key in similarity_dict_2_set:
    nome_sim_min = ""
    sim_min = 12
    similarity_dict_2_set[key]
    for elem in similarity_dict_2_set[key]:
        if elem[1] < sim_min: 
            nome_sim_min = elem[0]
            sim_min = elem[1]
    result1 = "{" + key + "," + nome_sim_min + "}: "
    result = ""
    for i in range(len(name_2_set[key])):
        try:
            perc_1 = float(name_2_set[key][i])
            perc_2 = float(name_2_set[nome_sim_min][i]) 
        except:
            continue
        # perc_1 = round(perc_1, 2)
        # perc_2 = round(perc_2, 2)
        perc_1 = float("{:.2f}".format(perc_1))
        perc_2 = float("{:.2f}".format(perc_2))
        # result += lista_mesi[i] + "," + name_2_set[key][i] + "%, " + name_2_set[nome_sim_min][i] + "%; "
        result += lista_mesi[i] + "," + str(perc_1) + "%, " + str(perc_2) + "%; "
    print(result1 + result)