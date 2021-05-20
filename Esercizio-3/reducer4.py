#!/usr/bin/env python3
"""reducer.py"""

import sys

name_2_set ={}
similarity_dict_2_set = {}
SOGLIA = 1
SOGLIA_MESI_COMUNE = 6
LISTA_MESI = ["GEN", "FEB", "MAR", "APR", "MAG", "GIU", "LUG", "AGO", "SETT", "OTT", "NOV", "DIC"]

def update (name1, name2, similarity): 
    if name1 not in similarity_dict_2_set: 
        similarity_dict_2_set[name1] = [(name2,similarity)]
    else: 
        similarity_dict_2_set[name1].append((name2,similarity))
    if name2 not in similarity_dict_2_set: 
        similarity_dict_2_set[name2] = [(name1,similarity)]
    else: 
        similarity_dict_2_set[name2].append((name1,similarity))

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    name = line[0]
    name_2_set[name] = line[1:]

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
            if diff <= SOGLIA: 
                delta += diff
                mesi_comuni +=1
            else:
                flag = True
                break
        if flag==False and mesi_comuni >= SOGLIA_MESI_COMUNE:
            update(i, j, delta)

for name in similarity_dict_2_set:
    # nome_sim_min = ""
    # sim_min = 12 * SOGLIA
    # similarity_dict_2_set[name]
    for elem in similarity_dict_2_set[name]:
        # if elem[1] < sim_min: 
        #     nome_sim_min = elem[0]
        #     sim_min = elem[1]
        result1 = "{" + name + "," + elem[0] + "}: "
        result = ""
        for i in range(len(name_2_set[name])):
            try:
                perc_1 = float(name_2_set[name][i])
                perc_2 = float(name_2_set[elem[0]][i]) 
            except:
                continue
            perc_1 = float("{:.2f}".format(perc_1))
            perc_2 = float("{:.2f}".format(perc_2))
            result += LISTA_MESI[i] + "," + str(perc_1) + "%, " + str(perc_2) + "%; "
        print(result1 + result)