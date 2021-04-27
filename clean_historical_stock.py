#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

dataStocks = pd.read_csv("dataset/historical_stocks.csv")

app = dataStocks.groupby('name').count()[['ticker', 'sector']]
app2 = app.loc[app.ticker != app.sector]
app3 = app2.loc[app2.sector != 0]
lista_aziende_con_NaN = list(app3.index)
set_aziende_con_NaN = set(lista_aziende_con_NaN)

diz = {}
for index, row in dataStocks.iterrows():
    if row['name'] in set_aziende_con_NaN:
        if not row.isnull().values.any():
            diz[row['name']] = [row['sector'], row['industry']]

for key in diz.keys():
    dataStocks.loc[dataStocks.name == key, 'sector'] = diz[key][0]
    dataStocks.loc[dataStocks.name == key, 'industry'] = diz[key][1]

dataStocks.dropna(axis=0, inplace=True)

dataStocks.to_csv('./dataset/historical_stocks_clean.csv', index = False)