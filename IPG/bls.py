# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 11:18:18 2021

@author: kpatel
"""

import requests
import json
import pandas as pd
from datetime import date
import matplotlib
import matplotlib.pyplot as plt

headers = {'Content-type': 'application/json'}
seriesid = ['CUUR0000SA0','SUUR0000SA0', ' LNS14000000']
data = json.dumps({"seriesid": seriesid ,"startyear":"2011", "endyear":"2020"})
response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
data = response.json()

d = dict.fromkeys(seriesid)
for s_id, temp in zip(seriesid,data.get('Results').get('series')):
    x = pd.DataFrame(temp.get('data'))
    x.year = x.year.astype(int)
    x.value = x.value.astype(float)
    x.period = x.period.apply(lambda x: x.strip('M')).astype(int)
    x = x.assign(date=x.apply(lambda y: date(y['year'], y['period'], 1), axis=1))
    d[s_id] = x
    
datenum = matplotlib.dates.date2num(d['CUUR0000SA0']['date'])
values = d['CUUR0000SA0']['value']
plt.plot(datenum, values)

datenum = matplotlib.dates.date2num(d['CUUR0000SA0']['date'])
values = d['SUUR0000SA0']['value']
plt.plot(datenum, values)