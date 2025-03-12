# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 17:31:59 2022

@author: kpatel
"""

import os
import pandas as pd
from pyairtable import Api, Base, Table
from pyairtable.formulas import match

API_KEY = 'keyAxyrUn7wGiLY6v'

table = Table(API_KEY, 'appehDuuurzjz8E4R', 'Metrics: Southeast Markets')

data = table.all(view='Raleigh', fields=['Date', 'Demand Units', 'Inventory Units'])
data = pd.DataFrame([x['fields'] for x in data])
data = data.assign(Date=data.Date.apply(lambda x: pd.to_datetime(x).date()))

table = Table(API_KEY, 'appehDuuurzjz8E4R', 'Raleigh MF Properties')

data = table.all(view='Table')

data = pd.DataFrame([x['fields'] for x in data])
data = data.assign(Date=data.Date.apply(lambda x: pd.to_datetime(x).date()))


class AirTable(object):    
    
    def __init__(self):        
    
        self.API_KEY = 'keyAxyrUn7wGiLY6v'