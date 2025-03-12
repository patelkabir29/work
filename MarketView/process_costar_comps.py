# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 16:08:34 2022

@author: kpatel
"""

import pandas as pd
import numpy as np

data = pd.DataFrame()

for _tn in range(1,3):
    temp = pd.read_csv('table-{}.csv'.format(_tn))
    #data = data.rename(columns={col: col.strip("'").strip() for col in data.columns})
    data = data.append(temp)

for col in data.columns:
    try:
        data = data.assign(**{col: data[col].apply(lambda x: x.strip("'").strip())})
    except:
        pass

data = data.assign(**{'SF Leased': data['SF Leased'].apply(lambda x: x.replace('.',',').replace(',',''))})
data = data.assign(**{'SF Leased': data['SF Leased'].apply(lambda x: int(x))})

data = data.assign(**{'Mos on Mrkt': data['Mos on Mrkt'].apply(lambda x: int(x.strip('Mos').strip('Mo').strip()) if 'Mos' in x else np.nan)})

str_cols=['Sign Date',
 'Start Date',
 'Address',
 'City',
 'Rent Type',
 'Use',
 'Tenant',
 'Lease Status',
 'Deal Type',
 'Move-In Date',
 'Submarket',
 'Rent PSF']

for col in str_cols:
    data = data.assign(**{col: data[col].apply(lambda x: replace(x))})

def replace(x):
    try:
        return x.replace('.',',').replace(',','').replace("'",'').strip("'").strip()
    except:
        return x