# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 16:51:03 2022

@author: kpatel
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

data = pd.read_csv('shap.csv')
data = data.drop(columns=['__joinsource_column_name__'])
data = data.rename(columns={col: col.replace('_label0','') for col in data.columns})
data = data[[col for col in data.columns if 'Lease:Industry' not in col]]

categories = ['Building', 'Lease', 'Market']

R = 17/11
M = 15
fig = plt.figure(figsize=(M*R,M))
gs = fig.add_gridspec(1,3)
ax = []
ax.append(fig.add_subplot(gs[0]))
ax.append(fig.add_subplot(gs[1]))
ax.append(fig.add_subplot(gs[2]))

d = {}
for category in categories:
    temp = data[[col for col in data.columns if category in col]]
    temp = temp.rename(columns={col: col.replace('{}:'.format(category),'') for col in temp.columns})
    temp = temp.rename(columns={col: '\n'.join(col.split(' ')) for col in temp.columns})
    d[category] = temp
    
for indx, category in enumerate(categories):
    p = d[category].iloc[0]
    p = p.sort_values()
    ax[indx].barh(p.index, p.values)
