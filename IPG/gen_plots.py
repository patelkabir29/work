# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 12:09:52 2022

@author: kpatel
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from scipy.ndimage.filters import gaussian_filter1d

METRIC = 'mae'
INCLUDE_UW=True

# PDP by Floor of Occupancy
fl = list(range(2,31))
fl.remove(13)
SF = [7025,41132,41460,41323,41247,40686,41604,29448,37845,16938,15865,14625,16263,16263,16267,16275,16314,16294,16428,16428,16428,16428,16288,16291,16130,16140,16298,14690]

fig = plt.figure(figsize=(17, 11))
gs = fig.add_gridspec(1,1)
ax = []
ax.append(fig.add_subplot(gs[0]))

predictions = dict.fromkeys(model.keys(), [])
predictions_smooth = dict.fromkeys(model.keys(), [])

for key, om in model.items():
        
    COLS_TRAIN = list(om.data['train']['input'].columns)
    preds = []
    for floor_num, sf in zip(fl,SF):
        x=om.data['all'][COLS_TRAIN]
        x = x[x['Building:Property Id']==290].iloc[0]
        
        # set most recent market KPIs
        x['Market:MN(A) Vacancy'] = 0.1
        x['Market:MN(A) Net Absorption'] = -3488981
        x['Market:MN(A) Availability'] = 0.18
        x['Market:Vacancy'] = .109
        x['Market:Net Absorption'] = -372911
        x['Market:Availability'] = .164
        
        # set 452 building attributes
        x['Building:Years Since Reno'] = 0
        x['Building:Age'] = 0
        x['Lease:Transaction Sqft'] = sf
        x['Lease:Execution Year'] = 2022
        x['Lease:Floors:Max'] = floor_num
        x['Lease:Floors:Avg'] = floor_num
        x['Lease:Relative Position'] = floor_num/30
        x['Lease:Term'] = 11*12
        x['Lease:Multi-Floor'] = True
        x['Lease:Entire Floor'] = True
    
        x = pd.DataFrame(x)
        y = om.regr.predict(x.values.T)
        preds.append(y[0])
    
    predictions_smooth[key] = gaussian_filter1d(preds, sigma=0.75)
    ax[0].plot(fl, predictions_smooth[key], linewidth=2)
    ax[0].fill_between(fl, predictions_smooth[key] + om.score[METRIC], predictions_smooth[key] - om.score[METRIC], alpha=0.125)
    
if INCLUDE_UW:
    uw = [85,85,85,85,85,85,85,85,115,125, 105, 105, 120, 120, 120, 120, 120, 120, 125, 125, 125, 125, 125, 130, 130, 130, 130, 130]
    ax[0].scatter(fl, uw, marker='o', color='gray')
    
ax[0].grid()
ax[0].set_title('452 5th Ave: Starting Rents', fontsize=14)
ax[0].set_xlabel('Floor #')

ax[0].set_ylabel('{} ($ PSF)'.format(om.target.lstrip('Lease:')))
ax[0].xaxis.set_major_locator(MaxNLocator(integer=True))
ax[0].yaxis.set_major_formatter('${x:1.0f}')

