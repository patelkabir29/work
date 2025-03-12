# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 18:20:20 2022

@author: kpatel
"""

from MachineLearning import IndustrialModel
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from scipy.ndimage.filters import gaussian_filter1d

cf = 'starting_rent_industrial.yaml'

m = IndustrialModel(config_file='C:\\Users\\kpatel\\analytics-research\\IPG\\config\\{}'.format(cf))
m._load_data()
m._feature_engineering()
m._create_train_test(test_pct=0.1)
m.run()
m.score
model[cf.rstrip('.yaml')] = m

INCLUDE_UW=True

fig = plt.figure(figsize=(30, 20))
gs = fig.add_gridspec(3,4)
ax = []
ax.append(fig.add_subplot(gs[0,:]))
ax.append(fig.add_subplot(gs[1,0]))
ax.append(fig.add_subplot(gs[1,1]))
ax.append(fig.add_subplot(gs[1,2]))
ax.append(fig.add_subplot(gs[1,3]))
ax.append(fig.add_subplot(gs[2,0]))
ax.append(fig.add_subplot(gs[2,1]))
ax.append(fig.add_subplot(gs[2,2:4]))

ax[0].pie(om.feature_impact['level1']['Impact'], labels=om.feature_impact['level1']['Category'], autopct=lambda p: '{:.1f}%'.format(p))
ax[0].set_title('Model Discovered Correlations to Starting Rent', fontsize=18)
ax[0].yaxis.set_major_formatter('{x:1.0f}%')
ax[0].text(0.925, 0.075, u"R\u00b2={}".format(round(om.score['r2'],2), style='italic'), transform=ax[0].transAxes, fontsize=16, verticalalignment='top')

COLORS = ['tab:blue', 'tab:orange','tab:green', 'tab:red']

for indx, category in enumerate(['Locational', 'Lease', 'Building', 'Market']):
    temp = om.feature_impact['level2'][om.feature_impact['level2']['Category']==category]
    if category=="Market":
        temp = temp.replace('Net Absorption', 'Submarket\nNet\nAbsorption')
        temp = temp.replace('Vacancy', 'Submarket\nVacancy')
        temp = temp.replace('Availability', 'Submarket\nAvailability')
        temp = temp.replace('MN(A) Net Absorption', 'Manhattan\nNet\nAbsorption')
        temp = temp.replace('MN(A) Vacancy', 'Manhattan\nVacancy')
        temp = temp.replace('MN(A) Availability', 'Manhattan\nAvailability')
    elif category in ['Lease', 'Building', 'Locational']:
        temp = temp.assign(Category2=temp.Category2.apply(lambda x: '\n'.join(x.split(' '))))
        
    temp = temp.sort_values(by='Impact')
    
    # normalize explained variance per category
    temp['Impact'] = temp['Impact']/temp['Impact'].sum()
    
    ax[indx+1].set_title('{} Attributes'.format(category), fontsize=16)
    ax[indx+1].barh(temp['Category2'], temp['Impact'], color=COLORS[indx])

COEFF=1.0
METRIC = 'mae'
COLS_TRAIN = list(om.data['train']['input'].columns)

train_target = om.data['train']['target']
test_target = om.data['test']['target']

ax[5].set_title('Distribution of Training & Test Data', fontsize=14)
ax[5].hist([train_target, test_target], bins=35, label=['Training Set (90%)', 'Test Set (10%)'], density=True, alpha=0.75)
ax[5].legend(loc='upper right')
ax[5].grid()

ax[6].set_title('Distribution of Mean Abs Percentage Error (on Test Data)', fontsize=14)
ax[6].hist(100*(om.data['test']['target'] - om.data['test']['predictions'])/om.data['test']['target'], bins=35, label='Mean Absolute Pct Error', density=True, alpha=0.75, range=(-30,30))
ax[6].legend(loc='upper right')
ax[6].grid()

# PDP by Floor of Occupancy
preds = []
fl = list(range(2,31))
fl.remove(13)
SF = [7025,41132,41460,41323,41247,40686,41604,29448,37845,16938,15865,14625,16263,16263,16267,16275,16314,16294,16428,16428,16428,16428,16288,16291,16130,16140,16298,14690]

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
    preds.append(COEFF*y[0])

preds_smooth = gaussian_filter1d(preds, sigma=0.75)
ax[7].plot(fl, preds_smooth, linewidth=2)
ax[7].fill_between(fl, preds_smooth + om.score[METRIC], preds_smooth - om.score[METRIC], alpha=0.125)

if INCLUDE_UW:
    uw = [85,85,85,85,85,85,85,85,115,125, 105, 105, 120, 120, 120, 120, 120, 120, 125, 125, 125, 125, 125, 130, 130, 130, 130, 130]
    ax[7].scatter(fl, uw, marker='o', color='orange')

ax[7].grid()
ax[7].set_title('452 5th Ave: Starting Rents', fontsize=14)
ax[7].set_xlabel('Floor #')

ax[7].set_ylabel('{} ($ PSF)'.format(om.target.lstrip('Lease:')))
ax[7].xaxis.set_major_locator(MaxNLocator(integer=True))
ax[7].yaxis.set_major_formatter('${x:1.0f}')

fig.savefig('output.png')

