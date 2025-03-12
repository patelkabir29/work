# -*- coding: utf-8 -*-
"""
Created on Mon May  8 14:40:40 2023

@author: keni.patel
"""

from Comps import Comps
from TIMS import JLL_TIMS, IPG_TIMS
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from datetime import date

def by_bin(data, column='Transaction SF'):
    data = data.assign(size_bin=pd.cut(data[column].values, bins=ranges, right=True))
    data = data.assign(size_bin=data.size_bin.apply(lambda x: '{:.0f}k-{:.0f}k SF'.format(x.left/1e3, x.right/1e3)), axis=1)
    data.size_bin.replace('500k-1000k SF','500k-1M SF', inplace=True)
    data.size_bin.replace('1000k-infk SF','1M SF+', inplace=True)

    tmp = data.pivot_table(index='size_bin', columns='quarter', values=column, aggfunc=np.sum)[['Q4 2022','Q1 2023','Q2 2023', 'Q3 2023']]
    return tmp

# Function to add text labels at the bottom of each bar
def add_bar_labels(ax):
    qtrs = ['Q4', 'Q1', 'Q2']
    L = len(ax.patches)
    for indx, p in enumerate(ax.patches):
        height = p.get_height()
        if height>0:
            ax.annotate(qtrs[int(indx/(L/3))], xy=(p.get_x() + p.get_width() / 2, 0),
                        xytext=(0, 5), textcoords='offset points', ha='center', va='bottom', fontsize=8)
            
            ax.annotate('{:,.0f} SF'.format(height), xy=(p.get_x() + p.get_width() / 2, height+5e3),
                        xytext=(0,5), textcoords='offset points', ha='center', va='top', fontsize=4)

tims_ipg = IPG_TIMS()
tims_jll = JLL_TIMS()

c0 = Comps('Q4 2022')
c0.data = c0.data.assign(quarter='Q4 2022')

c1 = Comps('Q1 2023')
c1.data = c1.data.assign(quarter='Q1 2023')

c2 = Comps('Q2 2023')
c2.data = c2.data.assign(quarter='Q2 2023')

c3 = Comps('Q3 2023')
c3.data = c3.data.assign(quarter='Q3 2023')

c4 = Comps('Q4 2023')
c4.data = c4.data.assign(quarter='Q4 2023')

"""
data = c0.data.append(c1.data)
data = data.append(c2.data)
data = data.append(c3.data)
data = data.append(c4.data)
"""

data = pd.concat([c0.data, c1.data, c2.data, c3.data, c4.data])

data['Class (Properties)'] = data['Class (Properties)'].apply(lambda x: x.pop() if isinstance(x,list) else x)

def combine_columns_with_precedence(df):
    """
    This function combines two columns into a single column.
    It gives precedence to col1 and takes the value from col2 only if col1 is empty.
    """
    # Use col1 value if it's not empty; otherwise, use col2 value
    combined = df.apply(lambda x: x['Class (Properties)'] if pd.notnull(x['Class (Properties)']) and x['Class (Properties)'] != '' else x['Class'], axis=1)
    return combined

df = combine_columns_with_precedence(data)
data = data.assign(Class=df)

shades_of_blue = ['#ADE1F5', '#6FB9E8', '#1F77B4', '#004C8C', '#ADE1F5']
#shades_of_green = ['#9ACD32', '#228B22', '#006400', '#9ACD32']

data = data.query('Class!="Land"')
data = data.query('Borough!="Staten Island"')

ranges = [0, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, np.inf]

# Create a single figure with 2 subplots
fig, axs = plt.subplots(1, 2, figsize=(17, 11), sharey=True)
gs = gridspec.GridSpec(2, 2)

# Plotting the first bar graph on the first subplot
axs[0] = plt.subplot(gs[0, :])
tmp = data.pivot_table(index='Class (Properties)', columns='quarter', values='Transaction SF', aggfunc=np.sum)[['Q4 2022','Q1 2023','Q2 2023', 'Q3 2023', 'Q4 2023']]
tmp.plot.bar(rot=0, fontsize=16, color=shades_of_blue, ax=axs[0])
axs[0].yaxis.set_major_formatter('{:,.0f} SF'.format)

# Labeling the top of each bar with its value
for patch in axs[0].patches:
    height = patch.get_height()
    axs[0].text(patch.get_x() + patch.get_width() / 2, height, f'{int(height):,}', ha='center', va='bottom', fontsize=8)

# Plotting the second bar graph on the second subplot
axs[1] = plt.subplot(gs[1, :])
data.pivot_table(index='Borough', columns='quarter', values='Transaction SF', aggfunc=np.sum)[['Q4 2022','Q1 2023','Q2 2023', 'Q3 2023', 'Q4 2023']].plot.bar(rot=0, fontsize=16, color=shades_of_blue, ax=axs[1])
axs[1].yaxis.set_major_formatter('{:,.0f} SF'.format)

# Labeling the top of each bar with its value
for patch in axs[1].patches:
    height = patch.get_height()
    axs[1].text(patch.get_x() + patch.get_width() / 2, height, f'{int(height):,}', ha='center', va='bottom', fontsize=8)

# Adjust the layout and spacing between subplots
plt.tight_layout()

for ax in axs:
    ax.legend(fontsize=14)

# Save the figure to a PNG file
fig.savefig('leasing.png', dpi=300, bbox_inches='tight')

def leasing_plot(text=True):
    
# By Size Bin
fig =  plt.figure(figsize=(17, 11))
gs = gridspec.GridSpec(2, 2, width_ratios=[1,1])
axs = [plt.subplot(gs[0, :]), plt.subplot(gs[1,0]), plt.subplot(gs[1,1])]

x = by_bin(data,'Transaction SF')
x = pd.DataFrame([x[col][x[col]>0] for col in x.columns]).T
x.plot.bar(rot=45, fontsize=16, color=shades_of_blue, ax=axs[0])
add_bar_labels(axs[0])

x = by_bin(data, 'Building SF')
x = pd.DataFrame([x[col][x[col]>0] for col in x.columns]).T
ax2 = plt.subplot(gs[1, 0])
x.plot.bar(rot=45, fontsize=16, color=shades_of_blue, ax=axs[1])
add_bar_labels(axs[1])

x = by_bin(data, 'Parking SF')
x = pd.DataFrame([x[col][x[col]>0] for col in x.columns]).T
x.plot.bar(rot=45, fontsize=16, color=shades_of_blue, ax=axs[2])
add_bar_labels(axs[2])

[_ax.yaxis.set_major_formatter('{:,.0f} SF'.format) for _ax in axs]
[_ax.set_xlabel('') for _ax in axs]
[_ax.legend(fontsize=14) for _ax in axs]

plt.tight_layout(pad=1.0)

fig.savefig('test.png', dpi=300)

def comparative_bar(c1, c2):
    
    fig, _ = plt.subplots(figsize=(17, 11), sharey=True)
    gs = gridspec.GridSpec(2, 2)
    axs = [None, None]
    axs[0] = plt.subplot(gs[0,:])
    axs[1] = plt.subplot(gs[1,:])
    
    bar_width = 0.35  # Width of each bar
    
    data = c1.data.query('Class!="Land"')
    c1_y = data.groupby('Class')['Transaction SF'].sum()
    missing_class=list(set(['A','B','C'])-set(c1_y.index))[0]
    
    if len(missing_class)>0:
        c1_y[missing_class] = 0
    
    c1_y.sort_index(ascending=True)
    
    index = np.arange(len(c1_y.index))
    c1_counts = data['Class'].value_counts()
    c1_counts[missing_class] = 0
    c1_counts = c1_counts[c1_y.index]
    c1_y=c1_y.sort_index()
    
    bar1 = axs[0].bar(index, c1_y.values, bar_width, label='Q1 2023')

    # Add labels to the bars
    for i, v in enumerate(c1_y.values):
        bar1[i].set_label(v)
        axs[0].text(bar1[i].get_x() + bar1[i].get_width() / 2.0, 
                        v+30000, '{:0,} SF'.format(v), ha='center', va='center', fontsize=18, color='gray')
    
    data = c2.data.query('Class!="Land"')
    
    c2_y = data.groupby('Class')['Transaction SF'].sum()
    missing_class=list(set(['A','B','C'])-set(c2_y.index))
    
    if len(missing_class)>0:
        c2_y[missing_class] = 0
    c2_y.sort_index(ascending=True)
    
    c2_counts = data['Class'].value_counts()
    c2_counts[missing_class] = 0
    c2_counts = c2_counts[c2_y.index]
    c2_y = c2_y.sort_index()
    
    pct_change = (c2_y-c1_y)/c1_y
    
    bar2 = axs[0].bar(index + bar_width, c2_y.values, bar_width, label='Q2 2023')
    
    # Add labels to the bars
    for i, v in enumerate(c2_y.values):
        bar2[i].set_label(v)
        axs[0].text(bar2[i].get_x() + bar2[i].get_width() / 2.0, 
                        v+30000, '{:,} SF'.format(v), ha='center', va='center', fontsize=18, color='gray')

        if ~(np.isinf(pct_change[i])|np.isneginf(pct_change[i])):
            axs[0].text(bar2[i].get_x() + bar2[i].get_width() / 2.0, 
                            v/2, '{:.0%}'.format(pct_change[i]), ha='center', va='center', fontsize=18, color='gray')
        
    axs[0].set_xticks(index + bar_width / 2)
    axs[0].set_xticklabels(['A', 'B', 'C'])
    
    [x.yaxis.set_major_formatter('{:,.0f} SF'.format) for x in axs]

"""
from CoStar import Industrial
i = Industrial("Industrial_Data.xlsx")
submarkets = list(i.data.keys())
submarkets.remove('Overall')
submarkets.remove('Class A')
DATE = date(2013,1,1)

i._plot_metric('Leasing Activity SF Total', smoothing='gaussian', param=2, date_min=DATE)
i._plot_metric('Net Absorption SF Total', smoothing='gaussian', param=2, date_min=DATE)
i._plot_metric('NNN Rent Direct', smoothing='gaussian', param=2, date_min=DATE)
i._plot_metric('Under Construction SF', smoothing='gaussian', param=2, scatter=False, date_min=DATE, figsize=(11,8.5))
i._plot_metric('Under Construction Bldgs', smoothing='gaussian', param=2, scatter=True, date_min=DATE, figsize=(17,5.5))
i._plot_metric('Deliveries SF', smoothing='gaussian', param=2, date_min=DATE)
i._plot_metric('Inventory SF', smoothing='gaussian', param=2, date_min=DATE)
i._plot_metric('Vacant Percent % Direct', smoothing='gaussian', param=2, date_min=date(2000,1,1))

i._plot_metric('Market Rent/SF', segments=submarkets, smoothing='gaussian', param=2, date_min=date(2000,1,1), scatter=False, figsize=(11,11))

i._plot_metric('Inventory SF', segments=list(i.data.keys()), smoothing='gaussian', param=2, date_min=date(2000,1,1), scatter=False)
i._plot_metric('Under Constr % of Inventory', segments=submarkets, smoothing='rolling', param=4, date_min=date(2000,1,1), scatter=False)

i._plot_metric('All Service Type Rent Direct', segments=['Class A'], smoothing='gaussian', param=2, date_min=date(2010,1,1), scatter=True, figsize=(17,11))

i._plot_metric('Leasing Activity SF Total',  smoothing='gaussian', param=1, date_min=DATE, scatter=True, figsize=(17,11))
i._plot_metric('Net Absorption SF Total', smoothing='gaussian', param=1, date_min=DATE, scatter=True, figsize=(17,5.5))
i._plot_metric('Total Available SF Direct', smoothing='gaussian', param=1, date_min=DATE, scatter=True, figsize=(17,11))
i._plot_metric('Vacant Percent % Direct', smoothing='gaussian', param=1, date_min=DATE, scatter=True, figsize=(17,11))

i._plot_metric('Vacancy Rate', segments=submarkets, smoothing='gaussian', param=1, date_min=date(2000,1,1), figsize=(17,11))
"""