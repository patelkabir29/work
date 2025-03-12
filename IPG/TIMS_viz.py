# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 21:12:57 2023

@author: kpatel
"""

import matplotlib.pyplot as plt
import numpy as np

# Create a 2x2 grid of subplots with figure size 17x11 inches
fig, axs = plt.subplots(2, 2, figsize=(17, 11))

# Plot the first subplot
x_values = ['2019', '2020', '2021', '2022', '2/28/2023', '3/31/2023']

y_values= [12846000,
           10085000,
           7495000,
           12004500,
           13155000,
           10442500]

count_values = [82, 68, 79, 116, 128, 100]

bar_plot = axs[0, 0].bar(x_values, y_values, color='blue')
axs[0, 0].bar(x_values, y_values)
axs[0, 0].set_title('JLL Tenant Requirements (SF)', fontsize=20)
#axs[0, 0].set_xlabel('Years', fontsize=18)
#axs[0, 0].set_ylabel('Total SF', fontsize=18)
axs[0, 0].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[0, 0].set_ylim([0, 13.5e6])

# Add labels to the bars
for i, v in enumerate(zip(y_values,count_values)):
    bar_plot[i].set_label(v[1])
    axs[0, 0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v[0]/2, str(v[1]), ha='center', va='center', fontsize=18, color='white', weight='bold')

y_values=[156659,148309,94873,60000,70000,72500]

bar_plot = axs[1, 0].bar(x_values, y_values, color='blue')
axs[1, 0].bar(x_values, y_values)
axs[1, 0].set_title('JLL Median Requirement (SF)', fontsize=20)
#axs[1, 0].set_xlabel('Years', fontsize=18)
#axs[1, 0].set_ylabel('Median Req (SF)', fontsize=18)
axs[1, 0].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[1,1].set_ylim([0,160e3])

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[1, 0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, f'{v:,} SF' , ha='center', va='center', fontsize=10, color='white')

# IPG TIMs
y_values= [0,0,0,2795000,4500000,4702500]

count_values = [np.nan, np.nan, np.nan,21,52,53]

bar_plot = axs[0, 1].bar(x_values, y_values, color='blue')
axs[0, 1].bar(x_values, y_values)
axs[0, 1].set_title('IPG Tenant Requirements (SF)', fontsize=20)
axs[0, 1].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[0, 1].set_ylim([0, 13.5e6])

# Add labels to the bars
for i, v in enumerate(zip(y_values,count_values)):
    bar_plot[i].set_label(v[1])
    axs[0, 1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v[0]/2, str(v[1]), ha='center', va='center', fontsize=18, color='white', weight='bold')

y_values=[0,0,0,50000,75000,100000]

bar_plot = axs[1, 1].bar(x_values, y_values, color='blue')
axs[1, 1].bar(x_values, y_values)
axs[1, 1].set_title('IPG Median Requirement (SF)', fontsize=20)
axs[1, 1].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[1,0].set_ylim([0,160e3])

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[1, 1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, f'{v:,} SF' , ha='center', va='center', fontsize=10, color='white')

# Display the chart
#plt.show()
#fig.suptitle('Industrial Demand: Leading Indictators', fontsize=22, weight='bold')
fig.savefig('Demand_Leading.png',dpi=300)

#####TIM Count by Size##########
fig, axs = plt.subplots(2, 1, figsize=(17, 11))

# Plot the first subplot
x_values = ['<25k SF', '25k-50k SF', '50k-100k SF', '100k-250k SF', '250k-500k SF', '500k-1M SF']
y_values = [10, 33, 43, 43, 8, 2]

bar_plot = axs[0].bar(x_values, y_values, color='blue')
axs[0].bar(x_values, y_values)
axs[0].set_title('JLL: TIM Count by Size Req', fontsize=16)
axs[0].set_ylim([0,45])

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, str(v) , ha='center', va='center', fontsize=13, color='white', weight='bold')


y_values = [19, 7, 8, 15, 3, 1]

bar_plot = axs[1].bar(x_values, y_values, color='blue')
axs[1].bar(x_values, y_values)
axs[1].set_title('IPG: TIM Count by Size Req', fontsize=16)

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, str(v) , ha='center', va='center', fontsize=13, color='white', weight='bold')

axs[1].set_ylim([0,45])
fig.suptitle('Industrial Demand: TIMs by Size Requirement', fontsize=22, weight='bold')
fig.savefig('Demand_Leading_Counts.png', dpi=300)

fig, axs = plt.subplots(1, 1, figsize=(17, 11))

# Plot the first subplot
years = ['2020', '2021', '2022']
quarters = ['Q1', 'Q2', 'Q3', 'Q4']

x_values = []
for y in years:
    for q in quarters:
        x_values.append(y+'-'+q)
x_values.append('2023-Q1')

y_values = [11e6, 10.25e6, 8.75e6, 9e6, 11.75e6, 11.8e6, 13.85e6, 14.2e6, 15.7e6, 11437000, 13167500, 12004500, 10442500]
cv = [85, 76, 66, 65, 79, 80, 95, 115, 131, 122, 128, 116, 100]

bar_plot = axs.bar(x_values, y_values, color='blue')
axs.bar(x_values, y_values)

# Add labels to the bars
for i, v in enumerate(cv):
    #bar_plot[i].set_label(v)
    axs.text(x=bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    y=y_values[i]/2, s=str(v) , ha='center', va='center', fontsize=16, color='white', weight='bold')

axs.set_title('JLL: Historical Demand', fontsize=16)
axs.get_yaxis().set_major_formatter('{:,.0f} SF'.format)
fig.savefig('JLL_Historical_Demand.png', dpi=300)