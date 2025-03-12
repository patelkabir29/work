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
x_values = ['12/31/2022', '1/31/2022', '2/28/2023', '3/31/2023']

y_values= [2238000, 3530500, 3570500, 2449000]

count_values = [27, 41, 40, 31]

bar_plot = axs[0, 0].bar(x_values, y_values, color='blue')
axs[0, 0].bar(x_values, y_values)
axs[0, 0].set_title('JLL Parking & IOS Requirements (SF)', fontsize=20)
axs[0, 0].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[0, 0].set_ylim([0, 4e6])

# Add labels to the bars
for i, v in enumerate(zip(y_values,count_values)):
    bar_plot[i].set_label(v[1])
    axs[0, 0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v[0]/2, str(v[1]), ha='center', va='center', fontsize=18, color='white', weight='bold')

y_values=[35000, 45000, 48750, 60000]

bar_plot = axs[1, 0].bar(x_values, y_values, color='blue')
axs[1, 0].bar(x_values, y_values)
axs[1, 0].set_title('JLL Median Parking & IOS Requirement (SF)', fontsize=20)
#axs[1, 0].set_xlabel('Years', fontsize=18)
#axs[1, 0].set_ylabel('Median Req (SF)', fontsize=18)
axs[1, 0].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
#axs[1,1].set_ylim([0,160e3])

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[1, 0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, f'{v:,} SF' , ha='center', va='center', fontsize=14, color='white')

# IPG TIMs
x_values = ['12/31/2022', '1/31/2022', '2/28/2023', '3/31/2023']

y_values = [1030000, 1920000, 1995999, 2330000]
count_values = [8, 19, 23, 27]

bar_plot = axs[0, 1].bar(x_values, y_values, color='blue')
axs[0, 1].bar(x_values, y_values)
axs[0, 1].set_title('IPG Parking & IOS Requirements (SF)', fontsize=20)
axs[0, 1].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[0, 1].set_ylim([0, 4e6])

# Add labels to the bars
for i, v in enumerate(zip(y_values,count_values)):
    bar_plot[i].set_label(v[1])
    axs[0, 1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0,\
                    v[0]/2, str(v[1]), ha='center', va='center', fontsize=18, color='white', weight='bold')

y_values=[75000, 60000, 40000, 40000]

bar_plot = axs[1, 1].bar(x_values, y_values, color='blue')
axs[1, 1].bar(x_values, y_values)
axs[1, 1].set_title('IPG Median Requirement (SF)', fontsize=20)
axs[1, 1].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
axs[1,0].set_ylim([0,60e3])

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[1, 1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, f'{v:,} SF' , ha='center', va='center', fontsize=14, color='white')

# Display the chart
#plt.show()
#fig.suptitle('Industrial Demand: Leading Indictators', fontsize=22, weight='bold')
fig.savefig('Demand_Leading.png',dpi=300)

#####TIM Count by Size##########
fig, axs = plt.subplots(2, 1, figsize=(17, 11))

# Plot the first subplot
x_values = ['<25k SF', '25k-50k SF', '50k-100k SF', '100k-250k SF', '250k-500k SF']

y_values = [6, 8, 6, 10, 1]

bar_plot = axs[0].bar(x_values, y_values, color='blue')
axs[0].bar(x_values, y_values)
axs[0].set_title('JLL: Parking & IOS TIMs Count by Size Req', fontsize=16)

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, str(v) , ha='center', va='center', fontsize=13, color='white', weight='bold')

y_values = [6, 8, 6, 6, 1]

bar_plot = axs[1].bar(x_values, y_values, color='blue')
axs[1].bar(x_values, y_values)
axs[1].set_title('IPG: Parking & IOS TIMs Count by Size Req', fontsize=16)

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(v)
    axs[1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, str(v) , ha='center', va='center', fontsize=13, color='white', weight='bold')

fig.savefig('Parking & IOS TIMs Counts.png', dpi=300)



