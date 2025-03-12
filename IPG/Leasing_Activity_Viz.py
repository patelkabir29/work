# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 11:42:00 2023

@author: kpatel
"""

from pyairtable import Api, Base, Table
from pyairtable.formulas import match
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

API_KEY = 'keyAxyrUn7wGiLY6v'
table = Table(API_KEY, 'app2QOo2ody2LwU9D', 'Linked: Lease Comps')
data = table.all(view='Q3 2023')
data = pd.DataFrame([x['fields'] for x in data])

ranges = [0, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, np.inf]
bins = pd.cut(data['Transaction SF'].values, bins=ranges, right=True)

# Count the number of values in each bin
counts = bins.value_counts()

#####TIM Count by Size##########
fig, axs = plt.subplots(1, 1, figsize=(17, 11))

# Plot the first subplot
x_values = ['<25k SF', '25k-50k SF', '50k-100k SF', '100k-250k SF']#, '250k-500k SF', '500k-1M SF', '1M+ SF']

# Group the data by the bins and sum the values in each
y_values = data.groupby(bins)['Transaction SF'].sum()[0:4]
cv = counts.values[0:4]

bar_plot = axs.bar(x_values, y_values, color='blue')
axs.bar(x_values, y_values)
axs.set_title('IPG: Q3 2023 Leasing Activity', fontsize=16)
axs.get_yaxis().set_major_formatter('{:,.0f} SF'.format)

# Add labels to the bars
for i, v in enumerate(y_values):
    bar_plot[i].set_label(cv[i])
    axs.text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                    v/2, str(cv[i]) , ha='center', va='center', fontsize=18, color='white', weight='bold')

fig.savefig('IPG Leasing Q3.png', dpi=300)