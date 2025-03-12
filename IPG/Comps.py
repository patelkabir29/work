# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 14:53:04 2023

@author: kpatel

"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyairtable import Api, Base, Table
from pyairtable.formulas import match
import matplotlib.gridspec as gridspec

def count_missing_values(series):
    return series.isna().sum()

class Comps(object):
    
    def __init__(self, view_name):

        API_KEY = 'patK2kzMIkyf4b6to.a3b0ba7bd169cae47034d5a8a80246ac1f5948407b3e5a0d866d0cd5b790b6ee' #'keyAxyrUn7wGiLY6v'
        table = Table(API_KEY, 'app2QOo2ody2LwU9D', 'Linked: Lease Comps')
        data = table.all(view=view_name)
        data = pd.DataFrame([x['fields'] for x in data])
                
        self.data = data
        DATE_COLS = [col for col in self.data.columns if 'Date' in col]
        
        for col in DATE_COLS:
            self.data = self.data.assign(**{col: pd.to_datetime(self.data[col])})
            self.data = self.data.assign(**{col: self.data[col].apply(lambda x: x.date())})
            
    def plot_by_size(self):
        
        ranges = [0, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, np.inf]
        
        fig, _ = plt.subplots(figsize=(17, 11))
        gs = gridspec.GridSpec(2, 2)
        axs = [None, None, None]
        axs[0] = plt.subplot(gs[0,:])
        axs[1] = plt.subplot(gs[1,0])
        
        axs[2] = plt.subplot(gs[1,1])
        for indx, size_category in enumerate(['Transaction SF', 'Building SF', 'Parking SF']):
            bins = pd.cut(self.data[size_category].values, bins=ranges, right=True)        
            counts = bins.value_counts()

            # Plot the first subplot
            x_values = ['<25k SF', '25k-50k SF', '50k-100k SF', '100k-250k SF', '250k-500k', '500k-1M', '1M+']

            # Group the data by the bins and sum the values in each bin
            y_values = self.data.groupby(bins)[size_category].sum()
            cv = counts.values

            bar_plot = axs[indx].bar(x_values, y_values, color='blue')
            axs[indx].bar(x_values, y_values)
            axs[indx].set_title(size_category[:-3], fontsize=16)
            axs[indx].get_yaxis().set_major_formatter('{:,.0f} SF'.format)

            # Add labels to the bars
            for i, v in enumerate(y_values):
                bar_plot[i].set_label(cv[i])
                axs[indx].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0,
                                v/2, str(cv[i]) , ha='center', va='center', fontsize=18, color='white', weight='bold')
            if indx>=1:
                axs[indx].tick_params(axis='x', rotation=45)
                
        plt.subplots_adjust(wspace=0.25, hspace=0.25)
        
        fig.savefig('by_size.png', dpi=300)
        
    def plot_by_borough(self):
        
        fig, _ = plt.subplots(figsize=(17, 11), sharey=True)
        gs = gridspec.GridSpec(2, 2)
        axs = [None, None]
        axs[0] = plt.subplot(gs[0,:])
        axs[1] = plt.subplot(gs[1,:])
        #axs[2] = plt.subplot(gs[1,1])

        y_values = self.data.groupby('Class')['Transaction SF'].sum()
        cv = self.data['Class'].value_counts()
        cv = cv[y_values.index]
        bar_plot = axs[0].bar(y_values.index, y_values)
        
        # Add labels to the bars
        for i, v in enumerate(y_values):
            bar_plot[i].set_label(cv[i])
            axs[0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                            v/2, str(cv[i]) , ha='center', va='center', fontsize=18, color='white', weight='bold')
            
        y_values = self.data.groupby('Borough')['Transaction SF'].sum().sort_values(ascending=False)
        cv = self.data['Borough'].value_counts()
        bar_plot = axs[1].bar(y_values.index, y_values, color='blue')
        
        axs[1].bar(y_values.index, y_values)
                
        # Add labels to the bars
        for i, v in enumerate(y_values):
            bar_plot[i].set_label(cv[i])
            axs[1].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                            v/2, str(cv[i]) , ha='center', va='center', fontsize=18, color='white', weight='bold')

        [x.yaxis.set_major_formatter('{:,.0f} SF'.format) for x in axs]
        
        fig.savefig('by_borough.png', dpi=300)
        
    def plot_by_known_rent(self, num_bins=10):
                        
         r_known = self.data[~self.data['Blended Rent'].isna()]['Transaction SF'].sum()
         r_unk = self.data[self.data['Blended Rent'].isna()]['Transaction SF'].sum()
         cv = [self.data[~self.data['Blended Rent'].isna()].shape[0], self.data[self.data['Blended Rent'].isna()].shape[0]]
         
         fig = plt.subplots(figsize=(17, 11))
         gs = gridspec.GridSpec(2, 2)
         axs = [None, None, None, None]
         axs[0] = plt.subplot(gs[0,:])
         axs[1] = plt.subplot(gs[1,:])
         
         x_values = ['Rent Known', 'Rent Unknown']
         y_values = [r_known, r_unk]
                  
         bar_plot = axs[0].bar(x_values, y_values)
         axs[0].get_yaxis().set_major_formatter('{:,.0f} SF'.format)
         
         # Add labels to the bars
         for i, v in enumerate(y_values):
             bar_plot[i].set_label(cv[i])
             axs[0].text(bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0,
                         v/2, str(cv[i]) , ha='center', va='center', fontsize=18, color='white', weight='bold')
          
         hist_plot = axs[1].hist(self.data['Blended Rent'], bins=num_bins)
         axs[1].get_xaxis().set_major_formatter('${:,.0f}'.format)

         fig[0].savefig('by_rent.png', dpi=300)
         
