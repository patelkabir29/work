# -*- coding: utf-8 -*-
"""
Updated on Sat Nov 11 16:56:00 2023

@author: dhaval.patel
"""

from Comps import Comps
from TIMS import JLL_TIMS, IPG_TIMS
import matplotlib.pyplot as plt
import seaborn as sns

import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from datetime import date
import sys


#tims_ipg = IPG_TIMS()
#tims_jll = JLL_TIMS()


def combine_columns_with_precedence(df):
    """
    This function combines two columns into a single column.
    It gives precedence to col1 and takes the value from col2 only if col1 is empty.
    """
    # Use col1 value if it's not empty; otherwise, use col2 value
    combined = df.apply(lambda x: x['Class (Properties)'] if pd.notnull(x['Class (Properties)']) and x['Class (Properties)'] != '' else x['Class'], axis=1)
    return combined

# Function to create data
def create_data():

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
    c5 = Comps('Q1 2024')
    c5.data = c5.data.assign(quarter='Q1 2024')

    data = pd.concat([c1.data, c2.data, c3.data, c4.data, c5.data])
    data['Borough'] = data['Borough'].str.strip()
    data['Class (Properties)'] = data['Class (Properties)'].apply(lambda x: x.pop() if isinstance(x,list) else x)
    df = combine_columns_with_precedence(data)
    data = data.assign(Class=df)

    return data

def add_bar_text(ax, per_ch_text, per_ch, shape, days, project_text=True, fontsize=10):
    bar_pos = []
    bar_val = []
    for container in ax.containers:
        ax.bar_label(container, fontsize=fontsize-3, fmt='{:,.0f} SF', 
                     label_type='edge', padding=3,
                     fontweight='bold')  # Labeling each bar
        
        for bar in container:
            bar_pos.append(bar.get_x() + bar.get_width() / 2)
            bar_val.append(bar.get_height())
            width = bar.get_width()
    
    text_null = [0]*len(bar_val)
    if days!=90:
        if project_text:
            # Check shape is not 0
            text_null = [0]*len(bar_val)
            if shape[0] != 0:
                for i in range(len(bar_val)):
                    if i > shape[0]*shape[1]-shape[0]-1:
                        
                        projected = (bar_val[i]/days)*(90-days) + bar_val[i]
                        proj_ch = (projected-bar_val[i-shape[0]])/bar_val[i-shape[0]]*100
                        # Check if it is 100, -100, inf, or nan
                        if proj_ch == 100 or proj_ch == -100 or np.isinf(proj_ch) or np.isnan(proj_ch):
                            proj_ch = 0
                        per_ch.append(proj_ch)
                        per_ch_text.append(f'{abs(proj_ch):.0f}%') #{abs(projected):.0f} SF\n
                        if projected == 0:
                            text_null.append(0)
                        else:
                            text_null.append(f'{abs(projected):,.0f} SF\n~proj')
                        bar_pos.append(bar_pos[i]+width*0.75)
                        bar_val.append(projected)
            else:
                projected = (bar_val[-1]/days)*(90-days) + bar_val[-1]
                proj_ch = (projected-bar_val[-2])/bar_val[-2]*100
                # Check if it is 100, -100, inf, or nan
                if proj_ch == 100 or proj_ch == -100 or np.isinf(proj_ch) or np.isnan(proj_ch):
                    proj_ch = 0
                # convert to list
                per_ch = per_ch.tolist()
                per_ch.append(proj_ch)
                per_ch_text.append(f'{abs(proj_ch):.0f}%')
                if projected == 0:
                    text_null.append(0)
                else:
                    text_null.append(f'{abs(projected):,.0f} SF\n~proj')
                bar_pos.append(bar_pos[-1]+width*0.3)
                bar_val.append(projected)


    for i, (bar, value, per, text, text_p) in enumerate(zip(bar_pos, bar_val, per_ch, per_ch_text, text_null)):
            # Determine arrow direction and color based on percentage change
        arrow = '↑' if per > 0 else '↓'
        color = 'green' if per > 0 else 'red'
        plus_minus = '+' if per > 0 else '-' if per < 0 else ''

        if per == 0:
            text = ''
            arrow = ''
        
        # Display percentage change and arrow at the top of each bar
        ax.text(bar, value+60000, f'{plus_minus}{text} {arrow}', ha='center', color=color, fontweight='bold', fontsize=fontsize-2)
        if project_text:
            if text_p != 0:
                ax.text(bar, value-10000, f'{text_p}', ha='center',fontweight='bold', fontsize=fontsize-4)

     

def per_change(values, shape=(3,5)): 
    import warnings
    warnings.filterwarnings("ignore")
    # Reshape the values to create a matrix of 3x5
    values = values.reshape(shape)
    per_ch = np.diff(values, axis=1)/values[:, :-1]*100
    per_ch = np.hstack((np.zeros((shape[0],1)), per_ch))
    # Replace NaN, inf, 100, -100 with zeros
    per_ch[np.isnan(per_ch)] = 0
    per_ch[np.isinf(per_ch)] = 0
    per_ch[per_ch == 100] = 0
    per_ch[per_ch == -100] = 0
    # Generate text for percentage change
    per_ch_text = [f'{abs(val):.0f}%' if val != 0 else '' for row in per_ch.T for val in row]   
    per_ch = [val for row in per_ch.T for val in row]

    return per_ch_text, per_ch
        
def leasing_plot(data, days):
    """ Function creates bar plot for leasing activity
        Hue: Quarter
        x-axis: Class
        y-axis: Transaction SF
    """ 
    data = data.query('Class!="Land"')
    data = data.query('Borough!="Staten Island"')

    class_map = {'A': 'Class A', 'B': 'Class B', 'C': 'Class C'}
    data.loc[:,'Class'] = data['Class'].map(class_map)
    ag_data = data.groupby(['Class', 'quarter'], as_index=False)['Transaction SF'].sum()

    # Order the quarters
    #hue_order = ['Q4 2022', 'Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023']
    hue_order = ['Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023', 'Q1 2024']
    ag_data['quarter'] = pd.Categorical(ag_data['quarter'], hue_order)
    
    # Create missing pairs of class and quarter
    ag_data = ag_data.set_index(['Class', 'quarter']).unstack(fill_value=0).stack().reset_index()
    values = ag_data['Transaction SF'].values
    per_ch_text, per_ch = per_change(values)

    fig, axs = plt.subplots(2, 1, figsize=(17, 11), sharey=True)

    sns.barplot(x='Class', y='Transaction SF', hue='quarter', data=ag_data, palette='Blues', hue_order=hue_order, ax=axs[0])
    add_bar_text(axs[0], per_ch_text, per_ch, (3,5), days) #3,5
    axs[0].set_ylabel('Transaction SF', fontsize=12)
    axs[0].legend()
    axs[0].yaxis.set_major_formatter('{:,.0f} SF'.format)
    axs[0].set_xlabel('')
    axs[0].set_ylim(0, 1.1e6)
    axs[0].legend(loc='upper left')

    # Plotting the second bar graph on the second subplot
    ag_data = data.groupby(['Borough', 'quarter'], as_index=False)['Transaction SF'].sum()
    ag_data['quarter'] = pd.Categorical(ag_data['quarter'], hue_order)
    ag_data = ag_data.set_index(['Borough', 'quarter']).unstack(fill_value=0).stack().reset_index()
    values = ag_data['Transaction SF'].values
    print(ag_data.Borough.unique())
    per_ch_text, per_ch = per_change(values)
    sns.barplot(x='Borough', y='Transaction SF', hue='quarter', data=ag_data, palette='Blues', hue_order=hue_order, ax=axs[1])
    add_bar_text(axs[1], per_ch_text, per_ch, (3,5), days) #3,5
    axs[1].set_ylabel('Transaction SF', fontsize=12)
    axs[1].legend()
    axs[1].yaxis.set_major_formatter('{:,.0f} SF'.format)
    axs[1].set_xlabel('')
    # add legend on the left
    axs[1].legend(loc='upper left')

    plt.show()
      



def by_bin(data, ranges, hue_order, column='Transaction SF'):
    
    if column == 'Transaction SF':
        labels = ['0-25K', '25K-50K', '50K-100K', '100K-250K']
        data['size_bin'] = pd.cut(data[column], bins=ranges, labels=labels, right=True)
        
    # tmp = data.pivot_table(index='size_bin', columns='quarter', values=column, aggfunc=np.sum)[quarters]
    ag_data = data.groupby(['size_bin', 'quarter'], as_index=False)[column].sum()

    # Order the quarters
    ag_data['quarter'] = pd.Categorical(ag_data['quarter'], hue_order)

    # Create missing pairs of class and quarter
    ag_data = ag_data.set_index(['size_bin', 'quarter']).unstack(fill_value=0).stack().reset_index()
    
    values = ag_data[column].values
    per_ch_text, per_ch = per_change(values, shape=(4,5))

    
    return ag_data, per_ch_text, per_ch, data

def test_plot(data, days):
    
    data = data.query('Class!="Land"')
    data = data.query('Borough!="Staten Island"')

    ranges = [0, 25000, 50000, 100000, 250000]
    #hue_order = ['Q4 2022', 'Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023']
    hue_order = ['Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023', 'Q1 2024']

    df1, per_ch_text, per_ch, data = by_bin(data, ranges, hue_order, 'Transaction SF')
    df2, per_ch_text2, per_ch2, data = by_bin(data, ranges, hue_order, 'Building SF')
    df3, per_ch_text3, per_ch3, data = by_bin(data, ranges, hue_order, 'Parking SF')

    fig, axs = plt.subplots(3, 1, figsize=(23, 18), sharey=True)

    sns.barplot(x='size_bin', y='Transaction SF', hue='quarter', data=df1, palette='Blues', hue_order=hue_order, ax=axs[0])
    add_bar_text(axs[0], per_ch_text, per_ch, (4,5), days, True) #4,5
    axs[0].set_ylabel('Transaction SF', fontsize=12)
    axs[0].legend()
    axs[0].yaxis.set_major_formatter('{:,.0f} SF'.format)
    axs[0].set_xlabel('')
    axs[0].set_ylim(0, 1.1e6)

    # Plotting the second bar graph on the second subplot
    sns.barplot(x='size_bin', y='Building SF', hue='quarter', data=df2, palette='Blues', hue_order=hue_order, ax=axs[1])
    add_bar_text(axs[1], per_ch_text2, per_ch2, (4,5), days, False) #4,5
    axs[1].set_ylabel('Building SF', fontsize=12)
    axs[1].legend()
    axs[1].yaxis.set_major_formatter('{:,.0f} SF'.format)
    axs[1].set_xlabel('')

    # Plotting the second bar graph on the second subplot
    sns.barplot(x='size_bin', y='Parking SF', hue='quarter', data=df3, palette='Blues', hue_order=hue_order, ax=axs[2])
    add_bar_text(axs[2], per_ch_text3, per_ch3, (4,5), days, False) #4,5
    axs[2].set_ylabel('Parking SF', fontsize=12)
    axs[2].legend()
    axs[2].yaxis.set_major_formatter('{:,.0f} SF'.format)
    axs[2].set_xlabel('')

    plt.show()


def quarterly_plot(data, days):
    data = data.query('Class!="Land"')
    data = data.query('Borough!="Staten Island"')

    #hue_order = ['Q4 2022', 'Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023']    
    hue_order = ['Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023', 'Q1 2024']
    # Group by quarter
    ag_data = data.groupby(['quarter'], as_index=False)['Transaction SF'].sum()
    ag_data['quarter'] = pd.Categorical(ag_data['quarter'], hue_order)
    ag_data = ag_data.sort_values('quarter')
    values = ag_data['Transaction SF'].values

    # Calculate percentage change
    per_ch = np.diff(values)/values[:-1]*100
    per_ch[np.isnan(per_ch)] = 0
    per_ch[np.isinf(per_ch)] = 0
    per_ch[per_ch == 100] = 0
    per_ch[per_ch == -100] = 0

    per_ch = np.hstack((np.zeros(1), per_ch))

    per_ch_text = [f'{abs(val):.0f}%' if val != 0 else '' for val in per_ch]   

    # Count number of entries in each quarter
    count = data.groupby(['quarter'], as_index=False)['Transaction SF'].count()
    count['quarter'] = pd.Categorical(count['quarter'], hue_order)
    count = count.sort_values('quarter')
    count = count['Transaction SF'].values
       
       
    
    # create a bar plot
    fig, ax = plt.subplots(figsize=(17, 11))
    sns.barplot(x='quarter', y='Transaction SF', data=ag_data, palette='Blues', ax=ax)
    # add the count values on the middle of the bars

    for p, text in zip(ax.patches, count):
        ax.annotate(f'{text}', (p.get_x()+p.get_width()/2, p.get_height()/2), ha='center', fontsize=14, fontweight='bold')


    add_bar_text(ax, per_ch_text, per_ch, (0,0), days, fontsize=12) #4,5
    ax.set_ylabel('Transaction SF', fontsize=14)
    ax.yaxis.set_major_formatter('{:,.0f} SF'.format)
    ax.set_xlabel('')
    ax.set_ylim(0, 1.7e6)
    
    
def main(root_path):
    #fig_path = root_path + '/MarketView/graphs/'
    data = create_data()
    days = 90
    leasing_plot(data, days)
    test_plot(data, days)
    quarterly_plot(data, days)
    

if __name__ == '__main__':
    root_path = sys.argv[1]
    main(root_path)
