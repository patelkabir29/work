# -*- coding: utf-8 -*-
"""
Updated on Sat Nov 11 16:56:00 2023

@author: dhaval.patel
"""

from Comps import Comps
from TIMS import JLL_TIMS, IPG_TIMS
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from datetime import date
import sys

tims_ipg = IPG_TIMS()
tims_jll = JLL_TIMS()


def by_bin(data, ranges, quarters, column='Transaction SF'):
    data = data.assign(size_bin=pd.cut(data[column].values, bins=ranges, right=True))
    data = data.assign(size_bin=data.size_bin.apply(lambda x: '{:.0f}k-{:.0f}k SF'.format(x.left/1e3, x.right/1e3)), axis=1)
    data.size_bin.replace('500k-1000k SF','500k-1M SF', inplace=True)
    data.size_bin.replace('1000k-infk SF','1M SF+', inplace=True)

    tmp = data.pivot_table(index='size_bin', columns='quarter', values=column, aggfunc=np.sum)[quarters]
    return tmp

# Function to add text labels at the bottom of each bar
def add_bar_labels(ax, rotatation=0):
    qtrs = ['Q4-22', 'Q1', 'Q2', 'Q3', 'Q4']
    L = len(ax.patches)
    for indx, p in enumerate(ax.patches):
        height = p.get_height()
        if height>0:
            ax.annotate(qtrs[int(indx/(L/5))], xy=(p.get_x() + p.get_width() / 2, 0),
                        xytext=(0, 5), textcoords='offset points', ha='center', va='bottom', fontsize=8, rotation=rotatation)
            
            ax.annotate('{:,.0f} SF'.format(height), xy=(p.get_x() + p.get_width() / 2, height+8e3),
                        xytext=(0,5), textcoords='offset points', ha='center', va='top', fontsize=8)


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

    data = pd.concat([c0.data, c1.data, c2.data, c3.data, c4.data])
    data['Borough'] = data['Borough'].str.strip()

    data['Class (Properties)'] = data['Class (Properties)'].apply(lambda x: x.pop() if isinstance(x,list) else x)

    df = combine_columns_with_precedence(data)
    data = data.assign(Class=df)

    return data

def leasing_plot(data, fig_path, text=True):
   
    shades_of_blue = ['#ADE1F5', '#6FB9E8', '#1F77B4', '#004C8C', '#ADE1F5']
    #shades_of_green = ['#9ACD32', '#228B22', '#006400', '#9ACD32']

    data = data.query('Class!="Land"')
    data = data.query('Borough!="Staten Island"')

    class_map = {'A': 'Class A', 'B': 'Class B', 'C': 'Class C'}
    data['Class'] = data['Class'].map(class_map)

    # Create a single figure with 2 subplots
    fig, axs = plt.subplots(2, 1, figsize=(17, 11), sharey=True)
    #gs = gridspec.GridSpec(2, 2)

    # Plotting the first bar graph on the first subplot
    #axs[0] = plt.subplot(gs[0, :])
    tmp = data.pivot_table(index='Class', columns='quarter', values='Transaction SF', aggfunc=np.sum)[['Q4 2022','Q1 2023',
                                                                                                        'Q2 2023', 'Q3 2023', 
                                                                                                        'Q4 2023']]
    tmp.plot.bar(rot=0, fontsize=16, color=shades_of_blue, ax=axs[0])
    axs[0].yaxis.set_major_formatter('{:,.0f} SF'.format)

    if text:
        add_bar_labels(axs[0], rotatation=0)
        # Labeling the top of each bar with its value
        # for patch in axs[0].patches:
        #     height = patch.get_height()
        #     axs[0].text(patch.get_x() + patch.get_width() / 2, height, f'{int(height):,}', ha='center', va='bottom', fontsize=8)

    # Plotting the second bar graph on the second subplot
    #axs[1] = plt.subplot(gs[1, :])
    data.pivot_table(index='Borough', columns='quarter', values='Transaction SF', aggfunc=np.sum)[['Q4 2022','Q1 2023',
                                                                                                   'Q2 2023', 'Q3 2023', 
                                                                                                   'Q4 2023']].plot.bar(rot=0, fontsize=16, color=shades_of_blue, ax=axs[1])
    axs[1].yaxis.set_major_formatter('{:,.0f} SF'.format)

    if text:
        add_bar_labels(axs[1], rotatation=0)
        # # Labeling the top of each bar with its value
        # for patch in axs[1].patches:
        #     height = patch.get_height()
        #     if height>0:
        #         axs[1].text(patch.get_x() + patch.get_width() / 2, height, f'{int(height):,}', ha='center', va='bottom', fontsize=8)

    # Adjust the layout and spacing between subplots
    plt.tight_layout()

    for ax in axs:
        ax.legend(fontsize=14)

    [_ax.set_xlabel('') for _ax in axs]

    # Save the figure to a PNG file
    fig.savefig(fig_path+f'leasing_text_{text}_1.png', dpi=450, bbox_inches='tight')


def test_plot(data, fig_path, text=True):
    shades_of_blue = ['#ADE1F5', '#6FB9E8', '#1F77B4', '#004C8C', '#ADE1F5']
    #shades_of_green = ['#9ACD32', '#228B22', '#006400', '#9ACD32']

    # By Size Bin
    fig =  plt.figure(figsize=(17, 11))
    gs = gridspec.GridSpec(2, 2, width_ratios=[1,1])
    axs = [plt.subplot(gs[0, :]), plt.subplot(gs[1,0]), plt.subplot(gs[1,1])]

    ranges = [0, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, np.inf]
    quarters = ['Q4 2022','Q1 2023','Q2 2023', 'Q3 2023', 'Q4 2023']

    x = by_bin(data, ranges, quarters, 'Transaction SF')
    x = pd.DataFrame([x[col][x[col]>0] for col in x.columns]).T
    x.plot.bar(rot=45, fontsize=16, color=shades_of_blue, ax=axs[0])
    if text:
        add_bar_labels(axs[0])

    x = by_bin(data, ranges, quarters, 'Building SF')
    x = pd.DataFrame([x[col][x[col]>0] for col in x.columns]).T
    
    x.plot.bar(rot=45, fontsize=16, color=shades_of_blue, ax=axs[1])
    if text:
        add_bar_labels(axs[1], rotatation=90)

    x = by_bin(data, ranges, quarters, 'Parking SF')
    x = pd.DataFrame([x[col][x[col]>0] for col in x.columns]).T
    x.plot.bar(rot=45, fontsize=16, color=shades_of_blue, ax=axs[2])
    if text:
        add_bar_labels(axs[2], rotatation=90)

    [_ax.yaxis.set_major_formatter('{:,.0f} SF'.format) for _ax in axs]
    [_ax.set_xlabel('') for _ax in axs]
    [_ax.legend(fontsize=14) for _ax in axs]

    plt.tight_layout(pad=1.0)

    fig.savefig(fig_path+f'test_{text}_1.png', dpi=450)

def quarterly_plot(data, fig_path, text=True):
    shades_of_blue = ['#ADE1F5', '#6FB9E8', '#1F77B4', '#004C8C', '#ADE1F5']
    dq = data.pivot_table(columns='quarter', values='Transaction SF', aggfunc=np.sum)
    # arrange columns in the right order
    dq = dq[['Q4 2022','Q1 2023','Q2 2023', 'Q3 2023', 'Q4 2023']]
    # create a bar plot
    fig, ax = plt.subplots(figsize=(17, 11))
    ax.bar(dq.columns, dq.values.flatten(), color=shades_of_blue)
    # set font size
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.yaxis.set_major_formatter('{:,.0f} SF'.format)
    if text:
        add_bar_labels(ax)
    plt.tight_layout()

    fig.savefig(fig_path+f'quarterly_plot.png', dpi=450)

    
def main(root_path):
    fig_path = root_path + '/MarketView/graphs/'
    data = create_data()
    leasing_plot(data, fig_path, text=True)
    leasing_plot(data, fig_path, text=False)
    test_plot(data, fig_path, text=True)
    test_plot(data, fig_path, text=False)
    quarterly_plot(data, fig_path, text=True)
    quarterly_plot(data, fig_path, text=False)

if __name__ == '__main__':
    root_path = sys.argv[1]
    main(root_path)
