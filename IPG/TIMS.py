# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 14:53:04 2023

@author: kpatel

"""
import pandas as pd
import matplotlib.pyplot as plt
from pyairtable import Api, Base, Table
from pyairtable.formulas import match
import matplotlib.dates as mdates
from scipy.interpolate import CubicSpline
import numpy as np

class IPG_TIMS(object):
    
    def __init__(self):

        API_KEY = 'keyAxyrUn7wGiLY6v'
        table = Table(API_KEY, 'app2QOo2ody2LwU9D', 'IPG TIMs')
        data = table.all(view='Raw Data')
        self.data = pd.DataFrame([x['fields'] for x in data])
        
        
    def get_reqs(self, req_type='Building'):
        
        if req_type=='Building':
            return dict(self.data[[col for col in self.data.columns if 'WH' in col]].sum())
        elif req_type=='Parking':
            return dict(self.data[[col for col in self.data.columns if 'PK' in col]].sum())

    def get_industries(self, req_type='Building'):
        pass
    
class JLL_TIMS(object):
    
    def __init__(self):

        self.qmap = {3:'Q1',
                     6:'Q2',
                     9:'Q3',
                     12:'Q4'}

        # API_KEY = 'keyAxyrUn7wGiLY6v'
        # table = Table(API_KEY, 'app2QOo2ody2LwU9D', 'JLL TIMs Dashboards')
        # data = table.all(view='Raw Data')

        API_KEY = 'patK2kzMIkyf4b6to.a3b0ba7bd169cae47034d5a8a80246ac1f5948407b3e5a0d866d0cd5b790b6ee' #'keyAxyrUn7wGiLY6v'
        table = Table(API_KEY, 'app2QOo2ody2LwU9D', 'JLL TIMs Dashboards')
        data = table.all(view='Raw Data')
        
        self.data = pd.DataFrame([x['fields'] for x in data])
        self.data = self.data.assign(Date=pd.to_datetime(self.data.Date))
        self.data = self.data.assign(Date=self.data.Date.apply(lambda x: x.date()))
        self.data = self.data.assign(quarter=self.data.Date.apply(lambda x: x.month).map(self.qmap))
        self.data = self.data.assign(year=self.data.Date.apply(lambda x: x.year))
        
        self.building = self.data[self.data.Category=='Building']
        self.parking = self.data[self.data.Category=='Parking']
        
    def plot_historical_demand(self, category='Building'):
        
        fig, axs = plt.subplots(1, 1, figsize=(17, 11))
        
        if category=='Building':                            
            d = self.building
        elif category=='Parking':
            d = self.parking
        
        #x_values = zip(d.quarter, d.year)1
        #x_values = [str(b)+'-'+a for a,b in x_values]
        x_values = d.Date
        y_values = d['Total SF'].values
        cv = d['Active Reqs'].values
        
        bar_plot = axs.bar(x_values, y_values, width=1.6)
        
        # Add labels to the bars
        for i, v in enumerate(cv):
            #bar_plot[i].set_label(v)
            axs.text(x=bar_plot[i].get_x() + bar_plot[i].get_width() / 2.0, 
                            y=y_values[i]/2, s=str(int(v)) , ha='center', va='center', fontsize=16, color='white', weight='bold')
        
        # Add dashed line with text for most recent quarter
        axs.plot([axs.get_xlim()[0], axs.get_xlim()[1]], [y_values[-1], y_values[-1]],
                 ls='--', c='k')                
                
        axs.set_title('JLL: Historical Demand (smoothed)', fontsize=16)
        axs.get_yaxis().set_major_formatter('{:,.0f} SF'.format)
                        
        fig.savefig('JLL_Historical_Demand.png', dpi=300)        
                
    def plot_smooth_historical_demand(self, fig_path, category='Building', interval_in_months=3):

        fig, axs = plt.subplots(1, 1, figsize=(17, 11))

        if category == 'Building':
            d = self.building
        elif category == 'Parking':
            d = self.parking

        # Convert date values to numerical format for interpolation
        x_num = mdates.date2num(d.Date)
        y_values = d['Total SF'].values

        # Create a cubic spline interpolation
        cs = CubicSpline(x_num, y_values)

        # Generate finer x values for the smooth curve
        x_fine_num = np.linspace(x_num.min(), x_num.max(), 500)

        # Evaluate the spline at the finer x values
        y_fine = cs(x_fine_num)

        # Convert the numerical x values back to dates
        x_fine = mdates.num2date(x_fine_num)

        # Plot the original data points
        axs.plot(d.Date, y_values, 'o', label='Actuals')

        # Plot the smooth curve
        axs.plot(x_fine, y_fine)

        #axs.set_xlabel('Date')
        axs.set_ylabel('Total SF')
        axs.legend()
        axs.set_title('JLL: Historical Demand')

        axs.get_yaxis().set_major_formatter('{:,.0f} SF'.format)
        axs.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        #if category=='Building':
        axs.xaxis.set_major_locator(mdates.MonthLocator(interval=interval_in_months))  # Show tick marks every 3 months
        fig.autofmt_xdate()
        axs.tick_params(axis='both', labelsize=14)
        
        check = 0
        flag = 0

        for x, y, yc in zip(d.Date, d['Active Reqs'], y_values):
            if category == 'Building':
                if check == 0:
                    yp = yc + 150e3
                    check = 1
                else:
                    # calculate the absolute change in y
                    dy = abs(yc - y_prev)/1e6
                    if (dy <= 0.4) and (flag==0):
                        yp = yc - 450e3
                        flag = 1
                    elif (dy <= 0.4) and (flag==1):
                        yp = yc + 150e3
                        flag = 0
                    else:
                        yp = yc + 150e3
                        flag = 0
            
                y_prev = yc
            else:
                yp = yc + 50e3
            axs.text(x, yp, str(int(y)), fontsize=16, ha='center', va='bottom')
        
        plt.show()
        fig.savefig(fig_path+'/JLL_{}_Smoothed_Demand.png'.format(category), dpi=300)
