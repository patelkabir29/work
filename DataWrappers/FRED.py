# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 14:29:28 2022

@author: kpatel
"""

from full_fred.fred import Fred
import pandas as pd
import requests
import json
from datetime import datetime, date
import matplotlib.pyplot as plt
import seaborn as sns

class FRED(object):
    
    def __init__(self):
        # connect to FRED
        self.fred = Fred("C:\\Users\\kpatel\\analytics-research\\IPG\\FRED_API_KEY.txt")
        #self.fred.set_api_key_file("C:\\Users\\kpatel\\analytics-research\\IPG\\FRED_API_KEY.txt")

        # get these series
        self.series_ids = {'10-Year Breakeven Inflation Rate': 'T10YIE',
                           '5-Year Breakeven Inflation Rate':'T5YIFR',
                           '10-Year Treasury Yield': 'DGS10',
                           'Producer Price Index: New Office Building Construction': 'PCU236223236223',
                           'Total Construction Spending: Office in the United States ':'TLOFCONS',                           
                           'All Employees: Lessors of Real Estate NYC': 'SMU36935615553110001A',
                           'Employees:NYC:All': 'SMS36935610000000001',
                           'Employees:NYC:Information':'SMU36935615000000001SA',
                           'Employees:NYC:Finance': 'SMU36935615500000001SA',
                           'Employees:NYC:Leisure & Hospitality':'SMU36935617072200001SA',
                           'Population:Manhattan': 'NYNEWY1POP',
                           'Unemployment:Manhattan': 'NYNEWY1URN',
                           'Unemployment:Queens': 'NYQUEE1URN',
                           'Unemployment:Bronx': 'NYBRON5URN',
                           'US Recession Probability': 'RECPROUSM156N',
                           'Dates of US Recessions': 'JHDUSRGDPBR',
                           'GDP:US': 'GDPC1',
                           'GDP:NY State': 'NYNQGSP',
                           'GDP:Manhattan': 'GDPALL36061',                           
                           'GDP:Annual Percentage Change': 'A191RI1Q225SBEA'} # Smoothed recession probabilities for the United States are obtained from a dynamic-factor markov-switching model applied to four monthly coincident variables: non-farm payroll employment, the index of industrial production, real personal income excluding transfer payments, and real manufacturing and trade sales.
        
        self.units = {'10-Year Breakeven Inflation Rate': None,
                      '5-Year Breakeven Inflation Rate': None,
                      '10-Year Treasury Yield': None,
                      'Producer Price Index: New Office Building Construction': None,
                      'Total Construction Spending: Office in the United States ': 1e6,
                      'All Employees: Lessors of Real Estate NYC': 1e3,
                      'Employees:NYC:All': 1e3,
                      'Employees:NYC:Information': 1e3,
                      'Employees:NYC:Finance': 1e3,
                      'Employees:NYC:Leisure & Hospitality': 1e3,
                      'Population:Manhattan': 1e3,
                      'Unemployment:Manhattan': None,
                      'Unemployment:Queens': None,
                      'Unemployment:Bronx': None,
                      'US Recession Probability': None,
                      'Dates of US Recessions': None,
                      'GDP:US': 1e9,
                      'GDP:NY State': 1e6,
                      'GDP:Manhattan': 1e3,
                      'GDP:Annual Percentage Change': None}
        
        self._get_series()
    
    def list_series(self):
        for sn in self.data.keys():
            print(sn)
        
    def _get_series(self):
        self.data = {}
        
        for sname, scode in self.series_ids.items():
            print('{}:{}'.format(sname, scode))
            temp = self.fred.get_series_df(scode)
            temp = temp.assign(date=pd.to_datetime(temp.date).apply(lambda x: x.date()))
            temp = temp[temp.date>=date(2000,1,1)]
            temp = temp[temp.value!='.']
            temp.value = temp.value.astype(float)
            
            if self.units[sname] is not None:
                temp.value = temp.value*self.units[sname]
                
            self.data[sname] = temp

    def get_series_by_name(self, sname):
        return self.data[sname]
    
    def plot_series(self, snames_list):
        df = {}

        fig = plt.figure(figsize=(11*2, 11*2))
        plt.tight_layout()
        gs = fig.add_gridspec(len(snames_list),1)
        ax = []
        
        for indx, sname in enumerate(snames_list):
            df = self.get_series_by_name(sname)
            ax.append(fig.add_subplot(gs[indx]))
                            
            ax[indx].plot(df.date, df.value, label=sname)
            ax[indx].set_title(sname, fontsize=18)
            ax[indx].legend(loc='upper left')
            ax[indx].grid()        