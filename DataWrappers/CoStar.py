# -*- coding: utf-8 -*-
"""
Created on Wed Nov  9 09:20:23 2022

@author: kpatel
"""

import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.ticker as mtick
from scipy.ndimage.filters import gaussian_filter1d

def quarter_to_date(x):
    Q = {1: (3,31),
         2: (6,30),
         3: (9,30),
         4: (12,31)}
         
    year, quarter = x.split('Q')
    year = int(year)
    if 'EST' in quarter:
        quarter = quarter[0]
    quarter = int(quarter)
    #year, quarter = [int(x) for x in x.split(' Q')]
    return date(year, Q[quarter][0], Q[quarter][1])

def month_to_date(x):
    M = {'Jan': (1,30),
         'Feb': (2,28),
         'Mar': (3,30),
         'Apr': (4,30),
         'May': (5,31),
         'Jun': (6,30),
         'Jul': (7,31),
         'Aug': (8,30),
         'Sep': (9,30),
         'Oct': (10,31),
         'Nov': (11,30),
         'Dec': (12,30)}
    
    if len(x)>0:
        month, year = x.split(' ')    
        return date(int(year), M[month][0], M[month][1])
    else: 
        return np.nan

def string_to_dollars(x):
    if len(x)>0:
        return float(x.strip('$'))
    else:
        return np.nan

market_area = {'Charlotte - NC': 3198,
               'Nashville - TN': 7484,
               'Austin - TX': 4279,
               'Raleigh - NC': 2118,
               'Tampa - FL': 2554}

class MultifamilyMetrics(object):
    
    def __init__(self, filename):
        
        self.sheet_names = ['Base Case', 'Moderate Upside', 'Moderate Downside', 'Severe Downside']
        
        self.filename = filename
        self.data = {}
        
        for _sheet in self.sheet_names:
            self.data[_sheet] = pd.read_excel(self.filename, sheet_name=_sheet)
            self.data[_sheet] = self.data[_sheet].assign(Date=self.data[_sheet]['Period'].apply(quarter_to_date))
            self.data[_sheet].set_index('Date', inplace=True)
            self.data[_sheet] = self.data[_sheet].assign(Population_Millions=self.data[_sheet]['Population']/1e6)        
            self.data[_sheet] = self.data[_sheet].assign(Market_Area = self.data[_sheet]['Geography Name'].map(market_area))
            self.data[_sheet] = self.data[_sheet].assign(Population_Density = self.data[_sheet]['Population']/self.data[_sheet]['Market_Area'])
        
        #self.data = self.data[self.data['Geography Name']!='Tampa - FL']
                
    def _get_historical(self):
        _today = date.today()
        return self.data['Base Case'].query('Date<=@_today')
            
    def _get_forecasted(self, scenario_name):
        _today = date.today()
        return self.data.query[scenario_name]('Date>@_today')
        
    def get_metric(self, geo_name, scenario, metric, date_min=date(1980,1,1), date_max=date.today() + relativedelta(years=5)):
        d = self.data[scenario].query('`Geography Name`==@geo_name').query('Date>=@date_min&Date<@date_max')[metric]        
        return d
    
    def split_metric(self, d):
        historical = d[d.index<=date.today()]
        forecast = d[d.index>date.today()]
        return (historical, forecast)
        
    def plot_split(self, d_split, ax, handles):
        line, = ax.plot(d_split[0])
        handles.append(line)
        
    def plot_metric_growth(self, date_min, date_max, metric):
        
        fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(17, 11))
        _handles = []
            
        for key, grp in self._get_historical().groupby('Geography Name'):
            
            if key=='Raleigh - NC':
                _lw = 3
            else:
                 _lw = 1
            
            line, = ax[0].plot(grp[metric], label=key, linewidth=_lw)
            _handles.append(line)
            
            x = grp
            growth = (x[metric].shift(-1)/x[metric][0:-1])-1
            growth = growth.rolling(4).sum()

            line2, = ax[1].plot(growth, label=key, linewidth=_lw)
        
        ax[0].tick_params(axis='x', labelsize=14)
        ax[0].tick_params(axis='y', labelsize=14)
        
        if 'Rent' not in metric:
            ax[0].get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
        
        ax[0].grid()
        ax[0].legend(handles=_handles, loc='upper left', fontsize=10) 
        ax[0].set_ylabel(metric, fontsize=20)

        ax[1].tick_params(axis='x', labelsize=14)
        ax[1].tick_params(axis='y', labelsize=14)
        ax[1].yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=2))
        ax[1].grid()
        ax[1].legend(handles=_handles, loc='upper left', fontsize=10)                
        ax[1].set_ylabel('YoY Growth', fontsize=20)
        
        fig.savefig('{}.png'.format(metric.replace('/SF','_PSF')), dpi=300, transparent=True, bbox_inches='tight')
        
class HotelMetrics(object):
    
    def __init__(self, filename):
        
        self.sheet_names = ['Properties', 'Overall', 'Economy', 'Midscale', 'Upper Midscale', 'Upper Upscale', 'Independent']
        
        self.filename = filename
        self.data = {}
        
        for _sheet in self.sheet_names:
            try:
                self.data[_sheet] = pd.read_excel(self.filename, sheet_name=_sheet)
            
                if _sheet!='Properties':        
                    self.data[_sheet] = self.data[_sheet].assign(Date=self.data[_sheet]['Period'].apply(month_to_date))            
                    self.data[_sheet].set_index('Date', inplace=True)                    
            except: 
                pass
                
    def _get_properties(self, city=None):
        if city is None:
            return self.data['Properties'].query('`Operational Status`=="Open"')        
        else:
            return self.data['Properties'].query('`Operational Status`=="Open" & City==@city')
        
    def _get_historical(self):
        _today = date.today()
        return self.data['Base Case'].query('Date<=@_today')
        
    def get_metric(self, geo_name, scenario, metric, date_min=date(1980,1,1), date_max=date.today() + relativedelta(years=5)):
        d = self.data[scenario].query('`Geography Name`==@geo_name').query('Date>=@date_min&Date<@date_max')[metric]        
        return d
    
    def split_metric(self, d):
        historical = d[d.index<=date.today()]
        forecast = d[d.index>date.today()]
        return (historical, forecast)
        
    def plot_split(self, d_split, ax, handles):
        line, = ax.plot(d_split[0])
        handles.append(line)
        
    def plot_metric_growth(self, date_min, date_max, metric):
        
        fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(17, 11))
        _handles = []
            
        for key, grp in self._get_historical().groupby('Geography Name'):
            
            if key=='Raleigh - NC':
                _lw = 3
            else:
                 _lw = 1
            
            line, = ax[0].plot(grp[metric], label=key, linewidth=_lw)
            _handles.append(line)
            
            x = grp
            growth = (x[metric].shift(-1)/x[metric][0:-1])-1
            growth = growth.rolling(4).sum()

            line2, = ax[1].plot(growth, label=key, linewidth=_lw)
        
        ax[0].tick_params(axis='x', labelsize=14)
        ax[0].tick_params(axis='y', labelsize=14)
        
        if 'Rent' not in metric:
            ax[0].get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
        
        ax[0].grid()
        ax[0].legend(handles=_handles, loc='upper left', fontsize=10)
        ax[0].set_ylabel(metric, fontsize=20)

        ax[1].tick_params(axis='x', labelsize=14)
        ax[1].tick_params(axis='y', labelsize=14)
        ax[1].yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=2))
        ax[1].grid()
        ax[1].legend(handles=_handles, loc='upper left', fontsize=10)                
        ax[1].set_ylabel('YoY Growth', fontsize=20)
        
        fig.savefig('{}.png'.format(metric.replace('/SF','_PSF')), dpi=300, transparent=True, bbox_inches='tight')
        
class Industrial(object):
    
    def __init__(self, filename="C:\\Users\\kpatel\\data\\industrial\\Industrial.xlsx"):
        self.filename = filename
        self.data = {}
        
        #self._read_property_data()
        self._read_market_data()
        
    def _read_property_data(self):
        sheet_names = ['Properties', 'Overall', 'Class A', 'Class B', 'Class C', 'Class F']
                        
        for _sheet in sheet_names:
            print(_sheet)
            tmp = pd.read_excel(self.filename, sheet_name=_sheet)
            tmp.replace('-', np.nan, inplace=True)
            tmp = tmp.drop(index=tmp[tmp.Period.apply(lambda x: 'QTD' in x)].index)
            
            self.data[_sheet] = tmp
            
            if _sheet!='Properties':
                self.data[_sheet] = self.data[_sheet].assign(Date=self.data[_sheet]['Period'].apply(quarter_to_date))
                self.data[_sheet].set_index('Date', inplace=True)
                
        self.cols = {'metrics': ['Deliveries Bldgs',
                                 'Deliveries SF',
                                 'Gross Absorption SF Total',
                                 'Inventory Bldgs',
                                 'Inventory SF',
                                 'Leasing Activity Deals Total',
                                 'Leasing Activity SF Total',
                                 'NNN Rent Direct',
                                 'NNN Rent Overall',
                                 'NNN Rent Sublet',
                                 'Net Absorption SF Direct',
                                 'Net Absorption SF Sublet',
                                 'Net Absorption SF Total',
                                 'Occupancy Percent',
                                 'Occupancy SF',
                                 'Total Available SF Total',
                                 'Vacant Percent % Total',
                                 'Vacant SF Total']}

    def _read_market_data(self):
        sheet_names = ['Overall', 'Bronx', 'North BK', 'South BK', 'Northeast QN', 'Central QN', 'South QN', 'Northwest QN', 'Class A']
                       
        for _sheet in sheet_names:
            tmp = pd.read_excel(self.filename, sheet_name=_sheet)
            tmp.replace('-', np.nan, inplace=True)
            tmp = tmp.drop(index=tmp[tmp.Period.apply(lambda x: 'QTD' in x)].index)
            
            self.data[_sheet] = tmp        
            self.data[_sheet] = self.data[_sheet].assign(Date=self.data[_sheet]['Period'].apply(quarter_to_date))
            self.data[_sheet].set_index('Date', inplace=True)
            self.data[_sheet].sort_index(ascending=True, inplace=True)
    
    def _plot_metric(self, metric, segments=['Overall'], date_min=date(2013,1,1), date_max=date(2022,1,1), smoothing=False, param=2, figsize=(17,11), scatter=False):
        
        fig, ax = plt.subplots(nrows=1, ncols=1, sharex=True, figsize=figsize)
        _handles = []
        
        for _segment in segments:
            _d = self.data[_segment].query('Date>=@date_min&Date<=@date_max')[metric]
            _d = _d.dropna()
            
            if scatter:
                line, = ax.plot(_d, label='{}: {}'.format(_segment, metric), marker='.', linestyle='', markersize=11)
                _handles.append(line)
            
                rolling_std = _d.rolling(window=4).std()
                ax.fill_between(_d.index, _d - rolling_std, _d + rolling_std, alpha=0.2, label='Volatility')
            
            if smoothing=='rolling':
                _d =  self.data[_segment].query('Date>=@date_min')[metric].rolling(param).mean()
            elif smoothing=='gaussian':
                _d = self.data[_segment].query('Date>=@date_min')[metric]
                _d.update(pd.Series(gaussian_filter1d(_d.values, param ), index=_d.index))                            
                            
            _d = _d.dropna()
            line, = ax.plot(_d, label=_segment, linewidth=3)
            _handles.append(line)
            ax.grid(False)
            #lt.grid(False)
            #vals = [self.data[key][metric][-1] for key in segments]
            #sort_indx = list(np.array(vals).argsort()[::-1])
            #_handles = sorted(_handles, key=lambda x:sort_indx.index(_handles.index(x)))            
                    
        ax.tick_params(axis='x', labelsize=14)
        ax.tick_params(axis='y', labelsize=14)
        
        if 'SF' in metric:
            ax.get_yaxis().set_major_formatter('{:,.0f} SF'.format)
        
        if 'Rent' in metric:
            ax.get_yaxis().set_major_formatter('${:,.0f}'.format)
            
        if '%' in metric:
            ax.get_yaxis().set_major_formatter(mtick.PercentFormatter(1.0, decimals=2))
        
        #ax.grid()
        ax.legend(handles=_handles, loc='upper left', fontsize=10)
        ax.set_ylabel(metric, fontsize=20)
        
        fig.savefig('{}.png'.format(metric.replace("/","_")), dpi=300, transparent=True, bbox_inches='tight')        

    def _plot_property_attribute(self, metric, segments=['Overall'], date_min=date(1990,1,1), smoothing=False):
        _d = self.data['Properties'].dropna(subset=['Year Built'])
        _d = _d.assign(Date=_d['Year Built'].apply(lambda x: date(int(x),6,1)))
        _d = _d.sort_values('Date')
        _d.set_index('Date', inplace=True)
      
        fig, ax = plt.subplots(nrows=1, ncols=1, sharex=True, figsize=(17, 11))
        _handles = []
        line, = ax.plot(_d.groupby('Date')[metric].mean().dropna(), linewidth=2)
        _handles.append(line)
        
        ax.tick_params(axis='x', labelsize=14)
        ax.tick_params(axis='y', labelsize=14)
        
        #ax[0].get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
        if '%' in metric:
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=2))
        
        ax.grid(False)
        ax.legend(handles=_handles, loc='upper left', fontsize=10) 
        ax.set_ylabel(metric, fontsize=20)
        
        fig.savefig('{}.png'.format(metric), dpi=300, transparent=True, bbox_inches='tight')
        
        return _d
        
    def _segment(self, building_class="A"):
        
        STATUS = ['Existing', 'Under Renovation']
        
        props = self.data['Properties'].query('`Building Class`==@building_class&`Building Status` in @STATUS')
        metrics = self.data['Class {}'.format(building_class)]
        
        return {'properties': props,
                'metrics': metrics}
    
class Comps(object):
    
    def __init__(self, file_list):
        
        self.data = pd.DataFrame()
        
        for file in file_list:
            tmp = pd.read_csv(file)
            self.data = self.data.append(tmp)
                        
    def _process(self):
        
        self.data = self.data.rename(columns={col: col.replace("'",'').strip() for col in self.data.columns})
        self.data = self.data.drop(columns=[col for col in self.data.columns if (('Unnamed' in col)|(len(col)==0))])
        self.data['Sign Date'].apply(lambda x: x.replace("'",''))
                    
        for col in self.data.columns:
            try:
                self.data = self.data.assign(**{col: self.data[col].apply(lambda x: x.strip("'").strip())})
            except:
                pass

        self.data = self.data.assign(**{'SF Leased': self.data['SF Leased'].apply(lambda x: x.replace('.',',').replace(',',''))})
        self.data = self.data.assign(**{'SF Leased': self.data['SF Leased'].apply(lambda x: int(x))})
        
        if 'Term' in self.data.columns:
            self.data = self.data.assign(Term=self.data.Term.apply(self._term_to_months))        
        
        DATE_COLS = [col for col in self.data.columns if 'Date' in col]
        
        for col in DATE_COLS:
            self.data = self.data.assign(**{col: self.data[col].apply(month_to_date)})

        self.data = self.data.assign(**{'Rent/SF/Yr': self.data['Rent/SF/Yr'].apply(string_to_dollars)})        
        if 'Submarket' in self.data.columns:
            self.data = self.data.assign(Submarket=self.data.Submarket.apply(lambda x: x.strip(' Lo').strip(' Lor')))
        
            SM_TO_M = {'Bronx': 'Bronx',
                       'Central Queens': 'Queens',
                       'North Brooklyn': 'Brooklyn',
                       'Northwest Queens': 'Queens',
                       'Northeast Queens': 'Queens',
                       'South Brooklyn': 'Brooklyn',
                       'South Queens': 'Queens',
                       'Staten Island': 'Staten Island',
                       'Stuyvesant Heights': 'Brooklyn',
                       'Williamsburg': 'Brooklyn'}
            
            self.data = self.data.assign(Market=self.data.Submarket.map(SM_TO_M))
                
    def _term_to_months(self, x):
        if len(x)==0:
            return np.nan
        elif 'yrs' in x:
            return int(x.strip(' yrs'))*12
        
def ForLease(object):
    
    def __init__(self, filename):
        data = pd.read_excel(filename)
        data = data.replace('-', np.nan)
        
        # convert rent columns to float
        for col in data.columns:
            if 'Rent' in col:
                data = data.assign(**{col: data[col].astype(float)})
        
        # convert ceiling height to float
        data = data.assign(**{'Ceiling Ht': data['Ceiling Ht'].apply(_convert_ceiling_ht)})
        data = data.assign(**{'Drive Ins': data['Drive Ins'].apply(_convert_driveins)})

    def _convert_ceiling_ht(x):
        try:
            a,b = x.replace('"','').split("'")
            return float(a) + float(b)/12
        except:
            return np.nan
        
    def _convert_driveins(x):
        if type(x)==str:
            if x=='None':
                return 0
            elif x=='Yes':
                return np.nan
            elif len(x)==1:
                return int(x)
            elif len(x)>1:
                return int(x[0])
        else:
            return np.nannn