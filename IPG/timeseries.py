# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 14:33:57 2021

@author: kpatel
"""

from datetime import date
from dateutil.relativedelta import relativedelta
from scipy.ndimage.filters import gaussian_filter1d
from statsmodels.stats.weightstats import DescrStatsW
import scipy.stats as st
from math import sqrt
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

def add_conf_intervals(stats, level=0.95):
    # compute the PPF assuming a normal distribution
    
    z = st.norm.ppf(1 - (1-level)/2)
    
    ci = z*stats['std']/sqrt(stats['count'])
    stats['conf_interval'] = ci
    stats['mean_true_lower'] = stats['mean'] - ci
    stats['mean_true_upper'] = stats['mean'] + ci

    #ci = z*stats['velocity_std']/sqrt(stats['velocity_count'])
    #stats['velocity_conf_interval'] = ci1
    #stats['velocity_true_upper'] = stats['velocity'] + ci

    return stats

def generateTimeSeries(leases, metric='net_effective_rent', time_col='execution_date', method='weighted', window_months=12, window_shift=1, sigma=2, quantile_range=(0,1)):
    
    time_col_derived = time_col + '_derived'
    
    leases = leases.assign(**{time_col_derived: leases[time_col].apply(lambda x: date(x.year, x.month, 1))})
    
    START = leases[time_col_derived].min()
    END = leases[time_col_derived].max()
            
    w_start = START
    if w_start < date(1998,1,1):
        w_start = date(1998,1,1)
        
    w_end = START
    S = list()
    
    while w_end < END:

        w_end = w_start + relativedelta(months=window_months)

        l = leases.query('{} >= @w_start &'\
                         '{} < @w_end &'\
                         '{} >= {}.quantile(@quantile_range[0]) & {} <= {}.quantile(@quantile_range[1])'.format(time_col, time_col, metric, metric, metric, metric))
        
    
        #l = l[['property_id', metric,'transaction_sqft', 'building_size']].dropna()
        #lv = l.transaction_sqft.sum()
        #SF_TOTAL = l[['property_id', 'building_size']].drop_duplicates()['building_size'].sum()

        if method=='weighted':
            
            stats = DescrStatsW(l[metric], l['transaction_sqft'])
            
            stats = {'start': w_start,
                     'end': w_end,
                     'mean': stats.mean,
                     'std': stats.std,
                     'var': stats.var,
                     'count': len(stats.data)}
                     #'velocity': lv/SF_TOTAL}
            
        elif method=='straight':
        
            stats = {'start': w_start,
                     'end': w_end,
                     'mean': l[metric].mean(),
                     'std': l[metric].std(),
                     'count': len(l)}
                     #'velocity': lv/SF_TOTAL}
        
        stats = add_conf_intervals(stats)
        S.append(stats)
        
        w_start = w_start + relativedelta(months=window_shift)
            
    df = pd.DataFrame()
    
    for attrib in stats.keys():
        df = df.assign(**{attrib: [x[attrib] for x in S]})
    
    df = df.assign(mid=df.start + relativedelta(months=round(window_months/2)))
    
    for col in df.columns:
        if ('mean' in col)|('velocity' in col):
            df = df.assign(**{col+'_smooth': gaussian_filter1d(df[col], sigma=sigma)})
            
    return df

def forecastTimeSeries(data, time_col, forecast_col, order=(1,1,12), n_samples=1):
    
    data = data.assign(point_type='actual')

    t = data[time_col].max()
    f_points = [t+relativedelta(months=x) for x in range(1,n_samples+1)]
                
    model = ARIMA(data[forecast_col], order=order)
    model_fit = model.fit()
    
    # make prediction
    forecasted = model_fit.predict(len(data), len(data)+n_samples-1, typ='levels')
    
    df = pd.DataFrame({data[time_col].name: f_points,
                       data[forecast_col].name: forecasted})
    
    return df

def linearRegression(data):
    data = data.assign(days=data.mid-df.mid[0])
    data = data.assign(days = data.days.apply(lambda x: x.days))
    
# f = forecastTimeSeries(data=df_midtown, time_col='mid', forecast_col='mean_smooth')
# fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(17, 11))
# ax = [ax]

# ax[0].yaxis.set_major_formatter('${x:1.0f}')

# _handles1 = []

# line1, = ax[0].plot(df_midtown.mid, df_midtown.mean_smooth, linewidth=2)
# #ax[0].fill_between(df_submarket.mid, df_submarket.mean_true_lower_smooth, df_submarket.mean_true_upper_smooth, alpha=0.125)
# line2, = ax[0].plot(f.mid, f['mean'], linewidth=2)

# plt.plot(df_midtown['mid'],df_midtown['mean'])
# plt.plot(f['mid'], f['mean'])

