# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 11:10:23 2022

@author: kpatel
"""
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from statsmodels.stats.weightstats import DescrStatsW
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from scipy.ndimage.filters import gaussian_filter1d

class TenantHistory(object):
    
    def __init__(self, leases, sigma=2):
        
        self.tenant_name = leases.tenant_name.iloc[0]
        self.sigma = sigma
        
        leases = leases.reset_index(drop=True)
        
        # form a date_range column using commencement and expiration dates with a daily frequency
        #leases = leases.assign(occ_range=leases.apply(lambda x: pd.date_range(x['commencement_date'], x['expiration_date'], freq='D'), axis=1))
        leases = leases.assign(occ_range=leases.apply(lambda x: pd.date_range(x['commencement_date'], x['expiration_date'], freq='D') if (type(x['commencement_date'])==date)&(type(x['expiration_date'])==date) else None, axis=1))
        
        for col in leases.columns:
            if 'date' in col:
                leases = leases.assign(**{col: pd.to_datetime(leases[col]).apply(lambda x: x.date())})
                leases = leases.assign(**{col.split('_')[0]+'_year': leases[col].apply(lambda x: x.year)})
                self.leases = leases
        
        self.timeseries = self.get_timeseries()
        
    def get_timeseries(self):
        
        COMMENCEMENT_EARLIEST = self.leases.commencement_date.dropna().min()
        EXPIRATION_LATEST = self.leases.expiration_date.dropna().max()
                        
        t = {'date': [], 'footprint': [], 'net_effective_rent': [], 'sf_per_location': []}
        
        for date_index in pd.date_range(COMMENCEMENT_EARLIEST, EXPIRATION_LATEST, freq='M'):
            date_v = date_index.date()
            l = self.leases.query('@date_v>execution_date & @date_v<expiration_date')
                        
            t['date'].append(date_v)
            t['footprint'].append(l.transaction_sqft.sum())
            t['net_effective_rent'].append(DescrStatsW(l['net_effective_rent'], l['transaction_sqft']).mean)
            t['sf_per_location'].append(l.transaction_sqft.sum()/len(set(l.street_address)))

        t = pd.DataFrame(t)
        t = t.assign(**{'footprint_smooth': gaussian_filter1d(t['footprint'], sigma=self.sigma)})
        t = t.assign(**{'ner_smooth': gaussian_filter1d(t['net_effective_rent'], sigma=self.sigma)})
        t = t.assign(**{'sf_per_location_smooth': gaussian_filter1d(t['sf_per_location'], sigma=self.sigma)})
        t = t.set_index('date')
        
        return t
        
    def plot_timeseries(self):
        
        fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(17, 11), sharex=True)
        fig.suptitle(self.tenant_name, fontsize=16)
        
        ax[0].set_title('Total Footprint (SF)')
        ax[0].plot(self.timeseries.footprint_smooth)
        ax[0].yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f} SF'))
        ax[0].grid()
        
        ax[1].set_title('Net Effective Rent (weighted)')
        ax[1].plot(self.timeseries.ner_smooth)
        ax[1].yaxis.set_major_formatter('${x:1.0f}')
        ax[1].grid()
        
        ax[2].set_title('Avg. Footprint per Location (projected)')
        ax[2].plot(self.timeseries.sf_per_location_smooth)
        ax[2].yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f} SF'))
        ax[2].grid()        