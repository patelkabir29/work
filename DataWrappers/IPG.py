# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 16:40:15 2021

@author: kpatel
"""
import boto3
import pandas as pd

class BrokerageDataCatalog(object):

    def __init__(self):
        self.BUCKET = 'ipg-analytics-data'
        self.DB = 'brokerage-research'
        self.data = {}
        
        for brokerage in ['newmark', 'cbre']:
            self.data[brokerage] = self._get_data(brokerage)
                        
    def _get_s3_csv(self, bucket, database, brokerage, file):
        filename = '{}/{}/{}'.format(database, brokerage, file)
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=filename)
        data = pd.read_csv(obj['Body'])
     
        return data
    
    def _get_data(self, brokerage):
        data = self._get_s3_csv(bucket=self.BUCKET,
                                database=self.DB,
                                brokerage=brokerage, 
                                file='nyc-supply-demand.csv')
        
        data = data.assign(period = data.period.apply(lambda x: pd.to_datetime(x).date()))
        data.set_index(keys='period', inplace=True)
        data = data.sort_values(by='period', ascending=True)
        
        return data    

    def get_most_recent_qtr(self, brokerage):
        data = self.data[brokerage]
        return data.period.max()
        
    def get_most_recent_data(self, brokerage):
        data = self.data[brokerage]
        qtr = self.get_most_recent_qtr(brokerage)
        data = data[data.period==qtr]        
        return data
    
    def get_most_recent_metrics(self, brokerage, market):
        data = self.get_most_recent_data(brokerage)
        data = data[data.market==market]
        return data.squeeze().to_dict()

    def plot_metric(self, brokerage, market, metric):
        data = self.data[brokerage]
        data = data[data.market==market]
        
        market_fmt = market.split(' ')
        market_fmt = [item.capitalize() for item in market_fmt]
        market_fmt = ' '.join(market_fmt)
        
        metric_fmt = metric.split('_')
        metric_fmt = [item.capitalize() for item in metric_fmt]
        metric_fmt = ' '.join(metric_fmt)
        
        data[metric].plot(title='Time-Series: {} {}'.format(market_fmt, metric_fmt),
                                                          grid=True,
                                                          xlabel='Time',
                                                          ylabel=metric_fmt)

    def plot_vacancy(self, brokerage, market):
        data = self.data[brokerage]
        data = data[data.market==market]
        
        market = market.split(' ')
        market = [item.capitalize() for item in market]
        market = ' '.join(market)
            
        (data.vacancy_rate*100).plot(title='Time-Series: {} Vacancy Rate'.format(market),
                                                          grid=True,
                                                          xlabel='Time',
                                                          ylabel='Vacancy Rate (%)')        
    def get_submarkets(self, brokerage):
        return set(self.data[brokerage].market)