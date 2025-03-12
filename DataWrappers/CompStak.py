# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 15:21:29 2021

@author: kpatel
"""

import boto3
import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
import logging

class CompStak(object):

    def __init__(self):
        
        sts_client = boto3.client('sts')
        self.RoleArn = "arn:aws:iam::278696104475:role/external-role-innovo"
        self.RoleSessionName = "compstak"
        
        logging.info('Establishing connection to: {}'.format(self.RoleArn))
        
        assumed_role_object=sts_client.assume_role(
            RoleArn=self.RoleArn,
            RoleSessionName=self.RoleSessionName
        )
        
        credentials=assumed_role_object['Credentials']

        s3=boto3.resource(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )

        self.bucket = s3.Bucket(name='compstak')
        
        self.data = {'leases': None,
                     'properties': None,
                     'sales': None}

        self.boroughs = {'Manhattan': ['Chelsea',
                                'City Hall Insurance',
                                'Columbus Circle',
                                'Financial District',
                                'Gramercy Park/Union Square',
                                'Grand Central',
                                'Hudson Square',
                                'Hudson Yards',
                                'Madison/Fifth Avenue',
                                'Midtown Eastside',
                                'Murray Hill',
                                'NoHo Greenwich Village',
                                'North Manhattan',
                                'Park Avenue',
                                'Penn Station',
                                'Sixth Avenue',
                                'SoHo',
                                'Times Square',
                                'Times Square South',
                                'Tribeca',
                                'UN Plaza',
                                'Upper Eastside',
                                'Upper Westside',
                                'World Trade Center'],
                         'Bronx': ['Bronx'],
                         'Brooklyn': ['Brooklyn'],
                         'Queens': ['Queens'],
                         'Staten Island': ['Staten Island'],
                         'New York': ['New York'],
                         'nan': ['nan']}

        self.today = date.today()
        self._get_counts()
        self.get_leases()
        self.get_sales()
        self.get_properties()
        
    def dump(self):                
        today = date.today()
        date_str = '{:02d}{:02d}{}'.format(today.month, today.day, today.year)
       
        for key, data in self.data.items():
            filename = "C://Users//kpatel//data//dump//compstak//feed//{}_{}.csv".format(key, date_str)
            data.to_csv(filename, index=False)
            
            
    def list_objects(self):
        for key in self.data.keys():
            objs = self.bucket.objects.filter(Prefix='datadumps/innovo-{}/'.format(key))
            for obj in objs:
                print(obj.key)
        
    def _get_counts(self):
        
        self.counts = {}
        
        objs = self._most_recent_objs()
        
        for comp_type, obj in objs.items():
            #print('Fetching {} data via CompStak Cloud Connection'.format(comp_type))
            self.counts[comp_type] = pd.read_csv(obj.get()['Body'], usecols=['Property Type'], dtype=str)
            #print('Connection successful.')

    def _most_recent_obj(self, comp_type, offset=0):
        
        get_last_modified = lambda obj: obj.last_modified
        
        objs = self.bucket.objects.filter(Prefix='datadumps/innovo-{}/'.format(comp_type))
        obj_last_added = [obj for obj in sorted(objs, key=get_last_modified, reverse=True)][offset]
        return obj_last_added
    
    # get most recent data objects for all comp types
    def _most_recent_objs(self, offset=0):
        
        objs = {'leases': None,
                'sales': None,
                'properties': None}

        for key in objs.keys():
            objs[key] = self._most_recent_obj(key, offset)
        
        return objs
    
    # get most recent data sets
    def get_data(self, comp_type, columns_list=None, offset=0):
        
        obj = self._most_recent_obj(comp_type, offset)
        s = str(obj.key).strip('.csv')
        logging.info('Latest dataset: {}'.format(s))
        
        if columns_list is None:
            d = pd.read_csv(obj.get()['Body'], low_memory=False)
        else:
            d = pd.read_csv(obj.get()['Body'], usecols=columns_list, low_memory=False)
        
        # Preprocessing
    
        # convert date strings to datetime objects
        cols_date = [col for col in d.columns if 'Date' in col]
        
        for col in cols_date:
            d = d.assign(**{col: pd.to_datetime(d[col]).dt.date})
        
        col_mapping = {col: col.lower().replace(' ','_').replace('(s)','s').replace('(','').replace(')','').replace('/','').replace('__','_') for col in d.columns}
        d = d.rename(columns=col_mapping)
        
        # building_class has nans, replace with string of 'None'
        d = d.assign(**{'building_class': d.building_class.fillna('None')})
        
        # tag active leases
        if comp_type=='leases':
            today = date.today()
            d = d.assign(active=d[['commencement_date', 'expiration_date']].apply(lambda x: (x['commencement_date'] <= today)&(x['expiration_date'] >= today), axis=1))
            d = d.assign(execution_year=d.execution_date.apply(lambda x: x.year))
            d = d.assign(commencement_year=d.commencement_date.apply(lambda x: x.year))
            d = d.assign(expiration_year=d.expiration_date.apply(lambda x: x.year))            
            d = d.assign(transaction_quarter = d.transaction_quarter.apply(lambda x: x[4:]))
                         
        self.data[comp_type] = d

        # split geo_point field from string of lat lon to individual columns for lat lon
        self.data[comp_type][['latitude','longitude']]=self.data[comp_type].geo_point.str.split(' ',expand=True).astype(float)
        
        return self.data
            
    def get_leases(self, usecols=None, offset=0):
        
        self.get_data('leases', usecols, offset)
        
        try:
            self.data['leases'].submarket = self.data['leases'].submarket.replace(np.nan,'nan')
            borough = self.data['leases'].submarket.apply(lambda x: [key for key in self.boroughs.keys() if x in self.boroughs[key]])
            borough = [x.pop() for x in borough]
            self.data['leases'] = self.data['leases'].assign(borough=borough)
        except:
            pass
        
        #self.data['leases'] = self.data['leases'].assign()        
        return self.data['leases']
    
        self.get_data('sales', usecols, offset)
    
    def get_sales(self, usecols=None, offset=0):
        self.get_data('sales', usecols, offset=0)
        
        #borough = self.data['sales'].submarket.apply(lambda x: [key for key in self.boroughs.keys() if x in self.boroughs[key]])
        #borough = [x.pop() for x in borough]
        #self.data['sales'] = self.data['sales'].assign(borough=borough)
        
        return self.data['sales']
        
    def get_properties(self, usecols=None, offset=0):
        self.get_data('properties', usecols, offset)
        return self.data['properties']
    
    def get_all_data(self, offset=0):
        self.get_leases(offset)
        self.get_sales(offset)
        self.get_properties(offset)
        return self.data
        
    def summarize(self):
        for key, value in self.counts.items():
            print('{}: {}'.format(key.capitalize(), len(value)))
            v = value.value_counts()
            for row in v.iteritems():
                print('  {}: {}'.format(row[0][0], row[1]))
                
    # move this logic outside of the CompStak module, filtering should be done in downstream
    # modules...intent here is to simply connect to CompStak datafeed and get data and preprocess
    def filter_leases(self,
                      submarkets: set={None},
                      property_type: set={'Office','Industrial','Retail','Multi-Use','Other'},
                      space_type: set={'Office','Industrial','Retail','Multi-Use','Other'},
                      building_class: set={'A', 'B', 'C', 'None'},
                      execution: tuple=(date(1900,1,1), date(2100,1,1)),
                      expiration: tuple=(date(1900,1,1), date(2100,1,1)),
                      transaction_types_exclude: set={None},
                      sqft_min: int=0,
                      active: set={True, False}):                  
                        
        d = self.data['leases']
        
        TRANS_TYPES_INCLUDE = set(d.transaction_type) - transaction_types_exclude
        
        qry = 'submarket in @submarkets & '\
              'property_type in @property_type & '\
              'space_type in @property_type & '\
              'building_class in @building_class & '\
              'transaction_type in @TRANS_TYPES_INCLUDE & '\
              'execution_date >= @execution[0] & '\
              'execution_date <= @execution[1] & '\
              'transaction_sqft > @sqft_min & '\
              'active in @active & '\
              'expiration_date >= @expiration[0] & '\
              'expiration_date <= @expiration[1]'
              
        d = d.query(qry)
        
        return d
        
    def get_active_leases(self):
        data = self.data['leases'].query('active==True')
        return data
    
    def get_submarket_leases(self, submarkets):
        l = self.data['leases'].query('submarket in @submarkets')
        return l

    def get_leases_expiring_in_next(self, months=12):
        leases = self.get_active_leases()
        today = date.today()
        one_year_from_now = today + relativedelta(months=months)
        leases = leases[(leases.expiration_date <= one_year_from_now)]
        return leases
        
    def _expiring_sqft(self, months=12):
        sqft = []
        for months in list(range(1,months+1)):
            temp = self._get_leases_expiring_in_next(months).transaction_sqft.sum()
            sqft.append(temp)
        return sqft    

class Comps(object):

    def __init__(self, data):
        self.data = data
        
class Lease(Comps):
    def __init__(self, data):
        super().__init__(data)
        
    def compute_avg_rent(self, property_type, date_start, date_end):
        pass

class OfficeLease(Lease):
    def __init__(self, data):
        super().__init__(self, data)
                    
class Sales(Comps):
    def __init__(self, data):
        super().__init__(data)

class IndustrialSales(Sales):
    def __init__(self, data):
        super().__init__(data)

class Property(object):
    def __init__(self, data):
        self.data = data

