# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:46:58 2021

@author: kpatel
"""

import string
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
from datetime import timedelta
import matplotlib.pyplot as plt
import numpy as np
import string
import csv
import math
import json
from opencage.geocoder import OpenCageGeocode
import Levenshtein as leven

key = 'cc9c107098984010a51f9bcab74c80fd'
geocoder = OpenCageGeocode(key)

class Tenant(object):
    def __init__(self, tenant_name, **kwargs):
        self.tenant = tenant_name
        self.tenant_simple = self.tenant.lower().translate(str.maketrans('','',string.punctuation))
        self.__dict__.update(**kwargs)

class Building(object):
    def __init__(self, number, street, city='Manhattan', state='NY', zipcode=None, **kwargs):
        self.key = 'cc9c107098984010a51f9bcab74c80fd'
        
        self.number = number
        self.street = street
        self.city = city
        self.state = state
        self.zipcode = zipcode
        self.__dict__.update(**kwargs)
        
    def get_address(self):
        return '{} {}, {}, {}'.format(self.number, self.street, self.city, self.state)
        
    def get_lat_lon(self):
        self.geocoder = OpenCageGeocode(key)
        query = u'{}'.format(self.get_address())
        results = geocoder.geocode(query)
        addr = [r.get('formatted') for r in results]
        [leven.ratio(self.get_address(), idx) for idx in indx]
        
        try:
            addr = [r.get('formatted') for r in results]
            l = [leven.ratio(self.get_address(), item) for item in addr]         
            self.address_formatted = results[np.argmax(l)]['formatted']
            self.latitude = results[indx]['geometry']['lat']
            self.longitude = results[indx]['geometry']['lng']
        except:
            self.latitude = None
            self.longitude = None
                                    
class BaseDeal(object):
    def __init__(self, tenant, number, street, city, state, rent_base_psf, sf, sign_date, **kwargs):
        self.tenant = Tenant(tenant)
        self.building = Building(number=number, street=street, city=city, state=state)        
        self.rent_base_psf = rent_base_psf
        self.sf = sf
        self.sign_date = sign_date
        self.__dict__.update(**kwargs)

class BaseDealNYC(BaseDeal):
    def __init__(self, tenant, number, street, rent_base_psf, sf, sign_date, **kwargs):
        super().__init__(tenant=tenant, 
                        number=number,
                        street=street, 
                        city='New York',
                        state='NY',
                        rent_base_psf=rent_base_psf,
                        sf=sf,
                        sign_date=sign_date,
                        **kwargs)
            
class RentSchedule(object):
    def __init__(self, term_year: list, rent: list, date_sign: date=None, date_commence:date=None):
        self.schedule = pd.DataFrame({'term_year': term_year,
                                      'rent': rent})
            
class LeaseComp(BaseDealNYC):
    def __init__(self, number: str, street: str, tenant:str, sign_date: date, rent_base_psf: float, sf: int, **kwargs) -> None:
        super().__init__(tenant=tenant,
                         number=number, 
                         street=street, 
                         rent_base_psf=rent_base_psf,
                         sf=sf,
                         sign_date=sign_date,
                         **kwargs)
        
        self.rent_schedule = RentSchedule(term_year=[2020,2021,2022,2023,2024], rent=[100,110,120,130,140])
        self.rent_escalation = None
        self.term_months = None
        self.rent_free_months = None
        self.ti_psf = None
        self.rent_net_effective = None        
        
        # add kwargs fields
        self.__dict__.update(**kwargs)
        
    def _compute_ner(self):
        if self.rent_net_effective is not None: # we already have it, done
            pass
        elif (len(self.rent_schedule.keys()) > 0 and (self.term_months and self.rent_free_months and self.sf)):
            pass
                                                
class Newmark(object):
    def __init__(self, PATH, filename):
        self.PATH = PATH
        self.filename = filename
        self.workbook = pd.ExcelFile("{}//{}".format(self.PATH, self.filename))
        self.sheet_names = self.workbook.sheet_names
                            
    def process(self):
        self.data = pd.DataFrame()

        for sheet in self.sheet_names:
            temp = self.workbook.parse(sheet)
            temp = temp[[col for col in temp.columns if 'Unnamed' not in col]]
            temp = temp.rename(columns={col: col.strip().replace(' ', '_').lower() for col in temp.columns})
            temp.dropna(how='all', axis=1, inplace=True)
            self.data = self.data.append(temp)
            
        self._columns = {'floor(s)': 'floors',
                         'terms_(months)': 'term_months',
                         'rent_($/sqft)': 'rent_psf',
                         'base_rent_($/sqft)': 'base_rent_psf',
                         'work_amount_($/sqft)': 'work_amount_psf',
                         'free_rent_(months)': 'rent_free_months',
                         'net_effective_rent_($/sqft)': 'rent_net_effective_psf',
                         'class': 'class_building'}
            
        self.data = self.data.rename(columns=self._columns)

        self.data = self.data.assign(industry =\
                                     self.data.industry.apply\
                                         (lambda x: x.replace(' - ','_')\
                                                     .replace(' -','_')\
                                                     .replace(' ','_')\
                                                     .replace('/','')\
                                                     .replace('&','and')\
                                                     .strip('_').lower()\
                                                     if type(x)==str else x))
            
        industry_mapping =\
            {'aerospace_and_defense': 'TAMI',
             'security_products_and_services': 'TAMI',
             'education': 'TAMI',
             'metals_and_mining': 'OTHER',
             'internet_new_media': 'TAMI',
             'business_services_advertising,_marketing,_pr': 'TAMI',
             'business_services_other': 'OTHER',
             'automotive': 'OTHER',
             'cultural_institutions': 'OTHER',
             'health_care': 'FIRE',
             'electronics': 'TAMI',
             'environmental_services_and_equipment': 'FIRE',
             'consumer_services': 'OTHER',
             'energy_and_utilities': 'FIRE',
             'telecommunications': 'TAMI',
             'membership_organizations': 'FIRE',
             'pharmaceuticals': 'FIRE',
             'financial_services': 'FIRE',
             'transportation_services': 'OTHER',
             'business_services_consulting': 'FIRE',
             'food_and_beverages': 'OTHER',
             'retail': 'OTHER',
             'government': 'FIRE',
             'business_services_legal_services': 'FIRE',
             'real_estate_coworking': 'FIRE',
             'real_estate': 'FIRE',
             'business_services_accounting': 'FIRE',
             'computer_hardware': 'TAMI',
             'business_services_staffing': 'FIRE',
             'construction': 'FIRE',
             'consumer_products_manufacturers_apparel': 'OTHER',
             'insurance': 'FIRE',
             'industrial_manufacturing': 'OTHER',
             'computer_software': 'TAMI',
             'publishing': 'FIRE',
             'consumer_products_manufactuers_apparel': 'OTHER',
             'architecture_engineering_design': 'FIRE',
             'media': 'TAMI',
             'consumer_products_manufacturers': 'OTHER',
             'leisure': 'OTHER',
             'information': 'TAMI',
             'social_services': 'FIRE',
             'computer_services': 'TAMI',
             'charitable_organizations_not-for-profit': 'FIRE',
             'chemicals': 'OTHER',
             np.nan: 'OTHER'}
            
        self.data = self.data.assign(tenant_type=self.data.industry.map(industry_mapping))
        
        self.data = self.data.assign(transaction_type=self.data.transaction_type.apply(lambda x: x.strip(' ') if isinstance(x,str) else x))
        self.data = self.data.assign(transaction_type=self.data.transaction_type.fillna('Unknown'))
        
        self.data = self.data.assign(lease_type=self.data.lease_type.apply(lambda x: x.strip(' ') if isinstance(x,str) else x))
        self.data = self.data.assign(lease_type=self.data.lease_type.fillna('Unknown'))
        
        self.data = self.data[self.data.term_months.apply(lambda x: isinstance(x, (int,float) or x=='Confidential'))]
        
        self.data.rent_net_effective_psf = self.data.rent_net_effective_psf.apply(lambda x: np.nan if isinstance(x,str) else x)
        self.data = self.data.replace('Confidential', np.nan)
                
        #reset index
        self.data = self.data.reset_index()
        
        # extract rent schedule by year from semi-structured rent_psf column of text
        self._create_rent_schedule()
        
    # extract rent schedule by year from semi-structured rent_psf column of text
    def _extract_rent_schedule(self, x):
        try:
            x = x.lower().strip('avg').replace(';',':').replace(':','').replace('$',': $').replace(' :',':').strip()
            if isinstance(x,str):
                rent_table = pd.DataFrame()
                if "\n" in x:
                    y = x.split('\n')
                else:
                    y = [x]
                    
                for item in y:
                    temp = item.split(':')
                    if len(temp)>1:
                        years = [int(float(y)) for y in temp[0].strip('yrs ').split('-')]
                        rent = float(temp[1].strip().strip('$'))
                        if len(years)==2:
                            rt = pd.DataFrame({'year': list(range(years[0],years[1]+1)),'rent': rent})
                        elif len(years)==1:
                            rt = pd.DataFrame({'year': years, 'rent': [rent]})
                        
                        rent_table = rent_table.append(rt)
                    
                return rent_table
        except:
            return pd.DataFrame({'year':[1], 'rent':[np.nan]})
        
    # create rent schedule for all comps
    def _create_rent_schedule(self):
        self.data = self.data.assign(transaction_id=self.data.index)
        rents = self.data.rent_psf.apply(self._extract_rent_schedule)
        
        self.rent_schedule = pd.DataFrame()
        for index in rents.index:
            try:
                rents[index] = rents[index].groupby('year').mean()
                rents[index] = rents[index].assign(transaction_id=index)
                self.rent_schedule = self.rent_schedule.append(rents[index])
            except:
                pass
            
        self.rent_schedule = self.rent_schedule.reset_index()
    
# =============================================================================
#     def _process_work_amount(self, x):
#         if isinstance(x,str):
#             if "\n" in x:
#                 y = x.split("\n")
#                 for item in y:
#                     if ":" in item:
#                         z = item.split(": $")
#                         print(float(z[1]))
# =============================================================================
        
# data.work_amount_psf = data.work_amount_psf.apply(lambda x: 'Unknown' if isinstance(x,str) else x)
# data.work_amount_psf = data.work_amount_psf.replace('Unknown', np.nan)

# data.rent_free_months = data.rent_free_months.apply(lambda x: 'Unknown' if isinstance(x,str) else x)
# data.rent_free_months = data.rent_free_months.replace('Unknown', np.nan)

# #data.sign_date=data.sign_date.astype(str)
# data.sign_date = data.sign_date.apply(lambda x: x.date())
# data.base_rent_psf = data.base_rent_psf.replace('Confidential', np.nan)
# #data = data.drop(columns=['floors', 'rent_psf', 'base_rent_psf'])

# data.district = data.district.replace('Noho/Soho', 'NoHo/SoHo')

# data = data.drop_duplicates(subset=['tenant_name','building_address','sf'])

# data = data.assign(latitude=float(0))
# data = data.assign(longitude=float(0))

# g = pd.read_json('address_to_coord2.csv')
# sdata=pd.merge(data, g, how='left', left_on='building_address', right_on='address')

# data = data.reset_index()
# data=data.assign(street=data.building_address.apply(lambda x: x.split(' ',maxsplit=1)[1].strip()))

# converter =\
#     {'First': '1st',
#      'Second': '2nd',
#      'Third': '3rd',
#      'Fourth': '4th',
#      'Fifth': '5th',
#      'Seventh': '7th',
#      'Eighth': '8th',
#      'Ninth': '9th',
#      'Tenth': '10th',
#      'Eleventh': '11th',
#      'Penn': 'Pennsylvania'}

# data.building_address = data.building_address.apply(lambda x: modify_address(x, converter))

# def modify_address(x, converter):
#     #f = [x.find(c)>0 for c in converter.keys()]
#     #m = {key: value for key,value in converter.items() if x.find(key)>0}
#     m = [(key, value) for key,value in converter.items() if x.find(key)>0]
    
#     if len(m)==1:        
#         return x.replace(m[0][0], m[0][1])
#     else: 
#         return x

# q_bottom = data.rent_net_effective_psf.quantile(0.1)
# q_top = data.rent_net_effective_psf.quantile(0.975)

# #data.to_csv('transactions_outliers_removed.csv', index=False)

# addresses = set(data.building_address)
# data = data.set_index('building_address')

# address_to_coord = {}

# for indx, address in enumerate(addresses):
#     query = u'{}, {}, {}'.format(address, 'Manhattan, New York', 'NY')
#     print('Querying ({}): {}'.format(indx, address))
#     results = geocoder.geocode(query)
#     try:
#         indx=[r.get('components').get('suburb') for r in results].index('Manhattan')
#         lat = results[indx]['geometry']['lat']
#         lng = results[indx]['geometry']['lng']
#         data.at[address, 'latitude'] = lat
#         data.at[address, 'longitude'] = lng
#         address_to_coord[address] = results[indx]
#     except:
#         data.at[address, 'latitude'] = None
#         data.at[address, 'longitude'] = None
#         address_to_coord[address] = None
        
# with open("address_to_coord2.json", "w") as outfile:
#     json.dump(address_to_coord, outfile, indent=4)

# data = data.reset_index()
# data.to_csv('transactions.csv', index=False)
# data.to_parquet('transactions.parquet.gzip', index=False)

# selector = ((data.rent_net_effective_psf > q_bottom)&(data.rent_net_effective_psf < q_top))|(data.rent_net_effective_psf.isna())
# selector = (selector)&(data.class_building=='A')
# data_outlier_removal = data[selector]

# data_outlier_removal.to_csv('transactions_outliers_removed.csv', index=False)

# def net_effective_rent(d, escalation_pct):
#     work_total = d.work_amount_psf*d.sf
#     rent_free_total = d.base_rent_psf*d.sf*d.rent_free_months
    
#     rent_total = 0
#     for m in range(int(d.rent_free_months)+1, int(d.term_months)+1):
#         yr_num = math.floor((m-1)/12)
#         RENT_FACTOR = math.pow((1+escalation_pct), yr_num)
#         rent_in_place = d.base_rent_psf*RENT_FACTOR
#         rent_total = rent_total + rent_in_place*d.sf    
#         print('Year {}, Month {}: {}'.format(yr_num, m, d.base_rent_psf*RENT_FACTOR))
    
#     print('Rent Total: {}'.format(rent_total))
#     print('Work Total: {}'.format(work_total))
#     print('Rent Free Total: {}'.format(rent_free_total))
    
#     total_payments = rent_total - work_total - rent_free_total
#     net_eff = total_payments/d.term_months/d.sf
#     print('Net Eff:{}'.format(net_eff))