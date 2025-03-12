# -*- coding: utf-8 -*-
"""
Created on Tue May 18 15:08:39 2021

@author: kpatel
"""

import pandas as pd
import Levenshtein as leven

def extract_name(x):
    indx1 = x.find('ORIG PARTY NAME:') + 16
    indx2 = x.find('REF FOR BEN')
    name = x[indx1:indx2].strip().strip('+').strip().strip("1/").strip('llc')
    name = name.replace('+', ' ').replace('#', ' ').replace('or', '').replace('revocable trust', '').replace('living trust', '')
    name = name.lower()
    return name
    
PATH="C://Users//kpatel//analytics-research//crowdstreet"

# filename = 'offers_052421.csv'
# offers_fullfile = '{}//{}'.format(PATH, filename)
investments_fullfile = '{}//{}'.format(PATH, 'offers_052521.csv')
account_fullfile = '{}//{}'.format(PATH,'account_history_052521.csv')

# data = pd.read_csv(offers_fullfile)
# data = data[[col for col in data.columns if 'Unnamed' not in col]]
# data = data.rename(columns={col: col.strip().replace(' ', '_').lower() for col in data.columns})
# offers = data
# offers.investing_entity = offers.investing_entity.astype(str).apply(lambda x: x.lower())

data = pd.read_csv(account_fullfile)
data = data[[col for col in data.columns if 'Unnamed' not in col]]
data = data.rename(columns={col: col.strip().replace(' ', '_').lower() for col in data.columns})
account = data
account = account.assign(name_extracted = account.description.apply(extract_name))
account = account.assign(account_history_row_id=list(account.index))

data = pd.read_csv(investments_fullfile)
data = data[[col for col in data.columns if 'Unnamed' not in col]]
data = data.rename(columns={col: col.strip().replace(' ', '_').lower() for col in data.columns})
investments = data
investments.investing_entity = investments.investing_entity.astype(str).apply(lambda x: x.lower())
investments = investments.assign(offers_row_id=list(investments.index))

output = pd.DataFrame()
counter=0
for index, credit in account.iterrows():
    ratios = investments.investing_entity.apply(lambda x: leven.ratio(x, credit.name_extracted))
    ratio_max = ratios.max()
    ratio_argmax = ratios.argmax()
    
    if ratio_max >= 0:
        investor_match = investments.iloc[ratio_argmax].to_dict()
        investor_match['ratio'] = ratio_max
        c = credit.to_dict()
        investor_match.update(c)
        output = output.append(pd.DataFrame(investor_match, index=[counter]))
        counter=counter+1
                
cols = ['offers_row_id',
        'account_history_row_id',
        'name_extracted',
        'investing_entity',
        'submitted_($)',
        'credit',
        'ratio',
        'post_date',
        'description']two 

output = output.sort_values(by='ratio',ascending=False)
output = output[cols]

output = output.rename(columns={'investing_entity': 'crowdstreet_investing_entity',
                                'name_extracted': 'bank_name_extracted',
                                'submitted_($)': 'crowdstreet_offer'})

output = output.assign(credit_matches_offer=output.crowdstreet_offer==output.credit)

output.to_csv('{}//{}'.format(PATH, 'output_052521.csv'), index=False)