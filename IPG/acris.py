# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 10:57:56 2021

@author: kpatel
"""

#!/usr/bin/env python

import pandas as pd
import numpy as np
from datetime import datetime

DEED_TYPES = {'DEED, LE',
              'DEED, TS',
              'DEEDP',
              'DEED',
              'DEEDO',
              'CONDEED',
              'DEED, RC',
              'DEED COR',
              'ASSTO'}

def has_deed(x):
    intersect = set(x.doc_type).intersection(DEED_TYPES)
    if len(intersect)>0:
        return True
    else:
        return False

def get_deeds(x):
    if has_deed(x):
        return x[x.doc_type.isin(DEED_TYPES)]
    else:
        return None

def get_deeds_gt_zero_transfer(x):
    if has_deed(x):
        deeds = get_deeds(x)
        deeds = deeds[(deeds.pct_transferred>0)]
        deeds = deeds.sort_values('year_recorded', ascending=False)
        return deeds
    else:
        return None
    
def get_deed_most_recent(x):
    if has_deed(x):
        deeds = get_deeds(x)
        YEAR_MOST_RECENT = deeds.year_recorded.max()
        return deeds[deeds.year_recorded==YEAR_MOST_RECENT]
    else:
        return None

def get_doc_most_recent(x):
    YEAR_MOST_RECENT = x.year_recorded.max()
    return x[x.year_recorded==YEAR_MOST_RECENT]

def format_address(x):
    if type(x)==str:
        address_items = [item.strip() for item in x.split(' ') if len(item)>0]
        address = ' '.join(address_items)
        address = address.lower().upper()
        return address
    else:
        return x

def get_docs_gt_zero_value(x):
    pass

PATH = 'C:\\Users\\kpatel\\data\\dump\\acris'
filename = "ACRIS_-_Real_Property_Master.csv"
master = pd.read_csv('{}\\{}'.format(PATH,filename))

cols = {'DOCUMENT ID': 'doc_id',
        'RECORD TYPE': 'record_type',
        'CRFN': 'cfrn',
        'BOROUGH': 'borough_recorded',
        'DOC. TYPE': 'doc_type',
        'DOC. DATE': 'doc_date',
        'DOC. AMOUNT': 'doc_amount',
        'RECORDED / FILED': 'date_recorded',
        'MODIFIED DATE': 'date_modified',
        'REEL YEAR': 'reel_year',
        'REEL NBR': 'reel_nbr',
        'REEL PAGE': 'reel_page',
        '% TRANSFERRED': 'pct_transferred',
        'GOOD THROUGH DATE': 'date_good_through'}

master = master.rename(columns=cols)
master = master.assign(year_doc=master.doc_date.apply(lambda x: int(x[-4:]) if type(x)==str else None))
master.year_doc = master.year_doc.convert_dtypes()

master = master.assign(year_recorded=master.date_recorded.apply(lambda x: int(x[-4:]) if type(x)==str else None))
master.year_recorded = master.year_recorded.convert_dtypes()

filename = 'ACRIS_Real_Property_Legals_Manhattan.csv'
legal = pd.read_csv('{}\\{}'.format(PATH,filename))

cols = {'DOCUMENT ID': 'doc_id',
        'RECORD TYPE': 'record_type',
        'BOROUGH': 'borough',
        'BLOCK': 'block',
        'LOT': 'lot',
        'EASEMENT': 'easement',
        'PARTIAL LOT': 'partial_lot',
        'AIR RIGHTS': 'air_rights',
        'SUBTERRANEAN RIGHTS': 'sub_rights',
        'PROPERTY TYPE': 'property_type',
        'STREET NUMBER': 'street_number',
        'STREET NAME': 'street_name',
        'UNIT': 'unit',
        'GOOD THROUGH DATE': 'date_good_through'}

legal = legal.rename(columns=cols)

# left join because we want to pull in master data for all legal records in Manhattan
ml = legal.merge(master, how='left', on='doc_id', suffixes=["_legal", "_master"])
ml = ml.assign(street_number=ml.street_number.convert_dtypes())
ml.street_number = ml.street_number.apply(lambda x: x.strip() if type(x)==str else x)

ml.street_name = ml.street_name.convert_dtypes()
ml.street_name = ml.street_name.apply(lambda x: x.lower().strip() if type(x)==str else x)

def format_street_name(x):
    if type(x)==str:
        x = x.lower().strip()
        x = x.replace('.', '')
        tokens = x.split(' ')
        tokens = [t.strip().upper() for t in tokens if len(t)>0]
        return ' '.join(tokens)    
    else:
        return x

def format_street_number(x):
    if '-' in x:
        items = x.split('-')
        items = [i.strip() for i in items]
        
ml.street_name = ml.street_name.apply(format_street_name)
    
ml = ml.assign(borough=ml.borough.astype(str))
ml = ml.assign(block=ml.block.astype(str).apply(lambda x: x.zfill(5)))
ml = ml.assign(lot=ml.lot.astype(str).apply(lambda x: x.zfill(4)))
ml = ml.assign(bbl=ml.borough+ml.block+ml.lot)

acris_codes = pd.read_csv('{}//acris_document_control_codes.csv'.format(PATH))
acris_codes = acris_codes[['doc_type', 'description']]
ml = ml.merge(acris_codes, on='doc_type', how='left')

ml = ml.drop(columns=['record_type_legal',
                      'doc_id',
                      'easement',
                      'partial_lot',
                      'cfrn',
                      'date_good_through_legal',
                      'record_type_master',
                      'borough_recorded',
                      'date_modified',
                      'reel_year',
                      'reel_nbr',
                      'reel_page',
                      'date_good_through_master'])


#ml = ml.assign(address=ml.street_number + " " + ml.street_name)
PATH = "C://Users//kpatel//data//dump//acris//output//"
bl = pd.read_csv('{}//buildings_gt_100kSF_bbl_manhattan_shapiro.csv'.format(PATH))
#bl = bl.assign(borough=bl.borough.astype(str))
#bl = bl.assign(block=bl.block.astype(str).apply(lambda x: x.zfill(5)))
#bl = bl.assign(lot=bl.lot.astype(str).apply(lambda x: x.zfill(4)))
#bl = bl.assign(bbl=bl.borough+bl.block+bl.lot)

m=bl.merge(ml, how='left', on='bbl', suffixes=['', '_legal'])
m = m.assign(condo_billing_lot=m.lot.apply(lambda x: True if x>7500 and x<7510 else False))
mg = m.groupby('property_address')

d = []
for prop in mg.groups.keys():
    grp = mg.get_group(prop)
    if not has_deed(grp):
        d.append(grp.property_address.iloc[0])

ml_office=ml[ml.property_type=='OF']
ml_office_grp = ml_office.groupby('bbl')

d = pd.DataFrame()
for bbl in ml_office_grp.groups.keys():
    grp = ml_office_grp.get_group(bbl)
    if not has_deed(grp):
        d = d.append(grp)
        
d = pd.DataFrame()
for bbl in mg.groups.keys():
    grp = mg.get_group(bbl)
    if has_deed(grp)==False:
        doc = get_doc_most_recent(grp)
        d = d.append(doc)
    else:
        deeds = get_deeds_gt_zero_transfer(grp)
        d = d.append(deeds)
        
doc_type_mapper={'AALR': None,
 'ACON': None,
 'AGMT': None,
 'AL&R': None,
 'AMTX': None,
 'ASPM': None,
 'ASST': None,
 'ASSTO': None,
 'ASTU': None,
 'CALR': None,
 'CDEC': None,
 'CERR': None,
 'CERT': None,
 'CMTG': None,
 'CNTR': None,
 'CODP': None,
 'CONDEED': None,
 'CONS': None,
 'CORR': None,
 'CORRD': None,
 'CORRM': None,
 'CTOR': None,
 'DECL': None,
 'DEED': None,
 'DEED COR': None,
 'DEED, RC': None,
 'DEEDO': None,
 'DEVR': None,
 'DTL': None,
 'EASE': None,
 'LDMK': None,
 'LEAS': None,
 'LIC': None,
 'LTPA': None,
 'M&CON': None,
 'MAPS': None,
 'MCON': None,
 'MERG': None,
 'MISC': None,
 'MLEA': None,
 'MTGE': None,
 'PAT': None,
 'PREL': None,
 'PSAT': None,
 'REL': None,
 'RPTT': 'Real Property Transfer Tax',
 'RPTT&RET': None,
 'RTXL': None,
 'SAGE': None,
 'SAT': None,
 'SMIS': None,
 'SMTG': None,
 'SPRD': None,
 'SUBL': None,
 'SUBM': None,
 'TERA': None,
 'TERL': None,
 'TERT': None,
 'TL&R': None,
 'TLS': None,
 'VAC': None,
 'WSAT': None,
 'ZONE': None}    
    