# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 14:24:30 2022

@author: kpatel
"""

import pandas as pd

class Properties(object):
    
    def __init__(self):
        PATH = "C:\\Users\\kpatel\\analytics-research\\scripts\\special_projects\\industrial\\data"
        filename = "Industrial Property List.xlsx"

        data = pd.DataFrame()

        for sn in ['BX', 'QN', 'BK']:
            temp = pd.read_excel('{}//{}'.format(PATH, filename), sheet_name=sn)
            data = data.append(temp)
        
        self.data = data

class Tenants(object):
    
    def __init__(self):
        pass
    
class Metrics(object):
    
    def __init__(self):
        PATH = "C:\\Users\\kpatel\\analytics-research\\scripts\\special_projects\\industrial\\data"
        filename = "NYC Outer Boroughs Market Metrics.xlsx"
        
class Supply(object):

    def __init__(self):
        pass

class Macroeconomics(object):

    def __init__(self):
        pass