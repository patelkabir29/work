# -*- coding: utf-8 -*-
"""
Created on Thu Aug 26 11:03:53 2021

@author: kpatel
"""

class Asset(object):
    
    def __init__(self, address, city='New York', state_abbr='NY'):
        
        self.address = address
        self.city = city
        self.state = state_abbr
        self.latitude = None
        self.longitude = None
    
class Office(Asset):
    
    def __init__(self, address, city, state_abbr):

        super().__init__(address, city, state_abbr)
        

        
        