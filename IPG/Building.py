# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 16:58:53 2021

@author: kpatel
"""

from typing import Dict, List, Tuple

#class Location(object):
#    def __init__(self, number: int, street: str, city: str, state: str, zip: int):
           
class Building(object):
    
    def __init__(self):
        self.address = None
        self.latitude = None
        self.longitude = None
        
        self.type = None
        self.year_to_market = None
        self.age = None
        
        self.service_level = None
        self.service_level_desc = None
        
        self.num_stories = None
        self.num_units = None
                    
        self.rented_summary: List[Unit]
        self.available_summary: List[Unit]
        self.listings: List[Unit]
    
class Unit(object):
    
    def __init__(self):
        self.type = None
        self.avg_gross = None
        self.avg_net = None
        self.avg_sample_size = None
        self.avg_sf = None
        self.avg_gross_psf = None
        self.avg_net_psf = None
        