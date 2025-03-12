# -*- coding: utf-8 -*-
"""
Created on Mon Jul 12 17:49:58 2021

@author: kpatel
"""

class Preprocessing(object):
    
    def __init__(self, preprocessor):
        self.preprocessor = preprocessor
    
    def get(self, preprocessor):
        return self.type_to_function(preprocessor)
    
    def type_to_function(self, preprocessor):
        switcher = {
            'newmark': self.dummy,
            'cbre': self.dummy,
            'construction_pipeline': self.dummy}
        
        func = switcher.get(preprocessor, lambda: "Invalid Preprocessor")
        
        return func()
    
    def dummy(self):
        pass