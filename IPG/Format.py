# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 16:58:18 2021

@author: kpatel
"""
import xlsxwriter
    
class Standard(object):
    def __init__(self):                        
        self.props = {'font_name': 'Arial',
                      'font_size': 8,
                      'valign': 'top'}    
        
class StandardWrap(Standard):
    def __init__(self):
        super().__init__()
        self.props.update({'text_wrap': True})
        
class PrimaryHeader(Standard):
    def __init__(self):
        super().__init__()
        self.props.update({'font_size': 14,
                           'bold': True,
                           'bottom': 6,
                           'valign': 'bottom'})

class PrimaryHeaderDate(PrimaryHeader):
    def __init__(self):
        super().__init__()
        self.props.update({'font_size': 6,
                           'align': 'right'})
        
class ColumnHeader(StandardWrap):
    def __init__(self):
        super().__init__()
        self.props.update({'bold': True,
                           'bottom': 2})

class ColumnHeaderCentered(ColumnHeader):
    def __init__(self):
        super().__init__()
        self.props.update({'align': 'center'})
        
class Item(StandardWrap):
    def __init__(self):
        super().__init__()
        self.props.update({'bottom': 1})

class ItemNumeric(Item):
    def __init__(self):
        super().__init__()
        self.props.update({'align': 'center'})
        
class ItemGray(Item):
    def __init__(self):
        super().__init__()
        self.props.update({'bg_color': '#DBDBDB'})

class ItemRed(Item):
    def __init__(self):
        super().__init__()
        self.props.update({'bg_color': '#F8CBAD'})
        
class ItemNumericGray(ItemNumeric):
    def __init__(self):
        super().__init__()
        self.props.update({'bg_color': '#DBDBDB'})
        
class ItemNumericRed(ItemNumeric):
    def __init__(self):
        super().__init__()
        self.props.update({'bg_color': '#F8CBAD'})



        