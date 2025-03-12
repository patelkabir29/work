# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 16:33:58 2021

@author: kpatel
"""
from Monday import Board
import Format
from datetime import datetime
import xlsxwriter
import yaml

def add_to_format(existing_format, dict_of_properties, workbook):
    """Give a format you want to extend and a dict of the properties you want to
    extend it with, and you get them returned in a single format"""
    new_dict={}
    for key, value in existing_format.__dict__.items():
        if (value != 0) and (value != {}) and (value != None):
            new_dict[key]=value
    del new_dict['escapes']
    
    new_dict.update(dict_of_properties)
    return workbook.add_format(new_dict)

class AnalyticsTactical(object):
    def __init__(self):
        self.HP_BOARD_ID = 1094383992
        self.board = Board(self.HP_BOARD_ID)
        self.today = datetime.today()
        
        self._setup_worksheet()
        self._set_format_objects()
                
    def _setup_worksheet(self):
        # Create a workbook and add a worksheet.       
        workbook = xlsxwriter.Workbook('A&R_Tactical_{}.xlsx'.format(self.today.strftime('%m-%d-%Y')))
        worksheet = workbook.add_worksheet('Tactical Plan')
        
        # set paper to tabloid (11x17)
        worksheet.set_paper(3)
        worksheet.set_landscape()
        worksheet.set_margins(left=0.25, right=0.25, top=0.75, bottom=0.75)
        
        # turn off gridlines in view and print mode
        worksheet.hide_gridlines(2)
        self.workbook = workbook
        self.worksheet = worksheet                
    
    def _set_format_objects(self):
        
        deal_title = self.workbook.add_format({'font_name': 'Arial',
                                               'font_size': 14,
                                               'bold': True,
                                               'bottom': 6})        

        standard = self.workbook.add_format({'font_name': 'Arial',
                                             'font_size': 8,
                                             'valign': 'top'})

        standard_wrap = add_to_format(standard, {'text_wrap': True}, self.workbook)
        
        hp_date = add_to_format(standard, {'bold': True,
                                           'bottom': 6,
                                           'align': 'right',
                                           'valign': 'bottom'}, self.workbook)
        
        item = add_to_format(standard_wrap, {'bottom': 1}, self.workbook)
        item_num = add_to_format(item, {'align': 'center'}, self.workbook)        
        
        item_bg_gray = add_to_format(item, {'bg_color': '#DBDBDB'}, self.workbook)
        item_num_bg_gray = add_to_format(item_num, {'bg_color': '#DBDBDB'}, self.workbook)
        
        item_bg_red = add_to_format(item, {'bg_color': '#F8CBAD'}, self.workbook)
        item_num_bg_red = add_to_format(item_num, {'bg_color': '#F8CBAD'}, self.workbook)
        
        col_header = add_to_format(standard_wrap, {'bold': True,'bottom': 2}, self.workbook)

        col_header_cntr = add_to_format(col_header, {'align': 'center'}, self.workbook)

        date_fmt = add_to_format(item, {'num_format': 'dd/mm/yy'}, self.workbook)
        
class MarketView(object):
    def __init__(self):
        
        