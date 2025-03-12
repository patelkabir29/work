# -*- coding: utf-8 -*-
"""
Created on Tue Feb  1 11:07:57 2022

@author: kpatel
"""

from datetime import date

def payment(balance: float, period: (date,date), term_end: date, ir: float) -> float:    
    
    c = ir/12
    months_remaining = 12*(term_end - period[0]).days/365
    factor = c*pow(1+c, months_remaining)/(pow(1+c, months_remaining)-1)
    payment = factor*balance
    amt_interest = balance*(ir*(period[1] - period[0]).days/365)
    amt_principal = payment - amt_interest
    
    return {'payment': payment,
            'interest': amt_interest,
            'principal': amt_principal}