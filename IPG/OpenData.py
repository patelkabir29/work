# -*- coding: utf-8 -*-
"""
Created on Thu Jul  8 14:22:06 2021

@author: kpatel
"""

import pandas as pd

class ACRIS(object):
    def __init__(self, records):
        self.records = records.sort_values(by='year_recorded', ascending=False)

        row = self.records.iloc[0]
        self.address = row.property_address
        #self.name = row.property_name
        #self.owner_name = row.owner_name
        #self.true_owner = row.true_owner_name
        #self.recorded_owner = row.recorded_owner_name
        self.borough = row.borough
        self.block = row.block
        self.lot = row.lot
        self.bbl = int(row.bbl)
        self.house_number = row.house_number
        self.street_name = row.street_name
        #self.submarket = row.submarket
        self.rba = row.rba
        #self.total_space = row['total_available_space_(sf)']
        #self.building_class = row.building_class
        #self.building_status = row.building_status
        #self.air_rights = row.air_rights
        #self.sub_rights = row.sub_rights
        #self.condo_billing_lot = row.condo_billing_lot
    
        self.records = self.records[['doc_type',
                                     'doc_amount',
                                     'year_recorded',
                                     'doc_date',
                                     'pct_transferred',
                                     #'description',
                                     'property_type']]
    
        self.DOC_TYPES = {'SAGE': 'SUNDRY AGREEMENT',
                          'RPTT&RET': 'BOTH RPTT AND RETT',
                          'DECL': 'DECLARATION',
                          'LEAS': 'LEASE',
                          'EASE': 'EASEMENT',
                          'LDMK': 'LANDMARK DESIGNATION',
                          'TL&R': 'TERMINATION OF ASSIGN OF L&R',
                          'AGMT': 'AGREEMENT',
                          'ASST': 'ASSIGNMENT, MORTGAGE',
                          'MTGE': 'MORTGAGE',
                          'AL&R': 'ASSIGNMENT OF LEASES AND RENTS',
                          'DEED': 'DEED',
                          'MAPS': 'MAPS',
                          'CODP': 'CONDEMNATION PROCEEDINGS',
                          'PAT': 'POWER OF ATTORNEY',
                          'SMTG': 'SUNDRY MORTGAGE',
                          'RTXL': 'RELEASE OF ESTATE TAX LIEN',
                          'SAT': 'SATISFACTION OF MORTGAGE',
                          'MISC': 'MISCELLANEOUS',
                          'DEEDO': 'DEED, OTHER',
                          'CERT': 'CERTIFICATE',
                          'SMIS': 'SUNDRY MISCELLANEOUS',
                          'ZONE': 'ZONING LOT DESCRIPTION',
                          'RPTT': 'NYC REAL PROPERTY TRANSFER TAX',
                          'MLEA': 'MEMORANDUM OF LEASE',
                          'SUBM': 'SUBORDINATION OF MORTGAGE',
                          'AALR': 'ASGN OF ASGN OF L&R',
                          'REL': 'RELEASE',
                          'SPRD': 'MORTGAGE SPREADER AGREEMENT',
                          'PSAT': 'PARTIAL SATISFACTION',
                          'TERA': 'TERA',
                          'M&CON': 'MORTGAGE AND CONSOLIDATION',
                          'TERL': 'TERMINATION OF LEASE OR MEMO',
                          'CTOR': 'COURT ORDER',
                          'CORRD': 'CORRECTION DEED',
                          'LOCC': 'LIEN OF COMMON CHARGES',
                          'CNTR': 'CONTRACT OF SALE',
                          'CONS': 'CONSENT',
                          'DEVR': 'DEVELOPMENT RIGHTS',
                          'PREL': 'PARTIAL RELEASE OF MORTGAGE',
                          'ASPM': 'ASSUMPTION OF MORTGAGE',
                          'DEED COR': 'DEED COR',
                          'CALR': 'CANCEL/TERM ASGN L&R',
                          'DTL': 'DISCHARGE OF TAX LIEN',
                          'TLS': 'TAX LIEN SALE CERTIFICATE',
                          'ASSTO': 'ASSIGNMENT OF LEASE',
                          'CORR': 'CORRECTION DOC-OFFICE USE ONLY',
                          'CORRM': 'CORRECTION MORTGAGE',
                          'AMTX': 'ADDITIONAL MORTGAGE TAX',
                          'LIC': 'LICENSE',
                          'MCON': 'MEMORANDUM OF CONTRACT',
                          'ADEC': 'AMENDED CONDO DECLARATION',
                          'SUBL': 'SUBORDINATION OF LEASE',
                          'WSAT': 'WITHHELD SATISFACTION',
                          'ATL': 'ASSIGNMENT OF TAX LIEN',
                          'CERR': 'CERTIFICATE OF REDUCTION',
                          'CMTG': 'CMTG',
                          'ASTU': 'UNIT ASSIGNMENT',
                          'NAPP': 'NAPP',
                          'RETT': 'NYS REAL ESTATE TRANSFER TAX',
                          'MERG': 'MERGER',
                          'CDEC': 'CONDO DECLARATION',
                          'AMTL': 'AMENDMENT OF TAX LIEN',
                          'STP': 'STREET PROCEDURE',
                          'TERT': 'TERMINATION OF TRUST',
                          'JUDG': 'JUDGMENT',
                          'RPAT': 'REVOCATION OF POWER OF ATTORNEY',
                          'DEED, RC': 'DEED, RC',
                          'VAC': 'VACATE ORDER',
                          'WILL': 'CERTIFIED COPY OF WILL',
                          'CONDEED': 'CONFIRMATORY DEED',
                          'XXXX': 'APPRT BREAKDWN OFFICE USE ONLY'}
        
        self.DEED_TYPES = {'DEED, LE',
                           'DEED, TS',
                           'DEEDP',
                           'DEED',
                           'DEEDO',
                           'CONDEED',
                           'DEED, RC',
                           'DEED COR'}
        
        self.TRANSFER_TAX_TYPES = {'RPTT', 'RETT', 'RPTT&RET'}
        
        deeds = self.records[self.records.doc_type.isin(self.DEED_TYPES)]
        if len(deeds) > 0:
            self.deeds = deeds
        else:
            self.deeds = None

        if self.deeds is not None:        
            deeds_gt_zero = self.deeds[self.deeds.doc_amount > 0]
            if len(deeds_gt_zero) > 0:
                self.deeds_gt_zero = deeds_gt_zero
            else:
                self.deeds_gt_zero = None
            
        self.mortgage_activity = self.records[self.records.doc_type.isin(['ASST',
                                                                          'MTGE',
                                                                          'SMTG',
                                                                          'SAT',
                                                                          'SUBM',
                                                                          'SPRD',
                                                                          'M&CON',
                                                                          'PREL',
                                                                          'ASPM',
                                                                          'CORRM',
                                                                          'AMTX'])]

    def get_deeds(self):
        return self.deeds

    def get_most_recent_deed(self):
        if self.deeds is not None:        
            return self.deeds.iloc[0]
    
    def get_most_recent_lease(self):
        r = self.records
        r = r[r.doc_type=='MLEA']
        if r is not None and len(r)>0:
            return r.iloc[0]
        
    def most_recent_lease_amount(self):
        l = self.get_most_recent_lease()
        if l is not None:
            return l.doc_amount

    def most_recent_lease_year(self):
        l = self.get_most_recent_lease()
        if l is not None:
            return l.year_recorded

    def get_most_recent_deed_year(self):
        d = self.get_most_recent_deed()
        if d is not None and len(d)>0:
            return d.year_recorded
        else:
            None
    
    def get_most_recent_positive_deed(self):
        if self.deeds is not None and len(self.deeds)>0:
            d = self.deeds[self.deeds.doc_amount>0]
            if d.empty:
                return None
            else:
                return d.iloc[0]
            
    def get_most_recent_sale_amount(self):
        d = self.get_most_recent_positive_deed()
        if d is not None:
            return int(d.doc_amount)
        else:
            return None

    def get_most_recent_sale_year(self):
        d = self.get_most_recent_positive_deed()
        if d is not None:
            return int(d.year_recorded)
        else:
            return None
        
    def any_deeds_gt_zero(self):
        d = self.deeds[self.deeds.doc_amount>0]
        if d.empty:
            return False
        else:
            return True
                
    def get_most_recent_mortgage(self):
        m = self.mortgage_activity
        if len(m)>0:
            mortgage = m[(m.doc_type=='MTGE')&(m.doc_amount>0)]
            if len(mortgage)>0:
                mortgage = mortgage.iloc[0]
                return mortgage
            else:
                return None
        else:
            return None
    
    def most_recent_mortgage_amount(self):
        m = self.get_most_recent_mortgage()
        if m is not None:
            return m.doc_amount
        else: 
            return None
    
    def most_recent_mortgage_year(self):
        m = self.get_most_recent_mortgage()
        if m is not None:
            return m.year_recorded
        else: 
            return None
        
    
    def get_positive_transfers(self):
        r = self.records
        r = r[r.pct_transferred>0]
        return r
    
    def get_most_recent_positive_transfer(self):
        r = self.get_positive_transfers()
        return r.iloc[0]
    
    def get_recent_docs(self, deed=False, mortgage=False, transfer=False, tax=False, get_all=False):
        docs = pd.DataFrame()
        
        if deed or get_all:
            d = self.get_most_recent_deed()
            docs = docs.append(d)
        if mortgage or get_all:
            d = self.get_most_recent_mortgage()
            docs = docs.append(d)
        if transfer or get_all:
            d = self.get_most_recent_positive_transfer()
            docs = docs.append(d)
        if tax or get_all:
            d = self.get_transfer_tax()
                    
        return docs
    
    def get_transfer_tax(self):
        r = self.records
        r = r[r.doc_type.isin(self.TRANSFER_TAX_TYPES)&(r.doc_amount>0)]
        return r
            
    def get_most_recent_transfer_tax(self):
        t = self.get_transfer_tax()
        if len(t)>=1:
            return t.iloc[t.year_recorded.argmax()]
        else:
            return None
    
    def most_recent_transfer_tax(self):
        tt = self.get_most_recent_transfer_tax()
        if tt is not None:
            return tt.doc_amount
        else:
            return None
    
    def most_recent_transfer_tax_year(self):
        tt = self.get_most_recent_transfer_tax()
        if tt is not None:                       
            return tt.year_recorded
        else:
            return None
    
    def most_recent_trade_year(self):
        deed = self.get_most_recent_positive_deed()
        transfer_tax = self.get_most_recent_transfer_tax()

        if deed is not None:
            return int(deed.year_recorded)
        elif transfer_tax is not None:
            return int(transfer_tax.year_recorded)
        
    def most_recent_trade_amount(self):
        deed = self.get_most_recent_positive_deed()

        if deed is not None:
            return int(deed.doc_amount)
    
    def get_ground_leases(self):
        gl = self.records[self.records.doc_type=='ASSTO']
        if gl is not None and len(gl)>0:
            return gl
        else:
            return None
    
    def most_recent_ground_lease_amount(self):
        gl = self.get_ground_leases()
        if gl is not None:
            return gl.iloc[0].doc_amount
        else:
            return None
        
    def most_recent_ground_lease_year(self):
        gl = self.get_ground_leases()
        if gl is not None:
            return gl.iloc[0].year_recorded
        else:
            return None
            
class PLUTO(object):
    def __init__(self):
        pass