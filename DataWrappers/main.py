# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 16:41:27 2021

@author: kpatel
"""

from CompStak import CompStak
from MarketView.Snapshot import Snapshot
from IPG import BrokerageDataCatalog
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 1000)

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# connect to compstak data feed
conn = CompStak()

config_file = "C://Users//kpatel//analytics-research//MarketView//config//office_manhattan_class_a.yaml"

s = Snapshot(config_file, conn)

# show configuration
print(s.params)

# compute some stats on taking rents
print('Taking Rents: Straightline')
print(s.compute_taking_rent_stats(method='straightline'))

print('Taking Rents: Weighted')
print(s.compute_taking_rent_stats(method='weighted'))

print('Free Rent: Straightline')
print(s.compute_free_rent_stats('straightline'))

print('Free Rent: Weighted')
print(s.compute_free_rent_stats('weighted'))


# free rent stats
print('Lease Term Statistics')
print(s.compute_lease_term_stats())

# transaction size stats
print('Executed Transaction Size Statistics')
print(s.compute_transaction_size_stats())

# compute stats on in place rents
print('In-Place Rent Statistics: Straightline')
print(s.compute_in_place_rent_stats('straightline'))

print('In-Place Rent Statistics: Weighted')
print(s.compute_in_place_rent_stats('weighted'))

# leasing velocity
print('Leasing Velocity')
print(s.compute_leasing_velocity())

print('Leasing Activity')
print(s.compute_leasing_activity())

print(s._get_leases_expiring_in_next(6))

b = BrokerageDataCatalog()