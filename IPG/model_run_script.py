# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 14:35:18 2022

@author: kpatel
"""

from OfficeModels import OfficeModel
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from scipy.ndimage.filters import gaussian_filter1d

config_file = 'starting_rent.yaml'

m = OfficeModel(config_file='C:\\Users\\kpatel\\analytics-research\\IPG\\config\\{}'.format(config_file))
m._load_data()
m._feature_engineering()
m._create_train_test(test_pct=0.01)
m.run()
m.score
m.generate_visuals(outfile='output_broad_comp_set.png', include_uw=True)