# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 19:13:46 2022

@author: kpatel
"""

import plotly.express as px
import pandas as pd
import plotly.io as pio
pio.renderers.default='browser'

leases = leases.sort_values('execution_date')
data = pd.DataFrame([dict(Location=x.street_address, Commencement=x.occ_range[0].date(), Expiration=x.occ_range[-1].date()) for _,x in leases.iterrows()])

fig = px.timeline(data, x_start="Commencement", x_end="Expiration", y="Location")
fig.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up
fig.show()

