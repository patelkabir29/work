# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 14:29:28 2022

@author: kpatel
"""

import pandas as pd
import requests
import json
from datetime import datetime, date
import matplotlib.pyplot as plt

class GreenStreet(object):
    
    def __init__(self):
        
        #self.CLIENT_ID='eDB6JClgWRKutgTY9HW8DQbbPoYTPcGv'
        self.CLIENT_ID = 'zGPPPcav0mTL4ZbrJijYUtkkI5E69lpD'
        #self.CLIENT_SECRET='A5rpqATd2Xgzps8ajwnY3jQlGRox7pjxlO7zROc_YMEzm1xL_CNQ5Oz_a0cf0It9'
        self.CLIENT_SECRET='D9dO0ev0gQaSfbD0b2Hd531FRppnBZGZh3a0okeHZnbLgRd-KBZ_mAAFt1PnzpHv'
        self.base_url = "https://api.greenstreet.com/"
        
        self._get_token()
        self._get_basic_data()
                
    def _get_token(self):
        
        url = self.base_url + "oauth/token"
    
        data = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "grant_type": "client_credentials",
            "audience": "https://api.greenstreet.com"
        }
    
        #response = requests.post(url, data=data)
        
        #if response.status_code==200:
        #    r = json.loads(response.text)
        
        self.token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Il9HTkgyUEFqZE5rWTJLWFZ1b1N2OCJ9.eyJodHRwczovL2RhdGEuZ3JlZW5zdHJlZXQuY29tL2dzdF9jbGllbnRfaWQiOiIxMTUxNzU1IiwiaHR0cHM6Ly9kYXRhLmdyZWVuc3RyZWV0LmNvbS9nc3RfY29tcGFueV9pZCI6IjE3NTg3IiwiaHR0cHM6Ly9kYXRhLmdyZWVuc3RyZWV0LmNvbS9wcm9kdWN0cyI6WyJVUyBSRUEiLCJORVdTIC0gUkVBbGVydCIsIk5FV1MgLSBDTUFsZXJ0IiwiTkVXUyAtIEFCQWxlcnQiLCJVUyBTYWxlcyBDb21wcyJdLCJpc3MiOiJodHRwczovL2xvZ2luLmdyZWVuc3RyZWV0LmNvbS8iLCJzdWIiOiJ6R1BQUGNhdjBtVEw0WmJySmlqWVV0a2tJNUU2OWxwREBjbGllbnRzIiwiYXVkIjoiaHR0cHM6Ly9hcGkuZ3JlZW5zdHJlZXQuY29tIiwiaWF0IjoxNjUyOTcxNDQ0LCJleHAiOjE2NTM1NzYyNDQsImF6cCI6InpHUFBQY2F2MG1UTDRaYnJKaWpZVXRra0k1RTY5bHBEIiwiZ3R5IjoiY2xpZW50LWNyZWRlbnRpYWxzIn0.H-UeQgtpySxbMVFqkpLqouY_m0pQ0cEANfDP3lqfxkf9zo43Wwz6ipmW5AmVBBrhnecp0ARW3Neb2v62BsA04NpaMgjPr8TjFvTPqPEQlRmCRyJtEuhFkWEE5M4KAvaFmeIcr9on7WFEhn5P7XVZ90v7AnBefCXvWCTfNN5Ptq6mb8xls-TYvFgb845uUdAKXGs_HckFhMU7Vlaacsf5YXCf8trHLjtTliopRUAocuKO1mG63yPnYWjbfwG3mG0TTE3anVkH8iPxpuYPkgEQL4eRsawJwjuTLcakKdCEQ8q-4W8rqyg5WqkTEroztFZKOqOWkmLVx9Oq473Kf7IbeA'
        self.headers = {"Authorization": "Bearer " + self.token}
        
    def _get_basic_data(self):
        
        self.get_markets()
        self.get_sectors()
        self.get_scenarios()

    def get_markets(self):
        url = self.base_url + "ids/markets"
        
        params = {"region": "na",
                  "limit": 1000}
                    
        response = requests.get(url, headers=self.headers, params=params)
        self.markets = json.loads(response.text)
                        
    def get_scenarios(self):
        url = self.base_url + "ids/scenarios"
        
        params = { "region": "na" }
        response = requests.get(url, headers=self.headers, params=params)
        scenarios = json.loads(response.text)
        self.scenarios = {x['scenario_publish']: x['scenario_id'] for x in scenarios}

    def get_sectors(self):
        url = self.base_url + "ids/sectors"
        params = {"region": "na"}
        response = requests.get(url, headers=self.headers, params=params)
        sectors = json.loads(response.text)
        self.sectors =  {x['sector_publish']: x['sector_id'] for x in sectors}

    def get_forecasts(self, market_id, sector_id, scenario_id):
        
        url = self.base_url + "forecasts/scenarios"
        
        params = {"date_start": "2022-01-01",
                  "date_end": "2025-01-01",
                  "market_id": [market_id],
                  "sector_id": [sector_id],
                  "scenario_id": scenario_id
                  }

        response = requests.get(url, headers=self.headers, params=params)
        
        return json.loads(response)
        
    def get_forecast_office_ny(self, market_id=195, sector_id=9):
        
        url = self.base_url + "forecasts/scenarios"
        
        f = {}
        for _scenario_name, _scenario_num in self.scenarios.items():
            
            params = {'date_start': '2000-01-01',
                      'date_end': '2030-12-31',
                      'market_id': [market_id],
                      'sector_id': [sector_id],
                      'scenario_id': _scenario_num}
            
            response = requests.get(url, headers=self.headers, params=params)
            
            temp = pd.DataFrame(json.loads(response.text))
            for col in temp.columns:
                if 'date' in col:
                    temp = temp.assign(**{col: pd.to_datetime(temp[col]).apply(lambda x: x.date())})
                
            if _scenario_name=='Baseline':
                f[_scenario_name] = temp
            else:
                f[_scenario_name] = temp[temp.date>=date(2021,5,31)]
                
        return f
    
    def get_market_sector_periodicals(self, market_id=195, sector_id=9):

        url = self.base_url + "market_sectors/periodicals"
        
        params = {"region": "na",
                  "period": "quarter",
                  "year_start": "2000",
                  "year_end": "2025",
                  "market_id": [market_id],
                  "sector_id": [sector_id]
                  }

        response = requests.get(url, headers=self.headers, params=params)
        
        return json.loads(response)

    def get_macro_historical(self, market_id=195, sector_id=9):

        url = self.base_url + "macros/summaries_macroeconomic/historical"
        
        params = {"region": "na",
                  "period": "month",
                  "date_start": "2000-01-01",
                  "date_end": "2025-01-01",
                  "limit": 1000
                  }

        response = requests.get(url, headers=self.headers, params=params)
        
        return json.loads(response)

# url = base_url + "macros/summaries_macroeconomic/historical"

# params = {'region':"na",
#           'period': 'month',
#           'date_start': '2000-01-01',
#           'date_end': '2021-12-01',
#           'limit': 300}

# response = requests.get(url, headers=headers, params=params)
# macro = json.loads(response.text)

# fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(17, 11), sharex=True)
# plt.yticks(fontsize=14)
# plt.xticks(fontsize=14)
# ax.yaxis.set_major_formatter('{x:1.2f}%')
# ax.set_ylabel('Nominal Cap Rate', fontsize=14)
# ax.set_xlabel("Date", fontsize=14)
# _handles = []

# for _scenario_name, df in f.items():
#     if 'Fed' not in _scenario_name:
#         line, = ax.plot(df.date, df.cap_rate_nominal*100, linewidth=2, label=_scenario_name)
#         _handles.append(line)

# ax.legend(handles=_handles, loc='lower right')
# ax.grid()

# url = base_url + "macros/summaries_macroeconomic/historical"

# params = {'region':"na",
#           'period': 'month',
#           'date_start': '2000-01-01',
#           'date_end': '2021-12-01',
#           'limit': 300}

# response = requests.get(url, headers=headers, params=params)
# macro = json.loads(response.text)