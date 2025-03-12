# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 15:48:38 2022

@author: kpatel
"""

from PLUTO import PLUTO
from CompStak import CompStak
from IPG import BrokerageDataCatalog
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import partial_dependence
from sklearn.model_selection import train_test_split
from sklearn import metrics
import shap
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np
from scipy.ndimage.filters import gaussian_filter1d
from datetime import date
import yaml
import re
import warnings
import logging

class OfficeModel(object):
    
    def __init__(self, config_file, load_compstak=True, load_pluto=False):

        logging.basicConfig(filename='office_model.log', encoding='utf-8', level=logging.INFO)
        
        self.derived_cols = {'lease': [],
                             'property': []}
        
        logging.info('Loading model configuration file')
        self.config = self.__load_config(config_file)
        self.config['load_compstak'] = load_compstak
        self.config['load_pluto'] = load_pluto
        
        self.submarket_mapping = self.__load_submarket_map()
        
    def _load_data(self):
        
        logging.info('Modeling Target: {}'.format(self.config['target']))
        logging.info('Input Features:')
        logging.info('Property:')
        
        for item in self.config['property']['include']:
            logging.info(item)
        
        logging.info('Lease:')
        for item in self.config['lease']['include']:
            logging.info(item)
        
        self.P_COLS = self.config['property']['include']
        self.L_COLS = self.config['lease']['include']
    
        if self.config['load_compstak']:
            self.c = CompStak()
        
        if self.config['load_pluto']:
            self.p = PLUTO()
        
        # load lease comps
        fullfile = self.config['data']['filename']
        
        leases = pd.read_excel(fullfile, sheet_name='Comps')
        leases = leases[leases.Submarket!='North Manhattan']
        leases = leases.merge(self.c.data['leases'][['id', 'rent_schedule']], how='left', left_on='Lease Id', right_on='id')
        leases = leases.drop(columns=['id'])
        
        # load properties
        props = pd.read_excel(fullfile, sheet_name='Properties')
    
        self.leases = leases
        self.props = props
        self.brokerage = BrokerageDataCatalog()
        
        self._load_market_metrics()
    
    def _load_market_metrics(self):

        # load market metrics from newmark
        metrics=self.brokerage.data['newmark']
        sm_mapping = self.submarket_mapping['newmark_to_compstak']
        metrics=metrics.assign(compstak_submarket=metrics['market'].map(sm_mapping))
        metrics = metrics.dropna(subset=['year','quarter','compstak_submarket'])
        
        MR = {'average_asking_rent': 'Market:Asking Rent',
              'vacancy_rate': 'Market:Vacancy',
              'availability_rate': 'Market:Availability',
              'leasing_activity': 'Market:Leasing Activity',
              'net_absorption': 'Market:Net Absorption',
              'quarter': 'Market:Quarter',
              'year': 'Market:Year'}

        self.metrics = metrics.rename(columns=MR)
        self.metrics = self.metrics.sort_values(by=['compstak_submarket', 'period'])
        
        metrics_mn = self.metrics[self.metrics['compstak_submarket']=='Manhattan']
        metrics_mn = metrics_mn[['Market:Year','Market:Quarter', 'Market:Vacancy', 'Market:Availability', 'Market:Net Absorption']]
                
        metrics_mn = metrics_mn.rename(columns={'Market:Vacancy': 'Market:MN(A) Vacancy',
                                                'Market:Availability': 'Market:MN(A) Availability',
                                                'Market:Net Absorption': 'Market:MN(A) Net Absorption'})
        
        self.metrics = self.metrics[self.metrics['compstak_submarket']!='Manhattan']
        self.metrics = self.metrics.merge(metrics_mn, how='inner', left_on=['Market:Quarter', 'Market:Year'], right_on=['Market:Quarter', 'Market:Year'])
        
    def _feature_engineering(self):
        
        leases = self.leases
        props = self.props
            
        leases = leases.rename(columns={'rent_schedule': 'Rent Schedule',
                                        'Lease Term': 'Term'})
                                        
        leases['Rent Schedule'] = leases['Rent Schedule'].replace(np.nan,'0/0')
        
        leases = leases.rename(columns={col: 'Lease:{}'.format(col) for col in leases.columns})
        props = props.rename(columns={col: 'Building:{}'.format(col) for col in props.columns if ':' not in col})
        #leases = leases.assign(**{'Derived:Lease:DTerm': leases.apply(self.derived_term, axis=1)})
        
        #leases = leases.assign(**{'Temp:Lease:Total Rent': leases.apply(self.total_rent, axis=1)})
        #leases = leases.assign(**{'Temp:Lease:Blended Gross Rent': leases.apply(self.blended_gross_rent, axis=1)})
        #leases = leases.assign(**{'Temp:Lease:Concession Value': leases.apply(self.concession_value, axis=1)})
        #leases = leases.assign(**{'Derived:Lease:Concession PSF': leases.apply(self.concession_psf, axis=1)})
        #leases = leases.assign(**{'Temp:Lease:NER': leases['Derived:Lease:Blended Gross Rent'] - leases['Derived:Lease:Concession PSF']})
         
        leases = leases[leases['Lease:Exclude'].isna()]
        leases = leases.reset_index(drop=True)
                
        leases = leases.assign(**{'Lease:Transaction Quarter': leases['Lease:Transaction Quarter'].apply(lambda x: x[1])})
        leases['Lease:Floors Occupied'].astype(str).apply(lambda x: re.findall(r'\d+', x))
        leases = leases.assign(**{'Derived:Lease:Floors:Max': leases['Lease:Floors Occupied'].apply(self.extract_highest_floor)})
        leases = leases.assign(**{'Derived:Lease:Floors:Avg': leases['Lease:Floors Occupied'].apply(self.compute_avg_floor)})
        
        leases = leases.assign(**{'Derived:Lease:Relative Position': leases['Derived:Lease:Floors:Avg']/leases['Lease:Number Of Floors']})
        leases = leases.assign(**{'Derived:Lease:Multi-Floor': leases['Lease:Floors Occupied'].apply(self.multi_floor)})
        leases = leases.assign(**{'Derived:Lease:Entire Floor': leases['Lease:Floors Occupied'].apply(self.entire_floor)})
        leases = pd.concat([leases, pd.get_dummies(leases['Lease:Tenant Industry'].replace(np.nan,'Unknown').apply(lambda x: 'Lease:Industry:{}'.format(x)))], axis=1)
        
        self.L_COLS_DERIVED = [x for x in leases.columns if 'Derived:' in x] + [x for x in leases.columns if 'Industry:' in x]
        
        props = props.merge(self.c.data['properties'][['id', 'latitude', 'longitude']], left_on='Building:Property Id', right_on='id')
        props = props.rename(columns={'latitude': 'Locational:Location (N/S)', 'longitude': 'Locational:Location (E/W)'})
        props = props.assign(**{'Derived:Building:Age': 2022-props['Building:Year Built']})
        props = props.assign(**{'Derived:Building:Years Since Reno': props.apply(self.years_since_reno,axis=1)})
        #props = props.assign(**{'Derived:Locational:Transit Accessibility:Walking': props[['Locational:Transit Accessibility:GCT via Walking','Locational:Transit Accessibility:Penn via Walking']].mean(axis=1)})
        props = pd.concat([props, pd.get_dummies(props['Building:Owner Type'].apply(lambda x: 'Building:Owner:{}'.format(x)))], axis=1)
        props = pd.concat([props, pd.get_dummies(props['Locational:Submarket'].apply(lambda x: 'Locational:Submarket:{}'.format(x)))], axis=1)
        
        filters = self.config['property'].get('filters')
        
        if filters is not None:
            
            gt_filters = filters.get('gt')
            if gt_filters is not None:
                for key, value in gt_filters.items():
                    props = props[props[key] >= value]       
            
            eq_filters = filters.get('eq')
            if eq_filters is not None:
                for key, value in eq_filters.items():
                    props = props[props[key]==value]
            
            lt_filters = filters.get('lt')
            
            if lt_filters is not None:
                for key, value in lt_filters.items():
                    props = props[props[key]<=value]
        
        self.props = props
        self.leases = leases

        self.P_COLS_DERIVED = [x for x in props.columns if 'Derived:' in x] + [x for x in props.columns if 'Owner:' in x]
        
        data = pd.merge(self.props, self.leases, how='left', left_on='Building:Property Id', right_on='Lease:Property Id')
        
        # get lease comps ready to merge with market metrics
        data = data.dropna(subset=['Lease:Transaction Quarter', 'Lease:Execution Year', 'Locational:Submarket'])        
        data['Lease:Transaction Quarter'] = data['Lease:Transaction Quarter'].astype(int)
        data['Lease:Execution Year'] = data['Lease:Execution Year'].astype(int)
        
        # join lease comps with market metrics
        data = data.merge(self.metrics, how='left', left_on=['Lease:Transaction Quarter', 'Lease:Execution Year', 'Locational:Submarket'], right_on=['Market:Quarter', 'Market:Year', 'compstak_submarket'])
        data = data.drop(columns=['period','month','market', 'rent_growth_rate', 'leasing_activity_pct', 'compstak_submarket'])
        
        COLS = list(data.columns)
        COLS.remove('Lease:Property Id')
        COLS.remove('id')
        data = data[COLS]
        
        data = data.rename(columns={col: col.lstrip('Derived:') for col in data.columns})
        
        self.L_COLS_DERIVED = [x.lstrip('Derived:') for x in self.L_COLS_DERIVED]
        self.P_COLS_DERIVED = [x.lstrip('Derived:') for x in self.P_COLS_DERIVED]
        self.MARKET_METRICS = self.config['market']['include']
        
        L = pd.cut(data['Lease:Starting Rent'], bins=50, labels=['L{}'.format(x) for x in range(1,51)])
        data = data.assign(**{'Lease:Rent Bin':L})
                
        self.data = {'all': data}
     
    def _create_train_test(self, test_pct=0.1):
        
        if self.config['model'] == 'standard':
            # define train and test sets
            self.data['train'] = {'input': None, 'target': None}
            self.data['test'] = {'input': None, 'target': None}
            
            data = self.data['all'].dropna(subset=self.P_COLS+self.P_COLS_DERIVED+self.L_COLS+self.L_COLS_DERIVED+self.MARKET_METRICS)
            
            train, test = train_test_split(data[self.P_COLS+self.P_COLS_DERIVED+self.L_COLS+self.L_COLS_DERIVED+self.MARKET_METRICS], test_size=test_pct)
    
            self.target = self.config['target']
            self.COLS = list(train.columns)
            self.COLS.remove(self.target)
    
            self.data['train']['input'] = train[self.COLS]
            self.data['train']['target'] = train[self.target]
            
            self.data['test']['input'] = test[self.COLS]
            self.data['test']['target'] = test[self.target]
        
        elif self.config['model']=='lwci':
            # define training (regression & lwci) and test sets
            self.data['train'] = {'regression': {'input': None, 'target': None},
                                  'lwci': {'input': None, 'target': None}}
            
            self.data['test'] = {'input': None, 'target': None, 'predictions': None, 'errors': None}
            
            data = self.data['all'].dropna(subset=self.P_COLS+self.P_COLS_DERIVED+self.L_COLS+self.L_COLS_DERIVED+self.MARKET_METRICS)
            
            train, test = train_test_split(data[self.P_COLS+self.P_COLS_DERIVED+self.L_COLS+self.L_COLS_DERIVED+self.MARKET_METRICS], test_size=test_pct)
            train_reg, train_lwci = train_test_split(train, test_size=0.5)
    
            self.target = self.config['target']
            self.COLS = list(train.columns)
            self.COLS.remove(self.target)
    
            self.data['train']['regression']['input'] = train_reg[self.COLS]
            self.data['train']['regression']['target'] = train_reg[self.target]
            
            self.data['train']['lwci']['input'] = train_lwci[self.COLS]
            self.data['train']['lwci']['target'] = train_lwci[self.target]
                        
            self.data['test']['input'] = test[self.COLS]
            self.data['test']['target'] = test[self.target]
            self.data['test']['predictions'] = {'target': None, 'lwci': None}
            self.data['test']['errors'] = {'target': None, 'lwci': None}  
            
    # def apply_target_transformation(self, func, target):
    #     return func(target)
        
    def run_standard(self):
        
        self.regr = RandomForestRegressor(n_estimators=100, random_state=0)
        self.regr.fit(self.data['train']['input'], self.data['train']['target'])
        self.regr_explainer = shap.TreeExplainer(self.regr)
        
        # predict on train set
        pred_train = self.regr.predict(self.data['train']['input'])
        self.data['train']['predictions'] = pred_train

        # predict on test set
        pred_test = self.regr.predict(self.data['test']['input'])
        self.data['test']['predictions'] = pred_test
        
        sr = self.data['all']['Lease:Starting Rent']
        dl = sr[sr<sr.mean()]-sr.mean()
        sigma_l = np.sqrt((dl*dl).sum()/dl.shape[0])
        dr = sr[sr>sr.mean()]-sr.mean()
        sigma_r = np.sqrt((dr*dr).sum()/dr.shape[0])
        
        # compute some error metrics
        self.score = {'r2': self.regr.score(self.data['test']['input'], self.data['test']['target']),
                      'rmse': np.sqrt(metrics.mean_squared_error(self.data['test']['target'], pred_test)),
                      'mae': metrics.mean_absolute_error(self.data['test']['target'], pred_test),
                      'mape': metrics.mean_absolute_percentage_error(self.data['test']['target'], pred_test),
                      'sigma_l': sigma_l,
                      'sigma_r': sigma_r}
    
        train_errors = np.abs(self.data['train']['target'] - pred_train)
    
        self.lwci = RandomForestRegressor(n_estimators=100, random_state=0)
        self.lwci.fit(self.data['train']['input'], train_errors)
        
        # predict errors on the test data
        lwci_test = self.lwci.predict(self.data['test']['input'])
        
        # actual errors on test data
        test_errors = np.abs(self.data['test']['target'] - self.data['test']['predictions'])
                
        self.score_lwci = {'r2': self.lwci.score(self.data['test']['input'], test_errors),
                           'rmse': np.sqrt(metrics.mean_squared_error(test_errors, lwci_test)),
                           'mae': metrics.mean_absolute_error(test_errors, lwci_test),
                           'mape': metrics.mean_absolute_percentage_error(test_errors, lwci_test)}

        features = self.regr.feature_names_in_
        impacts = self.regr.feature_importances_

        features = features[impacts.argsort()]
        impacts = impacts[impacts.argsort()]

        self.feature_impact = {'root': None,
                               'level1': None,
                               'level2': None}
        
        fi = pd.DataFrame({'Features': features,
                           'Impact': impacts})

        temp = pd.DataFrame(fi['Features'].apply(lambda x: x.split(':')).to_list(), columns=['Category','Category2','Category3'])
        
        self.feature_impact['root'] = pd.concat([fi, temp], axis=1)

        self.feature_impact['level1'] = self.feature_impact['root'].groupby('Category').sum().sort_values(by='Impact', ascending=False).reset_index()
        self.feature_impact['level2'] = self.feature_impact['root'].groupby(['Category', 'Category2']).sum().sort_values(by='Impact', ascending=False).reset_index()

    def run_lwci(self):
        
        self.regr = RandomForestRegressor(n_estimators=100, random_state=0)
        self.regr.fit(self.data['train']['regression']['input'], self.data['train']['regression']['target'])
        
        # predict starting rents on the training set
        pred_train = self.regr.predict(self.data['train']['regression']['input'])

        # predict starting rents on the lwci training set
        pred_lwci = self.regr.predict(self.data['train']['lwci']['input']) # predict the errors
        # now compute the actual compute actual errors
        self.data['train']['lwci']['errors'] = pred_lwci - self.data['train']['lwci']['target']

        # train the lwci model on the actual errors of the regression model on the lwci input
        self.lwci = RandomForestRegressor(n_estimators=100, random_state=0)
        # train the lwci model on the actual error of the regression model applied to lwci input
        self.lwci.fit(self.data['train']['lwci']['input'], self.data['train']['lwci']['errors'])
        
        # use the lwci model to predict the errors on the training lwci set
        pred_lwci = self.lwci.predict(self.data['train']['lwci']['input'])
        self.data['train']['lwci']['predictions'] = pred_lwci
        
        # predict the starting rent on the test set
        pred_test = self.regr.predict(self.data['test']['input']) # predicted rent
        # predict the errors of the regression model on the same test set
        pred_lwci = self.lwci.predict(self.data['test']['input']) # predicted error on rent
        
        self.data['test']['predictions']['target'] = pred_test
        self.data['test']['predictions']['lwci'] = pred_lwci

        # compute the error of the regression model on test data
        self.data['test']['errors']['target'] = np.abs(self.data['test']['target'] - pred_test)
        # compute the error of the lwci model on test data
        self.data['test']['errors']['lwci'] = np.abs(self.data['test']['predictions']['target'] - self.data['test']['target']) - pred_lwci
                
        self.score = {'regression': None, 'lwci': None}
        
        # compute error metrics on the regression model
        self.score['regression'] = {'r2': self.regr.score(self.data['test']['input'], self.data['test']['target']),
                                    'rmse': np.sqrt(metrics.mean_squared_error(self.data['test']['target'], pred_test)),
                                    'mae': metrics.mean_absolute_error(self.data['test']['target'], pred_test),
                                    'mape': metrics.mean_absolute_percentage_error(self.data['test']['target'], pred_test)}

        # compute error metrics on the lwci model
        self.score['lwci'] = {'r2': self.lwci.score(self.data['test']['input'], self.data['test']['errors']['target']),
                              'rmse': np.sqrt(metrics.mean_squared_error(self.data['test']['errors']['target'], pred_lwci)),
                              'mae': metrics.mean_absolute_error(self.data['test']['errors']['target'], pred_lwci),
                              'mape': metrics.mean_absolute_percentage_error(self.data['test']['errors']['target'], pred_lwci)}
    
        features = self.regr.feature_names_in_
        impacts = self.regr.feature_importances_

        features = features[impacts.argsort()]
        impacts = impacts[impacts.argsort()]

        self.feature_impact = {'root': None,
                               'level1': None,
                               'level2': None}
        
        fi = pd.DataFrame({'Features': features,
                           'Impact': impacts})

        temp = pd.DataFrame(fi['Features'].apply(lambda x: x.split(':')).to_list(), columns=['Category','Category2','Category3'])
        
        self.feature_impact['root'] = pd.concat([fi, temp], axis=1)

        self.feature_impact['level1'] = self.feature_impact['root'].groupby('Category').sum().sort_values(by='Impact', ascending=False).reset_index()
        self.feature_impact['level2'] = self.feature_impact['root'].groupby(['Category', 'Category2']).sum().sort_values(by='Impact', ascending=False).reset_index()

    def run(self):
        if self.config['model']=='standard':
            self.run_standard()
        elif self.config['model']=='lwci':
            self.run_lwci()
        
    def __load_config(self, fullfile):
        
        with open(fullfile, 'rb') as fp:
            config =   yaml.load(fp, Loader=yaml.FullLoader)
            return config
        
    def __load_submarket_map(self):

        fullfile = 'C:\\Users\\kpatel\\analytics-research\\IPG\\config\\submarket_mapping.yaml'
        
        with open(fullfile, 'rb') as fp:
            mapping = yaml.load(fp, Loader=yaml.FullLoader)            
            return mapping
        
    def total_rent(self, x):
        rs = str(x['Lease:Rent Schedule'])
        rs = rs.split(', ')
        
        if rs[0]!='0/0':
            acc_rent = []
            p = []
            for item in rs:
                rent, time = item.split('/')
                rent = float(rent)
                time, period = time.split()
                time = float(time)
                acc_rent.append(rent*time)
                p.append(time)
            return sum(acc_rent)*x['Lease:Transaction Sqft']/12
        else:
            return x['Lease:Starting Rent']*x['Lease:Transaction Sqft']*(x['Lease:Term']/12)
    
    def derived_term(self, x):
        rs = str(x['Lease:Rent Schedule'])
        rs = rs.split(', ')
        
        if rs[0]!='0/0':
            p = []
            for item in rs:
                _, time = item.split('/')
                time, period = time.split()
                time = float(time)
                p.append(time)
            return sum(p)
        else:
            return x['Lease:Term']
            
    def blended_gross_rent(self, x):
        bgr = x['Temp:Lease:Total Rent'] / x['Lease:Transaction Sqft'] / (x['Lease:Term']/12)
        return bgr
    
    def concession_value(self, x):
        rent_first = float(x['Lease:Rent Schedule'].split(', ')[0].split('/')[0])
        if rent_first==0:
            rent_first = x['Lease:Starting Rent']
        return ( (x['Lease:Free Rent']/12)*rent_first + x['Lease:Ti Value Work Value'])*x['Lease:Transaction Sqft']
    
    def concession_psf(self, x):
        return x['Temp:Lease:Concession Value']/x['Lease:Transaction Sqft']/(x['Lease:Term']/12)
    
    def extract_highest_floor(self, txt):
        txt = str(txt)
        x = re.findall(r'\d+', txt)
        x = [int(item) for item in x]
        
        if len(x):
            return max(x)
        else:
            return np.nan
    
    def compute_avg_floor(self, txt):
        txt = str(txt)
        x = re.findall(r'\d+', txt)
        x = [int(item) for item in x]
        
        if len(x):
            return np.mean(x)
        else:
            return np.nan
        
    def multi_floor(self, txt):
        txt = str(txt)
        x = re.findall(r'\d+', txt)
        x = [int(item) for item in x]
        
        if len(x)>1:
            return True
        else:
            return False
    
    def entire_floor(self, txt):
        txt = str(txt)
        if 'Entire' in txt:    
            return True
        else:
            return False        
    
    def years_since_reno(self, x):
        if np.isnan(x['Building:Year Renovated']):
            return (date.today() - date(int(x['Building:Year Built']), 7, 1)).days/365
        else:
            return (date.today() - date(int(x['Building:Year Renovated']),7,1)).days/365

    def get_previous_qtr_metrics(self, qtr_current, year_current, submarket):

        qtr_previous = qtr_current - 1
        if qtr_previous==0:
            qtr_previous = 4
            year = year_current - 1
        else:
            year = year_current
        
        bool_exp = (self.metrics['Market:Quarter']==qtr_previous)&(self.metrics['Market:Year']==year)&(self.metrics['compstak_submarket']==submarket)
        
        prev_qtr = self.metrics[bool_exp]
        
        return {'Market:Previous Quarter': [prev_qtr['Market:Quarter'].iloc[0]],
                'Market:Previous Year': [prev_qtr['Market:Year'].iloc[0]],
                'Market:Previous Vacancy': [prev_qtr['Market:Vacancy'].iloc[0]],
                'Market:Previous Asking Rent': [prev_qtr['Market:Asking Rent'].iloc[0]],
                'Market:Previous Availability': [prev_qtr['Market:Availability'].iloc[0]],
                'Market: Previous Net Absorption': [prev_qtr['Market:Net Absorption'].iloc[0]]}
    
    def predict(self, X):
        COLS_REQ = set(self.data['train']['input'].columns)
        COLS_IN = set(X.columns)

        if len(COLS_REQ - COLS_IN)==0:
                y = self.regr.predict(X)
                return y
        else:
            return None
            
    def compute_shapley(self, X):
        sv = self.regr_explainer(X)
        return sv
    
    #def plot_shapley(self, X):
    #    sv = self.regr_explainer(X)
                        
    def generate_visuals(self, outfile='output.png', metric='rmse', include_uw=False):
        
        fig = plt.figure(figsize=(30, 20))
        gs = fig.add_gridspec(3,4)
        ax = []
        ax.append(fig.add_subplot(gs[0,:]))
        ax.append(fig.add_subplot(gs[1,0]))
        ax.append(fig.add_subplot(gs[1,1]))
        ax.append(fig.add_subplot(gs[1,2]))
        ax.append(fig.add_subplot(gs[1,3]))
        ax.append(fig.add_subplot(gs[2,0]))
        ax.append(fig.add_subplot(gs[2,1]))
        ax.append(fig.add_subplot(gs[2,2:4]))
        
        ax[0].pie(self.feature_impact['level1']['Impact'], labels=self.feature_impact['level1']['Category'], autopct=lambda p: '{:.1f}%'.format(p))
        ax[0].set_title('Model Discovered Correlations to Starting Rent', fontsize=18)
        ax[0].yaxis.set_major_formatter('{x:1.0f}%')
        ax[0].text(0.925, 0.075, u"R\u00b2={}".format(round(self.score['r2'],2), style='italic'), transform=ax[0].transAxes, fontsize=16, verticalalignment='top')

        COLORS = ['tab:blue', 'tab:orange','tab:green', 'tab:red']
        
        for indx, category in enumerate(['Locational', 'Lease', 'Building', 'Market']):
            temp = self.feature_impact['level2'][self.feature_impact['level2']['Category']==category]
            if category=="Market":
                temp = temp.replace('Net Absorption', 'Submarket\nNet\nAbsorption')
                temp = temp.replace('Vacancy', 'Submarket\nVacancy')
                temp = temp.replace('Availability', 'Submarket\nAvailability')
                temp = temp.replace('MN(A) Net Absorption', 'Manhattan\nNet\nAbsorption')
                temp = temp.replace('MN(A) Vacancy', 'Manhattan\nVacancy')
                temp = temp.replace('MN(A) Availability', 'Manhattan\nAvailability')
            elif category in ['Lease', 'Building', 'Locational']:
                temp = temp.assign(Category2=temp.Category2.apply(lambda x: '\n'.join(x.split(' '))))
                
            temp = temp.sort_values(by='Impact')
            
            # normalize explained variance per category
            temp['Impact'] = temp['Impact']/temp['Impact'].sum()
            
            ax[indx+1].set_title('{} Attributes'.format(category), fontsize=16)
            ax[indx+1].barh(temp['Category2'], temp['Impact'], color=COLORS[indx])
        
        # PDP by Time Since Reno
        # AGE = range(0,26)
        # FLOOR = 15
        COEFF=1.0

        # preds = []
        COLS_TRAIN = list(self.data['train']['input'].columns)
        
        train_target = self.data['train']['target']
        test_target = self.data['test']['target']
        
        ax[5].set_title('Distribution of Training & Test Data', fontsize=14)
        ax[5].hist([train_target, test_target], bins=35, label=['Training Set (90%)', 'Test Set (10%)'], density=True, alpha=0.75)
        ax[5].legend(loc='upper right')
        ax[5].grid()
        
        ax[6].set_title('Distribution of Mean Abs Percentage Error (on Test Data)', fontsize=14)
        ax[6].hist(100*(self.data['test']['target'] - self.data['test']['predictions'])/self.data['test']['target'], bins=35, label='Mean Absolute Pct Error', density=True, alpha=0.75, range=(-30,30))
        ax[6].legend(loc='upper right')
        ax[6].grid()
        
        # PDP by Floor of Occupancy
        preds = []
        fl = list(range(2,31))
        fl.remove(13)
        SF = [7025,41132,41460,41323,41247,40686,41604,29448,37845,16938,15865,14625,16263,16263,16267,16275,16314,16294,16428,16428,16428,16428,16288,16291,16130,16140,16298,14690]
        
        for floor_num, sf in zip(fl,SF):
            x=self.data['all']
            x = x[x['Building:Property Id']==290][COLS_TRAIN].iloc[0]       
            
            # set most recent market KPIs
            x['Market:MN(A) Vacancy'] = 0.1
            x['Market:MN(A) Net Absorption'] = -3488981
            x['Market:MN(A) Availability'] = 0.18
            x['Market:Vacancy'] = .109
            x['Market:Net Absorption'] = -372911
            x['Market:Availability'] = .164
            
            # set 452 building attributes
            x['Building:Years Since Reno'] = 0
            x['Building:Age'] = 0
            x['Lease:Transaction Sqft'] = sf
            x['Lease:Execution Year'] = 2022
            x['Lease:Floors:Max'] = floor_num
            x['Lease:Floors:Avg'] = floor_num
            x['Lease:Relative Position'] = floor_num/30
            x['Lease:Term'] = 11*12
            x['Lease:Multi-Floor'] = True
            x['Lease:Entire Floor'] = True
        
            x = pd.DataFrame(x)
            x = pd.DataFrame({col:[value] for col,value in zip(x.index,x.values)})
            y = self.regr.predict(x)
            preds.append(COEFF*y[0])
        
        preds_smooth = gaussian_filter1d(preds, sigma=0.75)
        ax[7].plot(fl, preds_smooth, linewidth=2)
        ax[7].fill_between(fl, preds_smooth + self.score[metric]*2*self.score['sigma_r']/(self.score['sigma_l']+self.score['sigma_l']), preds_smooth - self.score[metric]*2*self.score['sigma_l']/(self.score['sigma_l']+self.score['sigma_r']), alpha=0.125)
        
        if include_uw:
            uw = [85,85,85,85,85,85,85,85,115,125, 105, 105, 120, 120, 120, 120, 120, 120, 125, 125, 125, 125, 125, 130, 130, 130, 130, 130]
            ax[7].scatter(fl, uw, marker='o', color='orange')
        
        ax[7].grid()
        ax[7].set_title('452 5th Ave: Starting Rents', fontsize=14)
        ax[7].set_xlabel('Floor #')
        
        ax[7].set_ylabel('{} ($ PSF)'.format(self.target.lstrip('Lease:')))
        ax[7].xaxis.set_major_locator(MaxNLocator(integer=True))
        ax[7].yaxis.set_major_formatter('${x:1.0f}')
        
        fig.savefig(outfile)
                
class IndustrialModel(object):
    
    def __init__(self, config_file, load_compstak=True, load_pluto=False):

        logging.basicConfig(filename='industrial_model.log', encoding='utf-8', level=logging.INFO)
        
        self.derived_cols = {'lease': [],
                             'property': []}
        
        logging.info('Loading model configuration file')
        self.config = self.__load_config(config_file)
        self.config['load_compstak'] = load_compstak
        self.config['load_pluto'] = load_pluto
        
        self.submarket_mapping = self.__load_submarket_map()
        
    def _load_data(self):
        
        logging.info('Modeling Target: {}'.format(self.config['target']))
        logging.info('Input Features:')
        logging.info('Property:')
        
        for item in self.config['property']['include']:
            logging.info(item)
        
        logging.info('Lease:')
        for item in self.config['lease']['include']:
            logging.info(item)
        
        self.P_COLS = self.config['property']['include']
        self.L_COLS = self.config['lease']['include']
    
        if self.config['load_compstak']:
            self.c = CompStak()            
        
        if self.config['load_pluto']:
            self.p = PLUTO()
        
        # load lease comps
        fullfile = self.config['data']['filename']
        
        leases = pd.read_excel(fullfile, sheet_name='Comps')                
        #leases = leases.drop(columns=['id'])
        
        # load properties
        props = pd.read_excel(fullfile, sheet_name='Properties')
    
        self.leases = leases
        self.props = props
        #self.brokerage = BrokerageDataCatalog()
        
        #self._load_market_metrics()
    
    def _load_market_metrics(self):

        # load market metrics from newmark
        metrics=self.brokerage.data['newmark']
        sm_mapping = self.submarket_mapping['newmark_to_compstak']
        metrics=metrics.assign(compstak_submarket=metrics['market'].map(sm_mapping))
        metrics = metrics.dropna(subset=['year','quarter','compstak_submarket'])
        
        MR = {'average_asking_rent': 'Market:Asking Rent',
              'vacancy_rate': 'Market:Vacancy',
              'availability_rate': 'Market:Availability',
              'leasing_activity': 'Market:Leasing Activity',
              'net_absorption': 'Market:Net Absorption',
              'quarter': 'Market:Quarter',
              'year': 'Market:Year'}

        self.metrics = metrics.rename(columns=MR)
        self.metrics = self.metrics.sort_values(by=['compstak_submarket', 'period'])
        
        metrics_mn = self.metrics[self.metrics['compstak_submarket']=='Manhattan']
        metrics_mn = metrics_mn[['Market:Year','Market:Quarter', 'Market:Vacancy', 'Market:Availability', 'Market:Net Absorption']]
                
        metrics_mn = metrics_mn.rename(columns={'Market:Vacancy': 'Market:MN(A) Vacancy',
                                                'Market:Availability': 'Market:MN(A) Availability',
                                                'Market:Net Absorption': 'Market:MN(A) Net Absorption'})
        
        self.metrics = self.metrics[self.metrics['compstak_submarket']!='Manhattan']
        self.metrics = self.metrics.merge(metrics_mn, how='inner', left_on=['Market:Quarter', 'Market:Year'], right_on=['Market:Quarter', 'Market:Year'])
        
    def _feature_engineering(self):
        
        leases = self.leases
        props = self.props
            
        # leases = leases.rename(columns={'rent_schedule': 'Rent Schedule',
        #                                 'Lease Term': 'Term'})
                                        
        # leases['Rent Schedule'] = leases['Rent Schedule'].replace(np.nan,'0/0')
        
        # leases = leases.rename(columns={col: 'Lease:{}'.format(col) for col in leases.columns})
        # props = props.rename(columns={col: 'Building:{}'.format(col) for col in props.columns if ':' not in col})
        # leases = leases.assign(**{'Derived:Lease:DTerm': leases.apply(self.derived_term, axis=1)})
        
        #leases = leases.assign(**{'Temp:Lease:Total Rent': leases.apply(self.total_rent, axis=1)})
        #leases = leases.assign(**{'Temp:Lease:Blended Gross Rent': leases.apply(self.blended_gross_rent, axis=1)})
        #leases = leases.assign(**{'Temp:Lease:Concession Value': leases.apply(self.concession_value, axis=1)})
        #leases = leases.assign(**{'Derived:Lease:Concession PSF': leases.apply(self.concession_psf, axis=1)})
        #leases = leases.assign(**{'Temp:Lease:NER': leases['Derived:Lease:Blended Gross Rent'] - leases['Derived:Lease:Concession PSF']})
         
        # leases = leases[leases['Lease:Exclude'].isna()]
        # leases = leases.reset_index(drop=True)
                
        # leases = leases.assign(**{'Lease:Transaction Quarter': leases['Lease:Transaction Quarter'].apply(lambda x: x[1])})
        # leases['Lease:Floors Occupied'].astype(str).apply(lambda x: re.findall(r'\d+', x))
        # leases = leases.assign(**{'Derived:Lease:Floors:Max': leases['Lease:Floors Occupied'].apply(self.extract_highest_floor)})
        # leases = leases.assign(**{'Derived:Lease:Floors:Avg': leases['Lease:Floors Occupied'].apply(self.compute_avg_floor)})
        
        # leases = leases.assign(**{'Derived:Lease:Relative Position': leases['Derived:Lease:Floors:Avg']/leases['Lease:Number Of Floors']})
        # leases = leases.assign(**{'Derived:Lease:Multi-Floor': leases['Lease:Floors Occupied'].apply(self.multi_floor)})
        # leases = leases.assign(**{'Derived:Lease:Entire Floor': leases['Lease:Floors Occupied'].apply(self.entire_floor)})
        # leases = pd.concat([leases, pd.get_dummies(leases['Lease:Tenant Industry'].replace(np.nan,'Unknown').apply(lambda x: 'Lease:Industry:{}'.format(x)))], axis=1)
        
        # self.L_COLS_DERIVED = [x for x in leases.columns if 'Derived:' in x] + [x for x in leases.columns if 'Industry:' in x]
        
        #props = props.merge(self.c.data['properties'][['id', 'latitude', 'longitude']], left_on='Building:Property Id', right_on='id')
        #props = props.rename(columns={'latitude': 'Locational:Location (N/S)', 'longitude': 'Locational:Location (E/W)'})
        self.props['Property:Year Built'] = self.props['Property:Year Built'].replace(np.nan,self.props['Property:Year Built'].mean())
        props = props.assign(**{'Derived:Building:Age': 2022-props['Property:Year Built']})
        props = props.assign(**{'Derived:Building:Years Since Reno': props.apply(self.years_since_reno,axis=1)})
        #props = props.assign(**{'Derived:Locational:Transit Accessibility:Walking': props[['Locational:Transit Accessibility:GCT via Walking','Locational:Transit Accessibility:Penn via Walking']].mean(axis=1)})
        #props = pd.concat([props, pd.get_dummies(props['Building:Owner Type'].apply(lambda x: 'Building:Owner:{}'.format(x)))], axis=1)
        #props = pd.concat([props, pd.get_dummies(props['Locational:Submarket'].apply(lambda x: 'Locational:Submarket:{}'.format(x)))], axis=1)
                
        filters = self.config['property'].get('filters')
        
        if filters is not None:
            
            gt_filters = filters.get('gt')
            if gt_filters is not None:
                for key, value in gt_filters.items():
                    props = props[props[key] >= value]
            
            eq_filters = filters.get('eq')
            if eq_filters is not None:
                for key, value in eq_filters.items():
                    props = props[props[key]==value]
            
            lt_filters = filters.get('lt')
            if lt_filters is not None:
                for key, value in lt_filters.items():
                    props = props[props[key]<=value]
        
        self.props = props
        self.leases = leases

        self.P_COLS_DERIVED = [x for x in props.columns if 'Derived:' in x] + [x for x in props.columns if 'Owner:' in x]
        
        data = pd.merge(self.props, self.leases, how='left', left_on='Property:Id', right_on='Property:Id')
        
        # get lease comps ready to merge with market metrics                
        data = data.dropna(subset=['Lease:Transaction Quarter', 'Lease:Execution Year', 'Locational:Submarket'])
        data['Lease:Transaction Quarter'] = data['Lease:Transaction Quarter'].astype(int)
        data['Lease:Execution Year'] = data['Lease:Execution Year'].astype(int)
        
        # join lease comps with market metrics
        data = data.merge(self.metrics, how='left', left_on=['Lease:Transaction Quarter', 'Lease:Execution Year', 'Locational:Submarket'], right_on=['Market:Quarter', 'Market:Year', 'compstak_submarket'])
        data = data.drop(columns=['period','month','market', 'rent_growth_rate', 'leasing_activity_pct', 'compstak_submarket'])
        
        COLS = list(data.columns)
        COLS.remove('Lease:Property Id')
        COLS.remove('id')
        data = data[COLS]
        
        data = data.rename(columns={col: col.lstrip('Derived:') for col in data.columns})
        
        self.L_COLS_DERIVED = [x.lstrip('Derived:') for x in self.L_COLS_DERIVED]
        self.P_COLS_DERIVED = [x.lstrip('Derived:') for x in self.P_COLS_DERIVED]
        self.MARKET_METRICS = self.config['market']['include']
        
        self.data = {'all': data}
     
    def _create_train_test(self, test_pct=0.1):
        # define train and test sets
        self.data['train'] = {'input': None, 'target': None}
        self.data['test'] = {'input': None, 'target': None}
        
        data = self.data['all'].dropna(subset=self.P_COLS+self.P_COLS_DERIVED+self.L_COLS+self.L_COLS_DERIVED+self.MARKET_METRICS)
        
        train, test = train_test_split(data[self.P_COLS+self.P_COLS_DERIVED+self.L_COLS+self.L_COLS_DERIVED+self.MARKET_METRICS], test_size=test_pct)

        self.target = self.config['target']
        self.COLS = list(train.columns)
        self.COLS.remove(self.target)

        self.data['train']['input'] = train[self.COLS]
        self.data['train']['target'] = train[self.target]
        
        self.data['test']['input'] = test[self.COLS]
        self.data['test']['target'] = test[self.target]
            
    def run(self):
        self.regr = RandomForestRegressor(n_estimators=100, random_state=0)
        self.regr.fit(self.data['train']['input'], self.data['train']['target'])
        
        # predict on test set
        pred_train = self.regr.predict(self.data['train']['input'])
        self.data['train']['predictions'] = pred_train

        # predict on test set
        pred_test = self.regr.predict(self.data['test']['input'])
        self.data['test']['predictions'] = pred_test        

        # compute some error metrics
        self.score = {'r2': self.regr.score(self.data['test']['input'], self.data['test']['target']),
                      'rmse': np.sqrt(metrics.mean_squared_error(self.data['test']['target'], pred_test)),
                      'mae': metrics.mean_absolute_error(self.data['test']['target'], pred_test),
                      'mape': metrics.mean_absolute_percentage_error(self.data['test']['target'], pred_test)}
    
        features = self.regr.feature_names_in_
        impacts = self.regr.feature_importances_

        features = features[impacts.argsort()]
        impacts = impacts[impacts.argsort()]

        self.feature_impact = {'root': None,
                               'level1': None,
                               'level2': None}
        
        fi = pd.DataFrame({'Features': features,
                           'Impact': impacts})

        temp = pd.DataFrame(fi['Features'].apply(lambda x: x.split(':')).to_list(), columns=['Category','Category2','Category3'])
        
        self.feature_impact['root'] = pd.concat([fi, temp], axis=1)

        self.feature_impact['level1'] = self.feature_impact['root'].groupby('Category').sum().sort_values(by='Impact', ascending=False).reset_index()
        self.feature_impact['level2'] = self.feature_impact['root'].groupby(['Category', 'Category2']).sum().sort_values(by='Impact', ascending=False).reset_index()
        
    def __load_config(self, fullfile):
        
        with open(fullfile, 'rb') as fp:
            config =   yaml.load(fp, Loader=yaml.FullLoader)
            return config
        
    def __load_submarket_map(self):

        fullfile = 'C:\\Users\\kpatel\\analytics-research\\IPG\\config\\submarket_mapping.yaml'
        
        with open(fullfile, 'rb') as fp:
            mapping = yaml.load(fp, Loader=yaml.FullLoader)            
            return mapping
        
    def total_rent(self, x):
        rs = str(x['Lease:Rent Schedule'])
        rs = rs.split(', ')
        
        if rs[0]!='0/0':
            acc_rent = []
            p = []
            for item in rs:
                rent, time = item.split('/')
                rent = float(rent)
                time, period = time.split()
                time = float(time)
                acc_rent.append(rent*time)
                p.append(time)
            return sum(acc_rent)*x['Lease:Transaction Sqft']/12
        else:
            return x['Lease:Starting Rent']*x['Lease:Transaction Sqft']*(x['Lease:Term']/12)
    
    def derived_term(self, x):
        rs = str(x['Lease:Rent Schedule'])
        rs = rs.split(', ')
        
        if rs[0]!='0/0':
            p = []
            for item in rs:
                _, time = item.split('/')
                time, period = time.split()
                time = float(time)
                p.append(time)
            return sum(p)
        else:
            return x['Lease:Term']
            
    def blended_gross_rent(self, x):
        bgr = x['Temp:Lease:Total Rent'] / x['Lease:Transaction Sqft'] / (x['Lease:Term']/12)
        return bgr
    
    def concession_value(self, x):
        rent_first = float(x['Lease:Rent Schedule'].split(', ')[0].split('/')[0])
        if rent_first==0:
            rent_first = x['Lease:Starting Rent']
        return ( (x['Lease:Free Rent']/12)*rent_first + x['Lease:Ti Value Work Value'])*x['Lease:Transaction Sqft']
    
    def concession_psf(self, x):
        return x['Temp:Lease:Concession Value']/x['Lease:Transaction Sqft']/(x['Lease:Term']/12)
    
    def extract_highest_floor(self, txt):
        txt = str(txt)
        x = re.findall(r'\d+', txt)
        x = [int(item) for item in x]
        
        if len(x):
            return max(x)
        else:
            return np.nan
    
    def compute_avg_floor(self, txt):
        txt = str(txt)
        x = re.findall(r'\d+', txt)
        x = [int(item) for item in x]
        
        if len(x):
            return np.mean(x)
        else:
            return np.nan
        
    def multi_floor(self, txt):
        txt = str(txt)
        x = re.findall(r'\d+', txt)
        x = [int(item) for item in x]
        
        if len(x)>1:
            return True
        else:
            return False
    
    def entire_floor(self, txt):
        txt = str(txt)
        if 'Entire' in txt:    
            return True
        else:
            return False        
    
    def years_since_reno(self, x):
        if np.isnan(x['Property:Year Renovated']):
            return (date.today() - date(int(x['Property:Year Built']), 7, 1)).days/365
        else:
            return (date.today() - date(int(x['Property:Year Renovated']),7,1)).days/365

    def get_previous_qtr_metrics(self, qtr_current, year_current, submarket):

        qtr_previous = qtr_current - 1
        if qtr_previous==0:
            qtr_previous = 4
            year = year_current - 1
        else:
            year = year_current
        
        bool_exp = (self.metrics['Market:Quarter']==qtr_previous)&(self.metrics['Market:Year']==year)&(self.metrics['compstak_submarket']==submarket)
        
        prev_qtr = self.metrics[bool_exp]
        
        return {'Market:Previous Quarter': [prev_qtr['Market:Quarter'].iloc[0]],
                'Market:Previous Year': [prev_qtr['Market:Year'].iloc[0]],
                'Market:Previous Vacancy': [prev_qtr['Market:Vacancy'].iloc[0]],
                'Market:Previous Asking Rent': [prev_qtr['Market:Asking Rent'].iloc[0]],
                'Market:Previous Availability': [prev_qtr['Market:Availability'].iloc[0]],
                'Market: Previous Net Absorption': [prev_qtr['Market:Net Absorption'].iloc[0]]}
    
    def generate_visuals(self, outfile='output.png', include_uw=False):
        
        fig = plt.figure(figsize=(30, 20))
        gs = fig.add_gridspec(3,4)
        ax = []
        ax.append(fig.add_subplot(gs[0,:]))
        ax.append(fig.add_subplot(gs[1,0]))
        ax.append(fig.add_subplot(gs[1,1]))
        ax.append(fig.add_subplot(gs[1,2]))
        ax.append(fig.add_subplot(gs[1,3]))
        ax.append(fig.add_subplot(gs[2,0]))
        ax.append(fig.add_subplot(gs[2,1]))
        ax.append(fig.add_subplot(gs[2,2:4]))
        
        ax[0].pie(self.feature_impact['level1']['Impact'], labels=self.feature_impact['level1']['Category'], autopct=lambda p: '{:.1f}%'.format(p))
        ax[0].set_title('Model Discovered Correlations to Starting Rent', fontsize=18)
        ax[0].yaxis.set_major_formatter('{x:1.0f}%')
        ax[0].text(0.925, 0.075, u"R\u00b2={}".format(round(self.score['r2'],2), style='italic'), transform=ax[0].transAxes, fontsize=16, verticalalignment='top')

        COLORS = ['tab:blue', 'tab:orange','tab:green', 'tab:red']
        
        for indx, category in enumerate(['Locational', 'Lease', 'Building', 'Market']):
            temp = self.feature_impact['level2'][self.feature_impact['level2']['Category']==category]
            if category=="Market":
                temp = temp.replace('Net Absorption', 'Submarket\nNet\nAbsorption')
                temp = temp.replace('Vacancy', 'Submarket\nVacancy')
                temp = temp.replace('Availability', 'Submarket\nAvailability')
                temp = temp.replace('MN(A) Net Absorption', 'Manhattan\nNet\nAbsorption')
                temp = temp.replace('MN(A) Vacancy', 'Manhattan\nVacancy')
                temp = temp.replace('MN(A) Availability', 'Manhattan\nAvailability')
            elif category in ['Lease', 'Building', 'Locational']:
                temp = temp.assign(Category2=temp.Category2.apply(lambda x: '\n'.join(x.split(' '))))
                
            temp = temp.sort_values(by='Impact')
            
            # normalize explained variance per category
            temp['Impact'] = temp['Impact']/temp['Impact'].sum()
            
            ax[indx+1].set_title('{} Attributes'.format(category), fontsize=16)
            ax[indx+1].barh(temp['Category2'], temp['Impact'], color=COLORS[indx])
        
        # PDP by Time Since Reno
        # AGE = range(0,26)
        # FLOOR = 15
        COEFF=1.0
        METRIC = 'mae'
        # preds = []
        COLS_TRAIN = list(self.data['train']['input'].columns)
        
        train_target = self.data['train']['target']
        test_target = self.data['test']['target']
        
        ax[5].set_title('Distribution of Training & Test Data', fontsize=14)
        ax[5].hist([train_target, test_target], bins=35, label=['Training Set (90%)', 'Test Set (10%)'], density=True, alpha=0.75)
        ax[5].legend(loc='upper right')
        ax[5].grid()
        
        ax[6].set_title('Distribution of Mean Abs Percentage Error (on Test Data)', fontsize=14)
        ax[6].hist(100*(self.data['test']['target'] - self.data['test']['predictions'])/self.data['test']['target'], bins=35, label='Mean Absolute Pct Error', density=True, alpha=0.75, range=(-30,30))
        ax[6].legend(loc='upper right')
        ax[6].grid()
        
        # PDP by Floor of Occupancy
        preds = []
        fl = list(range(2,31))
        fl.remove(13)
        SF = [7025,41132,41460,41323,41247,40686,41604,29448,37845,16938,15865,14625,16263,16263,16267,16275,16314,16294,16428,16428,16428,16428,16288,16291,16130,16140,16298,14690]
        
        for floor_num, sf in zip(fl,SF):
            x=self.data['all'][COLS_TRAIN]
            x = x[x['Building:Property Id']==290].iloc[0]
            
            # set most recent market KPIs
            x['Market:MN(A) Vacancy'] = 0.1
            x['Market:MN(A) Net Absorption'] = -3488981
            x['Market:MN(A) Availability'] = 0.18
            x['Market:Vacancy'] = .109
            x['Market:Net Absorption'] = -372911
            x['Market:Availability'] = .164
            
            # set 452 building attributes
            x['Building:Years Since Reno'] = 0
            x['Building:Age'] = 0
            x['Lease:Transaction Sqft'] = sf
            x['Lease:Execution Year'] = 2022
            x['Lease:Floors:Max'] = floor_num
            x['Lease:Floors:Avg'] = floor_num
            x['Lease:Relative Position'] = floor_num/30
            x['Lease:Term'] = 11*12
            x['Lease:Multi-Floor'] = True
            x['Lease:Entire Floor'] = True
        
            x = pd.DataFrame(x)
            y = self.regr.predict(x.values.T)
            preds.append(COEFF*y[0])
        
        preds_smooth = gaussian_filter1d(preds, sigma=0.75)
        ax[7].plot(fl, preds_smooth, linewidth=2)
        ax[7].fill_between(fl, preds_smooth + self.score[METRIC], preds_smooth - self.score[METRIC], alpha=0.125)
        
        if include_uw:
            uw = [85,85,85,85,85,85,85,85,115,125, 105, 105, 120, 120, 120, 120, 120, 120, 125, 125, 125, 125, 125, 130, 130, 130, 130, 130]
            ax[7].scatter(fl, uw, marker='o', color='orange')
        
        ax[7].grid()
        ax[7].set_title('452 5th Ave: Starting Rents', fontsize=14)
        ax[7].set_xlabel('Floor #')
        
        ax[7].set_ylabel('{} ($ PSF)'.format(self.target.lstrip('Lease:')))
        ax[7].xaxis.set_major_locator(MaxNLocator(integer=True))
        ax[7].yaxis.set_major_formatter('${x:1.0f}')
        
        fig.savefig(outfile)        