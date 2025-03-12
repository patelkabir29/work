# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 10:29:02 2022

@author: kpatel
"""

from MachineLearning import OfficeModel
import shap
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from scipy.ndimage.filters import gaussian_filter1d
from sklearn.mixture import GaussianMixture
import scipy.stats as stats
import math
import numpy as np

cf = 'starting_rent.yaml' #, 'ner.yaml', 'concession.yaml']

m = OfficeModel(config_file='C:\\Users\\kpatel\\analytics-research\\IPG\\config\\{}'.format(cf))
m._load_data()
m._feature_engineering()

sr=m.data['all']['Lease:Starting Rent']
sr.hist(bins=43)

gmm = GaussianMixture(n_components=2).fit(np.array(sr).reshape(-1,1))
gmm.weights_
mu_v = [x[0] for x in gmm.means_]
sigma_v = [np.sqrt(x[0][0]) for x in gmm.covariances_]

gmm_x = np.linspace(sr.min(),sr.max(),100)
gmm_y = np.exp(gmm.score_samples(gmm_x.reshape(-1,1)))
    
# Plot histograms and gaussian curves
fig = plt.figure(figsize=(17, 11))
gs = fig.add_gridspec(1,1)
ax = []
ax.append(fig.add_subplot(gs[0,0]))

#ax[0].hist(sr, bins=43, density=True, label='True Data')

indx=1
pdf = []
for mu, sigma, w in zip(mu_v, sigma_v, gmm.weights_):
    #x = np.linspace(mu - (mu/sigma)*sigma, sr.max(), 100)
    x = np.linspace(0, 225, 225)
    pdf.append(w*stats.norm.pdf(x, mu, sigma))
    ax[0].plot(x, pdf[indx-1], '--', color='black', lw=1, label='Component {}'.format(indx))
    indx+=1

indx_min = abs(pdf[0][75:125]-pdf[1][75:125]).argmin()
indx_min = (75-1) + indx_min

ax[0].fill_between(x[0:indx_min+1], pdf[1][0:indx_min+1], 0, color='red', alpha=0.25)
ax[0].fill_between(x[indx_min:225], pdf[0][indx_min:225], 0, color='red', alpha=0.25)

ax[0].plot(gmm_x, gmm_y, color="crimson", lw=2, label="Mixture")
    
ax[0].set_ylabel('Probability Density', fontsize=14)
ax[0].set_xlabel("Starting Rent (PSF)", fontsize=14)
ax[0].tick_params(axis='x', labelsize=14)
ax[0].tick_params(axis='y', labelsize=14)
ax[0].xaxis.set_major_formatter('${x:1.0f}')
ax[0].legend(fontsize=14)

# Estimate the Half Moment on the left and right sides of the asymmetric distribution
data=m.data['all']
sr = data['Lease:Starting Rent']
dl = sr[sr<sr.mean()]-sr.mean()
sigma_l = np.sqrt((dl*dl).sum()/dl.shape[0])

dr = sr[sr>sr.mean()]-sr.mean()
sigma_r = np.sqrt((dr*dr).sum()/dr.shape[0])