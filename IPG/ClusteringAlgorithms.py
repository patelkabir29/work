# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 19:59:40 2021

@author: kpatel
"""
from sklearn.cluster import KMeans
from sklearn_extra.cluster import KMedoids
from scipy.cluster.vq import whiten
import folium
from folium import Map, Marker, Choropleth, Polygon, LayerControl
from folium.plugins import HeatMap
import panel as pn
import numpy as np

class Clustering(object):
    
    def __init__(self, leases, method='KMedoids', n_clusters=10):
        
        self.cols = ['latitude',
                     'longitude',
                     'net_effective_rent',
                     'transaction_sqft']
                
        self.leases = leases.dropna(subset=self.cols).reset_index()
        
        print('Applying k-Medoids algorithm, solving for {} clusters'.format(n_clusters))
        
        if method=='KMedoids':
            self.km = KMedoids(n_clusters=n_clusters, random_state=0).fit(whiten(self.leases[self.cols]))
        elif method=='KMeans':
            self.km = KMeans(n_clusters=n_clusters, random_state=0).fit(whiten(self.leases[self.cols]))
                
    def generate_map(self, metric):
        
        m = Map(location=list(self.leases[['latitude','longitude']].mean()), zoom_start=16)
        
        grps = self.leases.groupby('street_address')
        
        for index, row in self.leases.iterrows():
            cluster_label = self.km.labels_[index] + 1
            
            folium.Circle(location=(row['latitude'], row['longitude']),
                          tooltip='<b>{}</b><br>{}<br>NER: ${}<br>SF: {}<br>Cluster #{}'.format(row['street_address'], row['tenant_name'], row['net_effective_rent'], row['transaction_sqft'], cluster_label),
                          radius=5,
                          fill=True).add_to(m)
            
        geo_std = np.std(self.leases[['latitude','longitude']])
        
        popup_labels = {'net_effective_rent': 'NER',
                        'lease_term': 'Lease Term',
                        'transaction_sqft': 'SF Leased'}
        
        for label in set(self.km.labels_):
            
            cluster = self.km.cluster_centers_[label]
            l_temp = self.leases[self.km.labels_==label]
            l_temp = l_temp.assign(cluster=label+1)
            
            ld = l_temp.describe()
            
            hm = HeatMap(
                    list(zip(l_temp.latitude.values, l_temp.longitude.values, l_temp[metric].values.astype(float))),
                    min_opacity=0.2,
                    radius=25,
                    blur=5,
                    max_zoom=18,
                    name='Cluster {}: ${} +/-${} ({})'.format(label+1, round(ld['net_effective_rent']['mean'],0), round(ld['net_effective_rent']['std'],0), len(l_temp)),
                    control=True,
                    show=False
                    )
            
            m.add_child(hm)
            
            popup = ''
            
            for metric_name in ['net_effective_rent']:
                popup = popup + popup_labels[metric_name] + ': ' + str(round(ld[metric_name]['mean'],0)) + '\n'
                tooltip = 'Cluster {}: ${} +/- ${}'.format(label+1, round(ld['net_effective_rent']['mean'],0), round(ld['net_effective_rent']['std'],0))
                
                mk = folium.Marker([cluster[0]*geo_std.latitude, cluster[1]*geo_std.longitude], popup=popup, tooltip=tooltip, color="#FF0000")
                m.add_child(mk)
            
        LayerControl(collapsed=False).add_to(m)
        
        pane = pn.pane.plot.Folium(m, sizing_mode='stretch_both')
        
        return pane
    
    