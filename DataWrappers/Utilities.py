# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 11:14:32 2021

@author: kpatel
"""
from functools import reduce
import yaml

def deep_get(dictionary, keys, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)

class Configuration(object):
    def __init__(self, config_file):
        with open(config_file, 'rb') as fp:
            self.params = yaml.load(fp, Loader=yaml.FullLoader)