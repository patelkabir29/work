# -*- coding: utf-8 -*-
"""
Created on Mon Sep 18 14:36:43 2023

@author: keni.patel
"""

import requests

def fetch_data_from_soda(domain, dataset_identifier, file_format="json", token=None, query_params=None):
    """
    Fetch data from a SODA API endpoint.
    
    Parameters:
    - domain: The domain of the SODA instance (e.g., "data.cityofnewyork.us").
    - dataset_identifier: The unique identifier for the dataset.
    - file_format: The desired file format for the dataset (default is "json").
    - token: (Optional) Your SODA API token.
    - query_params: (Optional) A dictionary of query parameters to refine the dataset.
    
    Returns:
    - Data from the dataset in the specified format.
    """
    
    base_url = f"https://{domain}/resource/{dataset_identifier}.{file_format}"
    
    headers = {}
    if token:
        headers["X-App-Token"] = token
    
    response = requests.get(base_url, headers=headers, params=query_params)
    
    if response.status_code == 200:
        if file_format == "json":
            return response.json()
        else:
            return response.text
    else:
        response.raise_for_status()

# Example usage:
domain = "data.ny.gov"
dataset_identifier = "w4pv-hbkt"
file_format = "csv"
api_token = None  # If you have an API token, replace None with your token as a string
query_params = {
    "$limit": 100,  # Limit the number of results returned
    # Add any additional query parameters here
}

data = fetch_data_from_soda(domain, dataset_identifier, file_format, api_token, query_params)
print(data)