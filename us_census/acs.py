# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 15:36:55 2025

@author: kpatel
"""

import requests
import pandas as pd 

data_df = pd.DataFrame()

for YEAR in range(2011,2024,1):
    print(YEAR)

    # URL to get metadata for DP05 variables
    metadata_url = f"https://api.census.gov/data/{YEAR}/acs/acs5/profile/variables.json"
    
    # Fetch the metadata
    response = requests.get(metadata_url)
    
    if response.status_code == 200:
        # Extract the variables metadata
        metadata = response.json()
        variables = metadata["variables"]
    
        # Filter for DP05 group variables
        dp05_variables = {
            key: value["label"]
            for key, value in variables.items()
            if key.startswith("DP05")
        }
    
        # Convert to DataFrame for better visualization (optional)
        df_rename_cols = pd.DataFrame(list(dp05_variables.items()), columns=["Variable Code", "Description"])
    
        # Save to CSV or print
        #df_rename_cols.to_csv(f"DP05_column_mapping_{YEAR}.csv", index=False)    
    else:
        print(f"Error fetching metadata: {response.status_code} - {response.text}")
    
    
    # Define the API URL and parameters
    url = f"https://api.census.gov/data/{YEAR}/acs/acs5/profile"
    params = {
        "get": "group(DP05)",
        "for": "county subdivision:*",
        "in": "county:183+state:37"
        }
    
    # Make the request
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Extract header (first row) and data (remaining rows)
        header = data[0]
        rows = data[1:]
        
        # Convert to Pandas DataFrame
        df = pd.DataFrame(rows, columns=header)        
        df.rename(columns=dp05_variables, inplace=True)
        df = df[[col for col in df.columns if 'Percent!!' not in col]]
        df = df[[col for col in df.columns if 'DP' not in col]]
        
        df = df.assign(NAME=df.NAME.apply(lambda x: x.split(',')[0].replace('township','')))
        # Display the DataFrame
        print(df)
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    id_vars = ["NAME", "state", "county", "county subdivision", "GEO_ID"]
    value_vars = [col for col in df.columns if col not in id_vars]
    tidy_df = df.melt(id_vars=id_vars, value_vars=value_vars, 
                          var_name="Tag", value_name="Value")
    
    tidy_df = tidy_df.assign(Year=YEAR)
    
    data_df = pd.concat([data_df,tidy_df])