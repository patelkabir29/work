import requests
import pandas as pd

# Function to get metadata and create a mapping for DP05 variable codes to descriptions
def get_variable_mapping(year):
    metadata_url = f"https://api.census.gov/data/{year}/acs/acs5/profile/variables.json"

    # Fetch the metadata
    response = requests.get(metadata_url)
    if response.status_code == 200:
        # Parse the metadata JSON
        metadata = response.json()
        variables = metadata["variables"]

        # Filter to include only variables from the DP05 group
        dp05_variables = {
            key: value["label"]
            for key, value in variables.items()
            if key.startswith("DP05")  # Ensure the variable starts with 'DP05'
        }

        # Convert the DP05 variables into a DataFrame for better visualization and processing
        df_rename_cols = pd.DataFrame(
            list(dp05_variables.items()), columns=["Variable Code", "Description"]
        )

        # Keep rows where the part after the underscore in "Variable Code" has 5 characters and ends with 'E'
        df_rename_cols = df_rename_cols[
            df_rename_cols["Variable Code"].str.split("_").str[1].str.len() == 5
        ]
        df_rename_cols = df_rename_cols[
            df_rename_cols["Variable Code"].str.split("_").str[1].str.endswith("E")
        ]

        # Create a simplified description by splitting on "!!" and taking the last part
        df_rename_cols["Description2"] = df_rename_cols["Description"].str.split("!!").str[-1]

        # Create and return the mapping dictionary
        return df_rename_cols.set_index("Variable Code")["Description2"].to_dict()
    else:
        print(f"Error fetching metadata for year {year}: {response.status_code}")
        return {}

# Function to get counties in a given state
def get_counties(state, year):
    data_url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get=group(DP05)&ucgid=pseudo(0400000US{state}$0500000)"
    response = requests.get(data_url)
    if response.status_code == 200:
        data = response.json()
        df_data = pd.DataFrame(data[1:], columns=data[0])
        return df_data[["NAME", "GEO_ID"]]
    else:
        print(f"Error fetching counties for state {state} in year {year}: {response.status_code}")
        return pd.DataFrame()

# Function to get zip codes in a given state and county
def get_zip_codes(state, county_code, year):
    data_url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get=group(DP05)&ucgid=pseudo({county_code}$8600000)"
    response = requests.get(data_url)
    if response.status_code == 200:
        data = response.json()
        df_data = pd.DataFrame(data[1:], columns=data[0])
        return df_data
    else:
        print(f"Error fetching zip codes for county {county_code} in state {state} for year {year}: {response.status_code}")
        return pd.DataFrame()

# Function to aggregate data for all zip codes in all counties of a state
def collect_zipcode_data(state, year, variable_code_to_description):
    # Get all counties in the state
    counties_df = get_counties(state, year)
    if counties_df.empty:
        return pd.DataFrame()

    final_df = pd.DataFrame()

    # Iterate over each county in the state
    for index, row in counties_df.iterrows():
        county = row['NAME']
        county_code = row['GEO_ID']

        if county == "New York County, New York":
            print(f"Fetching data for county: {county_code} - {county}")

            # Get zip codes for the county
            zip_codes_df = get_zip_codes(state, county_code, year)
            if not zip_codes_df.empty:
                # Filter for relevant DP05 columns
                temp = zip_codes_df.columns
                temp = temp[temp.str.startswith("DP05")]
                temp = temp[temp.str.split("_").str[1].str.len() == 5]
                temp = temp[temp.str.split("_").str[1].str.endswith("E")]
                temp = temp.append(zip_codes_df.columns[~zip_codes_df.columns.str.startswith("DP05")])

                # Filter and rename columns using the variable mapping
                zip_codes_df = zip_codes_df[temp]
                zip_codes_df = zip_codes_df.rename(columns=variable_code_to_description)
                
                # rename NAME column to Zip Code and remove ZCTA5 prefix
                zip_codes_df = zip_codes_df.rename(columns={"NAME": "Zip Code"})
                zip_codes_df["Zip Code"] = zip_codes_df["Zip Code"].str.replace("ZCTA5 ", "")

                # Add county and state information
                zip_codes_df['County'] = county.split(",")[0]
                zip_codes_df['County Code'] = county_code
                zip_codes_df['State Number'] = state
                zip_codes_df['State'] = county.split(",")[1].strip()
                zip_codes_df['Year'] = year

                

                # Remove duplicate columns
                zip_codes_df = zip_codes_df.loc[:, ~zip_codes_df.columns.duplicated()]

                final_df = pd.concat([final_df, zip_codes_df], ignore_index=True)

    return final_df

# Main script
if __name__ == "__main__":
    year_start = 2010  # Replace with the desired year
    state = "36"   # Replace with the state FIPS code
    year_end = 2024 # Replace with the current year

    combined_df = pd.DataFrame()

    for year in range(year_start, year_end + 1):
        variable_code_to_description = get_variable_mapping(year)
        if variable_code_to_description:
            print(f"Fetching data for year {year}")
            zipcode_data = collect_zipcode_data(state, year, variable_code_to_description)
            print(zipcode_data.head())
            combined_df = pd.concat([combined_df, zipcode_data], ignore_index=True)
        else:
            print("Failed to retrieve variable mapping.")
    
    # Save the combined data to a CSV file
    combined_df = combined_df[["Total population", "Zip Code", "County", "State Number", "State", "Year"]]
    combined_df.to_csv(f"Population_by_zipcode_{year_start}_to_{year_end}.csv", index=False)
