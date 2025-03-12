"""
Reference for pandas and snowflake connector: 
    https://docs.snowflake.com/developer-guide/python-connector/python-connector-pandas?_fsi=cmzgEm2Y

"""
import snowflake.connector
import json
import pandas as pd


# Define a custom function to parse the Apr-24 format
def custom_date_parser(date_str):
    try:
        # First, try to parse the standard datetime formats
        return pd.to_datetime(date_str, errors='raise')
    except ValueError:
        try:
            # If it fails, try to parse the Apr-24 format assuming the current year
            return pd.to_datetime('1-'+ date_str, format='%d-%m-%Y')
        except ValueError:
            # Return NaT if both parsing attempts fail
            return pd.NaT
        
def read_snowflake_table(config_path, table_name):
    """
    Function to read a table from snowflake and
    Return it as a pandas dataframe.
    """
    # Read the config file
    with open(config_path) as f:
        config = json.load(f)

    # Gets the version
    ctx = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        database=config['database'], #"IPG"
        )
    cs = ctx.cursor()
    try:
        cs.execute(f"SELECT * FROM {table_name}")
        df = cs.fetch_pandas_all()
    finally:
        cs.close()
    ctx.close()

    return df

def compare_lease_comps_snowflake(df, config_path):
    """
    Function to compare a pandas dataframe to a snowflake table
    and return the rows that are not in the snowflake table.
    """
    table = "IPG.MARKETVIEW_DEV.LEASE_COMPS"
    snowflake_df = read_snowflake_table(config_path, table)

    # Save the snowflake dataframe to a csv file
    snowflake_df.to_csv(r"D:\UofT\Work\IPG\data\market_view\test\snowflake_df.csv", index=False)

    snowflake_df['EXECUTION_DATE'] = pd.to_datetime(snowflake_df['EXECUTION_DATE']).dt.strftime('%d-%m-%Y')
        
    # Find new rows using reference table already in snowflake
    # Use three columns to identify new rows index
    df = df.reset_index(drop=True)
    snowflake_df = snowflake_df.reset_index(drop=True)

    new_leases = df[~df[['ADDRESS', 'EXECUTION_DATE', 'BUILDING_SF']].apply(tuple, 1).isin(snowflake_df[['ADDRESS', 'EXECUTION_DATE', 'BUILDING_SF']].apply(tuple, 1))]
    
    return new_leases


def extract_new_leases(config_path, df):
    """
    Function to read a textract csv file and
    Return it as a pandas dataframe for snowflake.
    """  

    diff_df = pd.DataFrame(columns=[
        "ADDRESS",
        "EXECUTION_DATE",
        "BUILDING_SF"
    ])

    # Map the columns to the correct names
    mapping = {
        "Address": "ADDRESS",
        "Sign Date": "EXECUTION_DATE",
        "SF Leased": "BUILDING_SF"
    }

    # Rename the columns in the dataframe to match the snowflake table
    df.rename(columns=mapping, inplace=True)

    # Remove any commas from the building square footage
    df['BUILDING_SF'] = df['BUILDING_SF'].str.replace(',', '')
    # Convert the string with double quotes to an integer
    df['BUILDING_SF'] = df['BUILDING_SF'].str.replace('"', '').astype(int)
    
    # Remove trailing whitespace from all columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Copy the columns to the snowflake dataframe
    for col in diff_df.columns:
        diff_df[col] = df[col]

    # Extract new leases
    new_leases = compare_lease_comps_snowflake(diff_df, config_path)

    return new_leases


def update_initial_lease_comps(config_path, lease_path, output_path):
    """
    Function to save only the new leases from the Costar data
    """

    df = pd.read_csv(lease_path)

    # Custom date parser
    df['Sign Date'] = df['Sign Date'].apply(custom_date_parser)
    df['Sign Date'] = df['Sign Date'].dt.strftime('%d-%m-%Y')
    df['Start Date'] = df['Start Date'].apply(custom_date_parser)
    df['Start Date'] = df['Start Date'].dt.strftime('%d-%m-%Y')

    new_leases = extract_new_leases(config_path, df)

    # Keep rows that are in new_leases
    df = df[df.index.isin(new_leases.index)]

    # Save the new leases
    df.to_csv(output_path, index=False)

    return df


def new_lease_comps_snowflake(new_lease_path, output_path):
    """
    Function to read a textract csv file and
    Return it as a pandas dataframe for snowflake.

    BEFORE USING THIS METHOD,
    PLEASE MAKE SURE THAT THE COSTAR DF is CLEANED, especially the brokers.
    """
    df = pd.read_csv(new_lease_path, dayfirst=True)

    snowflake_df = pd.DataFrame(columns=[
        "ADDRESS",
        "EXECUTION_DATE",
        "COMMENCEMENT_DATE",
        "BUILDING_SF",
        "PARKING_SF",
        "TERM",
        "FLOOR",
        "TENANT_BROKERAGE",
        "LANDLORD_BROKERAGE",
        "ASKING_RENT",
        "SOURCE",
        "TENANT_BROKERS",
        "LANDLORD_BROKERS",
        "TENANT",
        "LEASE_ID",
        "CREATED_DATE",
    ])

    # Map the columns to the correct names
    mapping = {
        "Address": "ADDRESS",
        "Sign Date": "EXECUTION_DATE",
        "Start Date": "COMMENCEMENT_DATE",
        "SF Leased": "BUILDING_SF",
        "Term": "TERM",
        "Floor": "FLOOR",
        "Tenant Rep Company": "TENANT_BROKERAGE",
        "Leasing Rep Company": "LANDLORD_BROKERAGE",
        "Asking Rent/SF/Yr": "ASKING_RENT",        
        "Tenant Rep Contact": "TENANT_BROKER",
        "Leasing Rep Contact": "LANDLORD_BROKER",
        "Tenant": "TENANT",
    }

    # Rename the columns in the dataframe to match the snowflake table
    df.rename(columns=mapping, inplace=True)

    # Add the missing columns
    df['PARKING_SF'] = 0
    df['SOURCE'] = 'Costar'

    # Copy the columns to the snowflake dataframe
    # Get the columns from the snowflake dataframe tgaht are in the df
    cols = [col for col in snowflake_df.columns if col in df.columns]
    for col in cols:
        snowflake_df[col] = df[col]

    # Add the created date
    snowflake_df['CREATED_DATE'] = pd.to_datetime('today')
    snowflake_df['CREATED_DATE'] = snowflake_df['CREATED_DATE'].dt.strftime('%d-%m-%Y')

    # Process the data to be in the correct format
    snowflake_df = snowflake_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    snowflake_df = snowflake_df.apply(lambda x: x.str.replace('\n', ' ') if x.dtype == "object" else x)
    snowflake_df = snowflake_df.apply(lambda x: x.str.strip(',') if x.dtype == "object" else x)
    snowflake_df = snowflake_df.apply(lambda x: x.str.split(',').str.join(' ') if x.dtype == "object" else x)
    snowflake_df['ASKING_RENT'] = snowflake_df['ASKING_RENT'].str.replace('$', '').str.replace(',', '').astype(float)

    # Create the lease_id
    snowflake_df['LEASE_ID'] = snowflake_df['ADDRESS'] + snowflake_df['EXECUTION_DATE'].astype(str) + snowflake_df['BUILDING_SF'].astype(str)

    # Save the new leases
    snowflake_df.to_csv(output_path, index=False)

    return snowflake_df

def update_properties_snowflake(config_path, new_lease_path, output_path):
    df = pd.read_csv(new_lease_path)

    # read the properties table from snowflake
    snowflake_refer_df = read_snowflake_table(config_path, "IPG.MARKETVIEW_DEV.PROPERTIES")

    # Use address not in snowflake_refer_df
    new_properties = df[~df['ADDRESS'].isin(snowflake_refer_df['ADDRESS'])]

    snowflake_prop_df = pd.DataFrame(columns=[
        'ADDRESS',
        'STREET_NUM',
        'STREET_NAME',
        'CITY',
        'SUBMARKET',
        'BOROUGH',
        'YEAR_BUILT',
        'RBA',
        'FAR',
        'LATITUDE',
        'LONGITUDE',
        'CLASS',
        'STORIES',
        'TENANCY',
        'PARKING_SPACES',
        'CEILING_HT',
        'DRIVE_INS',
        'DOCKS',
        'CONSTRUCTION_MAT',
        'POWER',
        'SOURCE_LINK',
        'CREATED_DATE'
    ])

    # Map the columns to the correct names
    mapping = {
        "Address": "ADDRESS",
        "City": "CITY",
        "Submarket": "SUBMARKET"
    }

    # Rename the columns in the dataframe to match the snowflake table
    new_properties.rename(columns=mapping, inplace=True)

    # Rename the columns in the dataframe to match the snowflake table
    cols = [col for col in snowflake_prop_df.columns if col in new_properties.columns]
    for col in cols:
        snowflake_prop_df[col] = new_properties[col]

    # Print snowflake_prop_df
    print(snowflake_prop_df)

    # Keep Borough based on text. If Queens in submarket, then borough is Queens, bronx, brooklyn, 'staten island'
    snowflake_prop_df['BOROUGH'] = snowflake_prop_df['SUBMARKET'].apply(lambda x: 'Queens' if 'Queens' in x else 'Bronx' if 'Bronx' in x else 'Brooklyn' if 'Brooklyn' in x else 'Staten Island' if 'Staten Island' in x else 'Unkwnown')

    # Add the created date
    snowflake_prop_df['CREATED_DATE'] = pd.to_datetime('today').strftime('%d-%m-%Y')

    # Save the new properties
    snowflake_prop_df.to_csv(output_path, index=False)




    
