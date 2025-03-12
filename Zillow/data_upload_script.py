import re
import pandas as pd
import snowflake.connector
import os

# ENTER YOUR SNOWFLAKE CREDENTIALS HERE 
USER='--'
PASSWORD='--'
ACCOUNT='--'
WAREHOUSE='--'
DATABASE ='--'

def create_table(table_name, locality):
    # Snowflake connection parameters
    conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE
        )


    # Create a cursor object
    cur = conn.cursor()

    create_schema_sql =  f"CREATE SCHEMA IF NOT EXISTS ZILLOW.TRANSFORMED_DATA;"

    cur.execute(create_schema_sql)
    conn.commit()
    query_add = ""

    #zhvf
    if locality == "County":
        query_add += """
        STATENAME VARCHAR,
        STATE VARCHAR,
        METRO VARCHAR,
        STATECODEFIPS VARCHAR,
        MUNICIPALCODEFIPS VARCHAR,"""
    elif locality == "City":
        query_add += """
        STATENAME VARCHAR,
        STATE VARCHAR,
        METRO VARCHAR,
        COUNTYNAME VARCHAR,"""
    elif locality == "Zip":
        query_add += """
        STATENAME VARCHAR,
        STATE VARCHAR,
        CITY VARCHAR,
        METRO VARCHAR,
        COUNTYNAME VARCHAR,"""
    elif locality == "Neighborhood":
        query_add += """
        STATENAME VARCHAR,
        STATE VARCHAR,
        CITY VARCHAR,
        METRO VARCHAR,
        COUNTYNAME VARCHAR,"""


    if "zhvf" in table_name:
        query_add += "BASEDATE DATE,"
    
    # Create or replace table in Snowflake
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS ZILLOW.TRANSFORMED_DATA.\"{table_name.upper()}\" (
        REGIONID VARCHAR,
        SIZERANK INTEGER,
        REGIONNAME VARCHAR,
        REGIONTYPE VARCHAR,
        {query_add}
        HOUSETYPE VARCHAR,
        TIER VARCHAR,
        LOCALITY VARCHAR,
        BEDROOM VARCHAR,
        PERIOD DATE,
        VALUE FLOAT
    );
    """
    cur.execute(create_table_query)
    cur.close()
    conn.close()
    print("Table created successfully.")

def upload_data(table_name, transformed_df):
    conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE
        )
    
    transformed_df.columns = transformed_df.columns.str.upper()
    print(transformed_df.columns)

    from snowflake.connector.pandas_tools import write_pandas

    cursor = conn.cursor()

    # Get Existing Periods from Snowflake
    if "zhvf" in table_name:
        query = f"SELECT DISTINCT BaseDate FROM ZILLOW.TRANSFORMED_DATA.{table_name.upper()};"
    else:
        query = f"SELECT DISTINCT Period FROM ZILLOW.TRANSFORMED_DATA.{table_name.upper()};"
    
    cursor.execute(query)
    existing_periods = {row[0] for row in cursor.fetchall()}

    # Filter Out Already Existing Periods from the Excel Data
    if "zhvf" in table_name:
        df_filtered = transformed_df[~transformed_df["BASEDATE"].isin(existing_periods)]  # Keep only new periods
    else:
        df_filtered = transformed_df[~transformed_df["PERIOD"].isin(existing_periods)]  # Keep only new periods
    # Append New Data
    if not df_filtered.empty:  # Check if there's new data to insert
        success, num_chunks, num_rows, _ = write_pandas(conn, df_filtered, table_name = table_name.upper(), schema="TRANSFORMED_DATA", database="ZILLOW")
        if success:
            print(f"✅ Successfully uploaded {num_rows} rows in {num_chunks} chunks!")
        else:
            print("❌ Upload failed.")
    else:
        print("⚠️ No new data to upload. All periods already exist in Snowflake.")

    cursor.close()
    conn.close()


current_dir = os.path.expanduser(r"C:\Users\patel\Downloads\zillow")
print(current_dir + '\n')

words = ['zhvi', 'zhvf', 'zori']
files = []

for _file in os.listdir(current_dir):
    if any(word in _file for word in words):
        files.append(_file)

if files:
    print(f"{len(files)} files were found for the specified date")
else:
    print("ERROR: Couldn't find any file with the current date")

# Load the Excel/CSV file
# file_path = r"C:\Users\patel\Downloads\Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"  # Replace with your actual file path

n = len(files)
for csv_file in files:
    house_types = ["sfr", "mfr", "condo", "sfrcondo", "sfrcondomfr"]
    tiers = ["0.33_0.67", "0.67_1.0", "0.0_0.33"]
    localities = ["Zip", "Metro", "National", "County", "City", "Neighborhood"]
    bedrooms = ["bdrmcnt_1", "bdrmcnt_2", "bdrmcnt_3", "bdrmcnt_4", "bdrmcnt_5"]

    house_type = ""
    house_type = next((word for word in house_types if f"_{word}_" in csv_file), "Not Specified")
    
    tier = ""
    tier = next((word for word in tiers if f"_{word}_" in csv_file), "Not Specified")

    locality = ""
    locality = next((word for word in localities if f"{word}_" in csv_file), "Not Specified")

    bedroom = ""
    bedroom = next((word for word in bedrooms if f"_{word}_" in csv_file), "Not Specified")

    if "zhvi" in csv_file:
        table_name = "zhvi"+f"_{locality}"
        print("zhvi found")
    elif "zhvf" in csv_file:
        table_name = "zhvf"+f"_{locality}"
        print("zhvf found")
    elif "zori" in csv_file:
        table_name = "zori"+f"_{locality}"
        print("zori found")
    elif "zordi" in csv_file:
        table_name = "zordi"+f"_{locality}"
        print("zordi found")
    elif "invt_fs" in csv_file:
        table_name = "invt_fs"+f"_{locality}"
        print("invt_fs found")
    elif "zorf" in csv_file and "sfr" in csv_file:
        table_name = "zorf_sfr"+f"_{locality}"
        print("zorf_sfr found")
    elif "zorf" in csv_file and "mfr" in csv_file:
        table_name = "zorf_mfr"+f"_{locality}"
        print("zorf_mfr found")
    else:
        table_name = "other"
        locality = "other"

    try:
        csv_file_path = os.path.join(current_dir, csv_file)
        df = pd.read_csv(csv_file_path)
        print("File loaded successfully.")
    except FileNotFoundError:
        print(f"Error: Couldn't find the file specified: '{csv_file}'.")
        exit()

    #adding columns for house type and tier
    df["Housetype"] = f"{house_type}"
    df["Tier"] = f"{tier}"
    df["Locality"] = f"{locality}"
    df["Bedroom"] = f"{bedroom}"

    print("Additional columns added successfully.")

    # Date columns
    date_columns = [col for col in df.columns if col.startswith('20')]

    # Columns that are not date columns
    non_date_columns = [col for col in df.columns if col not in date_columns]


    # Melt the DataFrame: turn date columns into rows
    transformed_df = pd.melt(df, id_vars=non_date_columns,
                            var_name="Period", value_name=f"Value")
    print("Data transformation complete.")

    # Save the transformed data to a CSV file
    output_file = f"transformed_{csv_file}"
    transformed_df.to_csv(output_file, index=False)

    print(f"Transformed data saved to {output_file}")

    # transformed_filepath = r"C:\Users\patel\transformed_data.csv"
    # transformed_df = pd.read_csv(transformed_filepath)

    create_table(table_name, locality)
    upload_data(table_name, transformed_df)

    print(f"{len(files)-n+1}/{len(files)} files processed susccessfully.")
    n -= 1

