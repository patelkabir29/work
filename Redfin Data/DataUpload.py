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

def create_table(table_name, columns):
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

    create_schema_sql =  f"CREATE SCHEMA IF NOT EXISTS REDFIN.DATA;"

    cur.execute(create_schema_sql)
    conn.commit()

    add_column_sql = ""

    included_columns = ['period_begin', 'period_end', 'period_duration', 'region_type', 'region_type_id', 'table_id', 'is_seasonally_adjusted', 'region', 'city', 'state', 'state_code', 'property_type', 'property_type_id', 'parent_metro_region', 'parent_metro_region_metro_code', 'last_updated']
    for col in columns:
        if col not in included_columns:
            add_column_sql += f"{col.upper()} FLOAT, "

    
    # Create or replace table in Snowflake
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS REDFIN.DATA.\"{table_name.upper()}\" (
        period_begin DATE,
        period_end DATE,
        period_duration INTEGER,
        region_type VARCHAR,
        region_type_id VARCHAR,
        table_id VARCHAR,
        is_seasonally_adjusted VARCHAR,
        region VARCHAR,
        city VARCHAR,
        state VARCHAR,
        state_code VARCHAR,
        property_type VARCHAR,
        property_type_id VARCHAR,
        parent_metro_region VARCHAR,
        parent_metro_region_metro_code VARCHAR,
        {add_column_sql}
        last_updated DATE);"""
    
    cur.execute(create_table_query)
    cur.close()
    conn.close()
    print("✅ Table created successfully.")

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

    if not transformed_df.empty:  # Check if there's new data to insert
        success, num_chunks, num_rows, _ = write_pandas(conn, transformed_df, table_name = table_name.upper(), schema="DATA", database="REDFIN")
        if success:
            print(f"✅ Successfully uploaded {num_rows} rows in {num_chunks} chunks!")
        else:
            print("❌ Upload failed.")
    else:
        print("⚠️ No data to upload.")

    cursor.close()
    conn.close()


current_dir = os.path.expanduser(r"C:\Users\patel\Downloads\Redfin")
print(current_dir + '\n')

word = 'redfin_data'
files = []

for _file in os.listdir(current_dir):
    if word in _file:
        files.append(_file)

if files:
    print(f"{len(files)} files were found for the specified date")
else:
    print("ERROR: Couldn't find any file with the current date")

# Load the Excel/CSV file
# file_path = r"C:\Users\patel\Downloads\Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"  # Replace with your actual file path

n = len(files)
for csv_file in files:
    
    table_name = "Redfin_Data_ZIP"

    try:
        csv_file_path = os.path.join(current_dir, csv_file)
        df = pd.read_csv(csv_file_path)
        print("File loaded successfully.")
    except FileNotFoundError:
        print(f"Error: Couldn't find the file specified: '{csv_file}'.")
        exit()

    # Columns to keep as is
    # columns = ('period_begin', 'period_end', 'period_duration', 'region_type', 'region_type_id', 'table_id', 'is_seasonally_adjusted', 'region', 'city', 'state', 'state_code', 'property_type', 'property_type_id', 'parent_metro_region', 'parent_metro_region_metro_code', 'last_updated')

    # # Melt the DataFrame: turn date columns into rows
    # transformed_df = pd.melt(df, id_vars=columns, var_name="Metrics", value_name="Value")
    # print("Data transformation complete.")

    # Save the transformed data to a CSV file
    # output_file = f"transformed_{csv_file}"
    # transformed_df.to_csv(output_file, index=False)

    # print(f"Transformed data saved to {output_file}")

    columns = [col for col in df.columns]

    create_table(table_name, columns)
    upload_data(table_name, df)

    # print(f"{len(files)-n+1}/{len(files)} files processed susccessfully.")
    # n -= 1

