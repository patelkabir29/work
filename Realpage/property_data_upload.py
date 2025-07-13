import re
import pandas as pd
import snowflake.connector
import os

# ENTER YOUR SNOWFLAKE CREDENTIALS HERE 
USER = '--'
PASSWORD = '--'
ACCOUNT = '--'
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

    create_schema_sql =  f"CREATE SCHEMA IF NOT EXISTS REALPAGE.DATA;"

    cur.execute(create_schema_sql)
    conn.commit()

    columns_str = ["AGG_TYPE", "Property Name",	"Type",	"Unique ID", "Property ID",	"Census Block Id", "City", "Development Company", "Management Company", "Market Name", "Property Owner", "Property Address", "Property Status", "Property Type", "Region", "State", "Submarket Name", "Property Style", "Unit Mix", "Asset Grade in Submarket", "Asset Grade in Market", "Phone", "Website", "TimeSlice", "Category", "Niche"]    
    columns_date = ["PERIOD", "Construction Finish Date", "Construction Start Date", "Renovation Finish", "Renovation Start", "Lease Start Date", "First Move-In Date"]
    columns_str = [col.upper().replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'PERC').replace('$', 'DOLLAR').replace('#', '').replace('.', '_').replace('-', '_').replace('/', '_OVER_') for col in columns_str]
    columns_date = [col.upper().replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'PERC').replace('$', 'DOLLAR').replace('#', '').replace('.', '_').replace('-', '_').replace('/', '_OVER_') for col in columns_date]
    columns_int = ["YEAR", "MONTH", "Stories", "Total Units", "Year Built", "ZIP Code"]
    columns_int = [col.upper().replace(' ', '_') for col in columns_int]
    query_add = ""

    for col in columns:
        if col in columns_str:
            query_add += f"{col} VARCHAR,"
        elif col in columns_date:
            query_add += f"{col} DATE,"
        elif col in columns_int:
            query_add += f"{col} INTEGER,"
        else:
            query_add += f"{col} FLOAT,"
    # Remove the last comma
    query_add = query_add.rstrip(',')
    
    # Create table in Snowflake
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS REALPAGE.DATA.\"{table_name.upper()}\" (
        {query_add}
    );
    """
    cur.execute(create_table_query)
    cur.close()
    conn.close()

def upload_data(table_name, df):
    conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE
        )

    from snowflake.connector.pandas_tools import write_pandas

    cursor = conn.cursor()

    if not df.empty:
        success, num_chunks, num_rows, _ = write_pandas(conn, df, table_name = table_name.upper(), schema="DATA", database="REALPAGE")
        if success:
            print(f"✅ Successfully uploaded {num_rows} rows in {num_chunks} chunks!")
        else:
            print("❌ Upload failed.")
    else:
        print("⚠️ No data to upload.")

    cursor.close()
    conn.close()


current_dir = os.path.expanduser(r"C:\Users\patel\Desktop\Personal\work\Realpage\property_data")
table_name = "PROPERTY_DATA"
print(current_dir + '\n')

words = ["2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017"]
folders = []
files = []

for fol in os.listdir(current_dir):
    if any(year in fol for year in words):
        folders.append(fol)

for fol in folders:
    try:
        sub_dir = os.path.join(current_dir, fol)
        print(f"Adding files from {sub_dir}")
    except FileNotFoundError:
        print(f"Error: Couldn't find the folder specified: '{fol}'.")
        exit()
    for _file in os.listdir(sub_dir):
        files.append(os.path.join(sub_dir, _file))

if files:
    print(f"{len(files)} files were found to upload.")
else:
    print("⚠️ ERROR: Couldn't find any files.")


n = len(files)
for _file in files:
    try:
        df = pd.read_excel(_file)
        print("✅: File loaded successfully.")
    except FileNotFoundError:
        print(f"⚠️ Error: Couldn't find the file: '{_file}'.")
        exit()

    quarter_start_months = {1: 1, 2: 4, 3: 7, 4: 10}
    df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('%', 'PERC').str.replace('$', 'DOLLAR').str.replace('#', '').str.replace('.', '_').str.replace('-', '_').str.replace('/', '_OVER_')
    
    if not df.empty:
        df["AGG_TYPE"] = df["TIMESLICE"].apply(lambda x: "Quarterly" if 'Q' in x else "Monthly")
        df["YEAR"] = df["TIMESLICE"].str.extract(r'Y(\d{4})')[0].astype(float).astype('Int64')
        # if df["AGG_TYPE"] == "Quarterly":
        #     df["MONTH"] = df["TIMESLICE"].str.extract(r'Q(\d)')[0].astype(float).astype('Int64').map(quarter_start_months)
        # else:
        #     df["MONTH"] = df["TIMESLICE"].str.extract(r'M(\d{2})')[0].astype(float).astype('Int64')

        df["MONTH"] = pd.NA

        quarterly_mask = df["AGG_TYPE"] == "Quarterly"
        df.loc[quarterly_mask, "MONTH"] = (
            df.loc[quarterly_mask, "TIMESLICE"]
            .str.extract(r'Q(\d)')[0]
            .astype('Int64')
            .map(quarter_start_months)
        )

        monthly_mask = df["AGG_TYPE"] == "Monthly"
        df.loc[monthly_mask, "MONTH"] = (
            df.loc[monthly_mask, "TIMESLICE"]
            .str.extract(r'M(\d{2})')[0]
            .astype('Int64')
        )

        df["PERIOD"] = pd.to_datetime(
            dict(year=df["YEAR"], month=df["MONTH"], day=1),
            errors='coerce'
        )
        df["PERIOD"] = df["PERIOD"].dt.strftime("%Y-%m-%d")

    else:
        print(f"⚠️ No data found in file: {_file}. Skipping upload.")
        print(f"{len(files)-n+1}/{len(files)} files processed successfully.")
        n -= 1
        continue


    columns = df.columns.tolist()

    create_table(table_name, columns)
    upload_data(table_name, df)

    print(f"{len(files)-n+1}/{len(files)} files processed successfully.")
    n -= 1

