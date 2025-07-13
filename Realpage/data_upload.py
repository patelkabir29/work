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

    columns_str = ["GEOGRAPHY_NAME", "GEOGRAPHY_TYPE", "MARKET_NAME", "UNIQUE_ID", "TIMESLICE", "CATEGORY", "NICHE"]
    query_add = ""

    for col in columns:
        if col in columns_str:
            query_add += f"{col} VARCHAR,"
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


current_dir = os.path.expanduser(r"C:\Users\patel\Desktop\Personal\work\Realpage")
table_name = "GEOGRAPHY_DATA"
print(current_dir + '\n')

word = "20"
folders = []
files = []

for fol in os.listdir(current_dir):
    if (word in fol):
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

    df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('%', 'PERC').str.replace('$', 'DOLLAR').str.replace('#', '').str.replace('.', '_').str.replace('-', '_').str.replace('/', '_OVER_')
    columns = df.columns.tolist()
    print(f"Columns in the file: {columns}")

    create_table(table_name, columns)
    upload_data(table_name, df)

    print(f"{len(files)-n+1}/{len(files)} files processed successfully.")
    n -= 1

