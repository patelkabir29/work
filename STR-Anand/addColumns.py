import snowflake.connector

USER = 'kabir'
PASS = 'Score@1000'
ACCOUNT = 'nzb10951.us-east-1'
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'STR'

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=USER,
    password=PASS,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE
)

cur = conn.cursor()

# ////////////////////////////////////////////////////////////////
# To add columns to tables


# Get all table names matching the pattern
cur.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME ILIKE '%year%' 
      OR TABLE_NAME ILIKE '%running%'
""")

tables = cur.fetchall()

# Iterate over tables and add columns
for schema, table in tables:
    alter_query = f"""
    ALTER TABLE STR."{schema}"."{table}"
    ADD COLUMN "2018" DOUBLE;"""
    print(f"Executing: {alter_query}")
    cur.execute(alter_query)

# Close connection
cur.close()
conn.close()

# ////////////////////////////////////////////////////////////////

# # To remove rows from tables matching a condition
# cur = conn.cursor()

# # Step 1: Find all tables that contain the column "Property Name"
# cur.execute("""
#     SELECT TABLE_SCHEMA, TABLE_NAME
#     FROM INFORMATION_SCHEMA.COLUMNS
#     WHERE COLUMN_NAME = 'PROPERTY_NAME'
# """)

# tables = cur.fetchall()

# # Step 2: Delete rows from each matching table
# property_name = "Holiday Inn Express & Suites Warwick Providence Airport"

# for schema, table in tables:
#     delete_query = f'''
#         DELETE FROM STR."{schema}"."{table}" 
#         WHERE "PROPERTY_NAME" = '{property_name}';
#     '''
    
#     print(f"Executing: {delete_query}")
#     cur.execute(delete_query)

# cur.close()
# conn.close()


# ////////////////////////////////////////////////////////////////
# # To update PROPERTY_ID for specific properties
# # Get all tables with PROPERTY_ID
# cur.execute("""
#     SELECT TABLE_SCHEMA, TABLE_NAME 
#     FROM INFORMATION_SCHEMA.COLUMNS
#     WHERE COLUMN_NAME = 'PROPERTY_ID'
# """)

# tables = cur.fetchall()

# # Process each table
# for schema, table in tables:
#     alter_steps = [
#         f'ALTER TABLE "{schema}"."{table}" ADD COLUMN PROPERTY_ID_TMP VARCHAR;',
#         f'UPDATE "{schema}"."{table}" SET PROPERTY_ID_TMP = CAST(PROPERTY_ID AS VARCHAR);',
#         f'ALTER TABLE "{schema}"."{table}" DROP COLUMN PROPERTY_ID;',
#         f'ALTER TABLE "{schema}"."{table}" RENAME COLUMN PROPERTY_ID_TMP TO PROPERTY_ID;'
#     ]
    
#     for step in alter_steps:
#         print(f"Executing: {step}")
#         cur.execute(step)


# # Update PROPERTY_ID for specific properties
# for schema, table in tables:
#     update_query1 = f"UPDATE \"{schema}\".\"{table}\" SET PROPERTY_ID = '1792' WHERE PROPERTY_NAME = 'Holiday Inn Express & Suites Warwick Providence Airport';"
#     update_query2 = f"UPDATE \"{schema}\".\"{table}\" SET PROPERTY_ID = 'CT122' WHERE PROPERTY_NAME = 'Econo Lodge Bethel';"
    
#     print(f"Executing: {update_query1}")
#     cur.execute(update_query1)

#     print(f"Executing: {update_query2}")
#     cur.execute(update_query2)

# Close connection
cur.close()
conn.close()