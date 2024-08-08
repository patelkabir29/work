import snowflake.connector

conn = snowflake.connector.connect(
    user='kabir',
    password='Score@1000',
    account='nzb10951.us-east-1',
    warehouse='COMPUTE_WH',
    database ='STR'
)

cursor = conn.cursor()

cursor.execute("SHOW SCHEMAS")
schemas = cursor.fetchall()

for schema in schemas:
    schema_name = schema[1]  # Schema name is in the second column
    cursor.execute(f'SHOW TABLES IN SCHEMA "{schema_name}"')
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[1]  # Table name is in the second column
        truncate_query = f'TRUNCATE TABLE "{schema_name}"."{table_name}"'
        print(truncate_query)
        cursor.execute(truncate_query)

# Close the cursor and connection
cursor.close()
conn.close()