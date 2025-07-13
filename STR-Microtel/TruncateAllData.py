import snowflake.connector

conn = snowflake.connector.connect(
USER='--'
PASSWORD='--'
ACCOUNT='--'
WAREHOUSE='--'
DATABASE ='--'
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
