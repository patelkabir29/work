import snowflake.connector
import json

def test_installation(config_path):
    """
    Function to test the installation of the snowflake connector. 
    It reads the version of the snowflake connector.
    """
    # Read the config file
    with open(config_path) as f:
        config = json.load(f)

    # Gets the version
    ctx = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()