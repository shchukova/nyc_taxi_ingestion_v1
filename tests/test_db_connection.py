# test_db_connection.py
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USERNAME'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    
    cursor = conn.cursor()
    
    # Check current database and schema
    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    result = cursor.fetchone()
    print(f"Current database: {result[0]}")
    print(f"Current schema: {result[1]}")
    
    # Test creating a simple table
    cursor.execute("CREATE OR REPLACE TABLE test_table (id INT)")
    print("✅ Table creation successful!")
    
    # Clean up
    cursor.execute("DROP TABLE test_table")
    print("✅ Table dropped successfully!")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
    print(os.getenv('SNOWFLAKE_DATABASE'))