import boto3
import duckdb
import os
#load config.keys.env
from dotenv import load_dotenv
import io
from datetime import datetime
import asyncio


pqdb = duckdb.connect()
#load csv from datasets_Banking_BankCreditAndDeposits_IN.csv
pqdb.execute("CREATE OR REPLACE TABLE temp_table AS SELECT * FROM read_csv_auto('datasets_Banking_BankCreditAndDeposits_IN.csv')")

query = """
SELECT max(period_end)
FROM temp_table
WHERE period_end > '2025-01-01'
"""

print ("Executing query:", query)
result = pqdb.execute(query).fetchdf()
print(result)