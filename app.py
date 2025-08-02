#from datasets.Inflation import CPI_YoY_IN, CPI_YoY_CN
from apis import india_rbi
from tools import parquet_handler
from datasets.IIP.CN import IIP as IIP_CN
from datasets.Banking.IN import BankCreditAndDeposits as BankCreditAndDeposits_IN
from datasets.Banking.IN import BankRatios as BankRatios_IN
import asyncio


async def main():    
    tasks = [
        BankCreditAndDeposits_IN.update(overwrite_history=True),
        BankRatios_IN.update(overwrite_history=True)
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":    
    '''
    asyncio.run(main())
    
    #load from s3 using presigned url
    conn = duckdb.connect(database=':memory:')
    presigned_url = parquet_handler.get_presigned_url('datasets/Inflation/cpi_yoy')

    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Example: Query pushdown – Only select filtered columns and rows directly from the file
    query = f"""
    SELECT *
    FROM read_parquet('{presigned_url[0]}') 
    WHERE region = 'IN'
    AND period_end = '2025-06-30'
    """
    result = conn.execute(query).fetchdf()
    print(result)
    '''
    asyncio.run(main())