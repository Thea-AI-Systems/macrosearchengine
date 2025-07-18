from datasets.Inflation import CPI_YoY_IN, CPI_YoY_CN
from tools import parquet_handler
import asyncio
import duckdb


async def main():
    await CPI_YoY_IN.update()
    #await CPI_YoY_CN.update()



if __name__ == "__main__":    
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
    
    
    
    