from tools.parquet_handler import get_presigned_url
import duckdb



def build_query(dataset = None, ticker=None, metric=None, country=None, dimensions=None, dimensions_list=None):
    query = []
    if dataset:
        query.append(f"dataset = '{dataset}'")
    if ticker:
        query.append(f"ticker = '{ticker}'")        
    if metric:
        query.append(f"metric = '{metric}'")
    if country:
        query.append(f"country = '{country}'")
    
    query = " AND ".join(query)    
    return query
    
async def get_latest_date(parquet_loc, dataset, ticker=None, country=None):
    presigned_url = await get_presigned_url(parquet_loc)  
    if not presigned_url:        
        return None
    presigned_url = presigned_url[0]  # Get the first URL if there are multiple
    
    query = build_query(dataset, ticker=ticker, country=country)    

    query = f"SELECT MAX(period_end) FROM read_parquet('{presigned_url}') WHERE {query}"        
    
    dt = duckdb.query(query).fetchone()
    if dt and dt[0]:
        return dt[0]
    return None
