import httpx
from tools.helpers import get_last_day_of_month
import asyncio
from httpx import HTTPStatusError
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception


datasets = {
    "IIP": {
        "url": "https://api.mospi.gov.in/api/iip/getIIPMonthly",
        "default_params": {
            "base_year": "2011-12"
        }
    },
    "CPI_Items": {
        "url": "https://api.mospi.gov.in/api/cpi/getItemIndex",
        "default_params": {
            "base_year": "2012",
            "Format": "json",
            "codetype": "item_code"
        }
    },
    "CPI":{
        "url": "https://api.mospi.gov.in/api/cpi/getCPIIndex",
        "default_params": {
            "base_year": "2012",
            "series": "Current",
            "Format": "json",                
            "state_code": "99",  # All India
            "sector_code": "3",  # Combined
            "codetype": "subgroup_code"
        }
    }
}

import time
def handle_rate_limit_error(exception):
    """Retry on 429 or 5xx errors, respect Retry-After if present."""
    if isinstance(exception, HTTPStatusError):
        status = exception.response.status_code
        if status == 429:
            retry_after = exception.response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = int(retry_after)
                except ValueError:
                    delay = 5
                print(f"[Rate limit] Waiting {delay} seconds before retry...")
                time.sleep(delay)  # Blocking sleep is fine for tenacity sync retry
            else:
                print("[Rate limit] No Retry-After header, using backoff...")
            return True
        if 500 <= status < 600:
            return True
    return False

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(multiplier=2, max=60),retry=retry_if_exception(handle_rate_limit_error))
async def _one_request(url, params):    
    timeout = httpx.Timeout(
        connect=10.0,   # time to establish connection (default is 5.0)
        read=60.0,      # time to wait for server response
        write=10.0,
        pool=10.0
    )
    with httpx.Client(timeout=60) as client:
        res = client.get(url, params=params)
        res.raise_for_status()
        return res.json()


async def one_request(url, params):
    #first request is for metadata
    print ("Fetching for "+ url + " with params: " + str(params))
    first_res = await _one_request(url, params)    
    _data = first_res.get('data', [])
    pages = first_res.get('meta_data', {}).get("totalPages", 1)
    
    for page in range(2, pages + 1):
        params['page'] = page
        print(f"Fetching page {page} of {pages}")
        try:
            res = await _one_request(url, params)            
        except Exception as e:
            print ("Error fetching page {}: {}".format(page, e))
            #this means incomplete do not return any data
            return []
        _data.extend(res.get('data', []))
            
    for d in _data:
        #convert month to number
        month_num = months.index(d["month"]) + 1
        d["month"] = month_num
    return _data    
        

months = ['January', 'February', 'March', 'April', 'May', 'June', 
          'July', 'August', 'September', 'October', 'November', 'December']

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def get(ticker, period_start, period_end=None, updated_dates=None):
    url = datasets[ticker]["url"]
    params = datasets[ticker]["default_params"]

    #get all periods
    start_year = period_start.year
    end_year = period_end.year if period_end else start_year
    start_month = period_start.month
    end_month = period_end.month if period_end else 12
    
    dates_to_update = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                continue
            month_end_dt = get_last_day_of_month(year, month)
            if month_end_dt not in updated_dates:
                dates_to_update.append((year, month))            

    tasks = []
    for year, month in dates_to_update:
        local_params = params.copy()
        local_params.update({
            "year": year,
            "month_code": month
        })
        tasks.append(one_request(url, local_params))
    
    batch_size = 1
    final_results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        results = await asyncio.gather(*batch)        
        for result in results:
            final_results.extend(result)        
   
    return final_results