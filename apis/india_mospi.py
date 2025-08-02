import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential
from helpers import get_last_day_of_month
import asyncio

datasets = {
    "IIP": {
        "url": "https://api.mospi.gov.in/api/iip/getIIPMonthly",
        "default_params": {
            "base_year": "2011-12"
        }
    }
}

async def one_request(url, params):
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        results = response.json()
        _data = results.get("data", [])        
        for d in _data:
            #convert month to number
            month_num = months.index(d["month"]) + 1
            _data["month"] = month_num
        return _data
        

months = ['January', 'February', 'March', 'April', 'May', 'June', 
          'July', 'August', 'September', 'October', 'November', 'December']

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def get(ticker, period_start, period_end=None, updated_dates=None):
    url = datasets["IIP"]["url"]
    params = datasets["IIP"]["default_params"]

    #get all periods
    start_year = period_start.year
    end_year = period_end.year if period_end else start_year
    start_month = period_start.month
    end_month = period_end.month if period_end else 12

    updated_dates = [dt.date() for dt in updated_dates] if updated_dates else []

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
        params.update({
            "year": year,
            "month_code": months[month - 1]
        })
        tasks.append(one_request(url, params.copy()))
    
    batch_size = 25
    final_results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        results = await asyncio.gather(*batch)
        for result in results:
            final_results.extend(result)

    return final_results