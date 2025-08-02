import httpx
import asyncio
import calendar
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_random_exponential

async def _one_request(params=None):
    url = "https://data.stats.gov.cn/english/easyquery.htm"
    async with httpx.AsyncClient() as client:        
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
baseparams = {
    'm': 'QueryData',
    'dbcode': 'hgyd',
    'rowcode': 'zb',
    'colcode': 'sj',
    'wds': '[]'        
}

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def get(series_code, period_from, period_to=None):        
    dfwds = []
    dfwds.append({"wdcode": "zb", "valuecode": series_code})
    period_code = period_from+"-"
    if period_to is not None:
        period_code += period_to
    dfwds.append({"wdcode": "sj", "valuecode": period_code})
    params = baseparams.copy()
    params['dfwds'] = str(dfwds).replace("'", '"')
    res = await _one_request(params=params)    
    
    if res['returncode'] != 200:
        raise Exception(f"Error fetching data for {series_code} from {period_from} to {period_to}: {res['retmsg']}")
    
    data = res['returndata']['datanodes']
    recs = []
    for d in data:
        code = d.get("code")
        series_code, period = code.split("_")
        series_code = series_code.replace("zb.", "")
        period = period.replace("sj.", "")
        #period_end is end of calendar month 
        year = int(period[:4])
        month = int(period[4:6])
        period_end = calendar.monthrange(year, month)[1]
        period_end = datetime(year, month, period_end)
        data = d['data']['data'] if d['data'].get("hasdata") else None
        if data is None:
            continue
        recs.append({
            "series_code": series_code,
            "period_end": period_end,
            "value": data
        })

    return recs
        

    




