import httpx
import asyncio
import calendar
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_random_exponential

datasets = {
    "CPI": {
        "url": "https://api.mospi.gov.in/api/iip/getIIPMonthly",
        "default_params": {
            'm': 'QueryData',
            'dbcode': 'hgyd',
            'rowcode': 'zb',
            'colcode': 'sj',
            'wds': '[]'    
        },       
        "get":{
            "CPI":{
                "series_code": "A01010G",
                "label_map":{
                    "A01010G01": "CPI",
                    "A01010G0D": "Core_CPI",                                        
                    "A01010G02": "Food_Tobacco_And_Alcohol",
                    "A01010G03": "Apparel",
                    "A01010G04": "Housing_And_Utilities",
                    "A01010G05": "Household_Goods_And_Services",
                    "A01010G06": "Transportation_And_Communication",
                    "A01010G07": "Education_Culture_And_Recreation",
                    "A01010G08": "Healthcare",
                    "A01010G08": "Miscellaneous",
                    "A01010G0B": "ConsumerGoods",
                    "A01010G0C": "Services"
                }
            },
            "Food_Tobacco_And_Alcohol": {
                "series_code": "A010103",
                "label_map":{
                    "A01010301": "Food",
                    "A01010302": "Grains",
                    "A01010303": "Meat",
                    "A01010305": "Eggs",
                    "A01010306": "Seafood",
                    "A01010307": "Vegetables",
                    "A01010308": "Fruits",
                    "A01010309": "Edible_Oils",
                    "A0101030A": "Pork",
                    "A0101030D": "Dairy",
                    "A0101030E": "Tobacco",
                    "A0101030F": "Alcohol"
                }                
            },
            "Housing": {
                "series_code": "A01010B",
                "label_map":{
                    "A01010B01": "Housing_Ex_Utilities",
                    "A01010B02": "Water_Electricity_And_Household_Fuels"
                }
            },
            "Home_Appliances": {
                "series_code": "A01010C",
                "label_map":{
                    "A01010C01": "Home_Appliances",                    
                }
            },
            "Transport_And_Communication": {
                "series_code": "A01010D",
                "label_map":{
                    "A01010D01": "Transport_Equipment",
                    "A01010D02": "Energy",
                    "A01010D03": "Transport_Services",
                    "A01010D04": "Communication_Equipment",
                    "A01010D05": "Communication_Services",
                }
            },
        }
    }
}

url = "https://data.stats.gov.cn/english/easyquery.htm"

async def _one_request(params=None):    
    async with httpx.AsyncClient() as client:        
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def get(series_code, period_start, period_end=None):
    #convert to YYYYMM
    period_from = period_start.strftime('%Y%m')
    period_to = period_end.strftime('%Y%m') if period_end else datetime.now().strftime('%Y%m')


    

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
        

    




