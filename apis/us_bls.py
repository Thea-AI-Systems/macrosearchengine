from tenacity import retry, stop_after_attempt, wait_fixed
from datetime import datetime
import requests
from config import api_registration_keys
api_key = api_registration_keys.access['US']['bls']

url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

series_map = {
    "CPI":{
        "CUSR0000SA0": "All Items",
        "CUSR0000SAF": "Food",
        "CUSR0000SAH": "Housing",
        "CUSR0000SAA": "Apparel",
}

datasets = {
    "CPI": {        
        "default_params": {            
            "calculations": True,
            "registrationkey": api_key
        }
    },
    
}

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get(dataset, period_from, period_to=None, updated_dates=[]):
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)