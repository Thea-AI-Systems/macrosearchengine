from curl_cffi import requests
from datetime import datetime
import json
import calendar
import pandas as pd
import os
#check if config folder and api_registration_keys.py exist
if not os.path.exists('config') or not os.path.exists('config/api_registration_keys.py'):
    print ("This API requires a valid API key. Please register at https://www.bls.gov/developers/ and add your key either manually or to the config/api_registration_keys.py file.")
    exit(1)    
    
from config import api_registration_keys
api_key = api_registration_keys.access['US']['bls']


itemcode_key = {
    "All Items": "SA0",    
    "Food and Beverages": "SAF",
    "Housing": "SAH",
    "Apparel": "SAA",
    "Transportation": "SAT",
    "Medical Care": "SAM",
    "Recreation": "SAR",
    "Education and Communication": "SAE",
    "Other Goods and Services": "SAG",
}

def get_data_as_csv(startyear=2011, endyear=2025):
    headers = {'Content-type': 'application/json'}
    series_ids = []
    
    # --- Step 1: Create series IDs for each item
    for item, code in itemcode_key.items():
        series_ids.append(f"CUSR0000{code}")    
    
    data = json.dumps({
        "seriesid": series_ids,
        "startyear":"2025", "endyear":"2025",
        "calculations": True,
        "registrationkey": api_key
        })
    
    try:
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        json_data = json.loads(p.text)
        if json_data.get('status') != 'REQUEST_SUCCEEDED' or not json_data.get('Results'):
            raise ValueError("No data found for the specified series.")
        
        recs = []
        for result in json_data['Results']['series']:
            itemcode = result['seriesID'][len('CUUSR0000')-1:]
            print (itemcode)
            itemlabel = next((k for k, v in itemcode_key.items() if v == itemcode), "Unknown")
            
            for data in result['data']:
                year = int(data['year'])
                month = int(data['period'][1:])
                days_in_month = calendar.monthrange(year, month)[1]
                 
                #last day of the month
                period_end = datetime(year, month, days_in_month)
                
                cpi_yoy = data.get("calculations",{}).get('pct_changes', {}).get('12', None)

                rec = {                    
                    "itemlabel": itemlabel,
                    "period_end": period_end,
                    "period_duration": "M",
                    "value": cpi_yoy,
                    "unit": "PERCENT"

                }
                recs.append(rec)

        df = pd.DataFrame(recs)
        df.to_csv('US_CPI_data.csv', index=False)
    except Exception as e:
        print (e)
        raise ValueError("Failed to fetch data from the BLS API. Please check your API key and network connection.")    
    
if __name__ == "__main__":
    get_data_as_csv()
    print("CSV file created successfully.") 



