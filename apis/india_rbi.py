import asyncio
from tools import stubborn_browser
from bs4 import BeautifulSoup
from tools import helpers
from dateutil import parser
from curl_cffi import requests
from tenacity import retry, stop_after_attempt, wait_fixed
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio  # use tqdm for async
from tqdm import tqdm
parallel_requests = 25


datasets = {
    "lending_and_deposit_rates": {                
        "url":"https://www.rbi.org.in/Scripts/Pr_DataRelease.aspx?SectionID=369&DateFilter=Year",
        "params": {
            "year": "hdnYear",
            "month": "hdnMonth"
        },
        "default_params": {
            "hdnYear": "2022",
            "hdnMonth": "0",            
            "hdnIsMonth":"",
            "UsrFontCntr%24txtSearch": "",
            "UsrFontCntr%24btn": ""
        }
    },
    "BankCreditAndDeposits":{        
        "url":"https://rbi.org.in/Scripts/WSSViewDetail.aspx",        
        "table_base_url": "https://rbi.org.in/Scripts/",
        "fetch_type": "weekly_table",
        "page_search_term": "Scheduled Commercial Banks - Business in India",        
        "params": {
            "TYPE":"Section",
            "PARAM1":3
        }
    },
    "BankRatios":{     
        "url":"https://rbi.org.in/Scripts/WSSViewDetail.aspx",
        "table_base_url": "https://rbi.org.in/Scripts/",
        "fetch_type": "weekly_table",        
        "page_search_term": "Ratios and Rates",
        "params": {
            "TYPE":"Section",
            "PARAM1":4
        }
    },
    "BankLendingAndDepositRates":{     
        "url":"https://rbi.org.in/Scripts/WSSViewDetail.aspx",
        "table_base_url": "https://rbi.org.in/Scripts/",
        "fetch_type": "weekly_table",
        "page_search_term": "Ratios and Rates",        
        "params": {
            "TYPE":"Section",
            "PARAM1":4
        }
    },
    "MoneyMarketRates":{     
        "url":"https://rbi.org.in/Scripts/WSSViewDetail.aspx",
        "table_base_url": "https://rbi.org.in/Scripts/",
        "fetch_type": "weekly_table",         
        "page_search_term": "Ratios and Rates",
        "params": {
            "TYPE":"Section",
            "PARAM1":4
        }
    },
    "PolicyRates":{     
        "url":"https://rbi.org.in/Scripts/WSSViewDetail.aspx",
        "table_base_url": "https://rbi.org.in/Scripts/",
        "fetch_type": "weekly_table",        
        "page_search_term": "Ratios and Rates",
        "params": {
            "TYPE":"Section",
            "PARAM1":4
        }
    }
}

async def get_weekly_table(dataset, url, release_date):
    res = await stubborn_browser.get({'url':url, 'compression': True})
    soup = BeautifulSoup(res.text, 'html.parser')
    page_search_term = datasets[dataset]["page_search_term"]    
    table_row = soup.find("tr", string=page_search_term)
    main_table = table_row.find_next("tr").find("table")

    #br is causing an error due to improper formatting    
    return {
        'table': main_table,        
        'release_date': release_date,
        'source': url
    }

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get(dataset, period_from, period_to=None, updated_dates=[]):
    #period_from and period_to have to be datetime objects
    archival_page_search_term = datasets[dataset]["page_search_term"]
    url = datasets[dataset]["url"]
    params = datasets[dataset]["params"]
    encoded_params = requests.utils.urlencode(params)
    url = f"{url}?{encoded_params}"
    
    base_url = datasets[dataset]["table_base_url"]
    
    res = await stubborn_browser.get({'url':url, 'compression': True})
    soup = BeautifulSoup(res.text, 'html.parser')    
    
    links = soup.find_all("a", string=archival_page_search_term)
    recs = []
    
    for link in links: 
        try:
            parent_tr = link.find_parent("tr")
            previous_tr = parent_tr.find_previous("tr")

            dt = previous_tr.find("th").text.strip()
            
            dt = datetime.strptime(dt, "%d %b %Y")            

            if dt in updated_dates:
                continue

            if period_to and not (period_from <= dt <= period_to):
                continue
            
            if not period_to and dt < period_from:
                continue

            recs.append({
                "release_date": dt,
                "link": base_url+link.get("href"),
            })            
        except Exception as e:
            print (f"Error: {e}")
            pass

    # get excels from links
    tables = []    
    #perform in batches of parallel requests
    for i in tqdm(range(0, len(recs), parallel_requests), desc=f"Processing {dataset}", ncols=100):
        batch = recs[i:i+parallel_requests]
        tasks = []
        for rec in batch:
            if datasets[dataset]["fetch_type"] == "weekly_table":
                tasks.append(get_weekly_table(dataset, rec["link"], rec["release_date"]))            
        tables.extend(await asyncio.gather(*tasks, return_exceptions=True))

    # Filter out failed (None) results
    return [t for t in tables if t]
    
    