import httpx
import asyncio
import calendar
from curl_cffi import requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from pyppeteer import launch

from tenacity import retry, stop_after_attempt, wait_random_exponential
import stubborn_browser
# Step 1: Get the URLs for the datasets


urls = {
    2025: "http://www.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/302277/6348926/index.html",
    2024: "http://www.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/302277/5668662/index.html"
}
data ={
"url":"http://www.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/302277/6348926/index.html",
"renderType":"plainText"
}

table_links = [        
        {
            "label":"import_export_by_country",
            "must_have":["(2)"]
        },
        {
            "label":"import_export_by_hs_code",
            "must_have":["(4)"]
        },        
        {
            "label":"import_by_country_by_hs_code",
            "must_have":["(15)"]
        },
        {
            "label":"export_by_country_by_hs_code",
            "must_have":["(16)"]
        }
    ]

    
'''
    ### Examples   
    # input
    res = china_customs.get("import_export", "2025-01", "2025-12")

    # output
    [
        {
            "from_country": "<2digit_country_code>",
            "to_country": "<2digit_country_code>",
            "metric": "imports" or "exports",
            "hs_code": "<hs_code>",            
            "period_end": datetime(2025, 12, 31), # end of month
            "value": 123456789,
            "unit": "USD"
        },
        ...
    ]    
    
    get("import_export_by_hs_code", "2025-01", "2025-12")
'''

async def month_page_link(table_label, year, month):
    '''
    async with httpx.AsyncClient() as client:
        response = await client.get(urls[2025])
        response.raise_for_status()
    '''
    
    try:        
        response = await stubborn_browser.antibot_get({'url':urls[year]})
    except Exception as e:
        print (e)
        raise ValueError(f"Failed to fetch the page for year {year}: {e}")
    
    soup = BeautifulSoup(response.get("content"), 'html.parser')
    
    usd_table = soup.find('table', id=f'yb{year}USD')
    
    if not usd_table:
        raise ValueError("Table with id 'yb2025USD' not found in the HTML content.")
    
    table_must_haves = next((link for link in table_links if link['label'] == table_label), None)
    if not table_must_haves:
        raise ValueError(f"Table label {table_label} not found in the predefined links.")    
    
    table_must_haves = table_must_haves.copy()["must_have"]    
    
    for i, tr in enumerate(usd_table.find_all('tr')):
        first_col = tr.find('td')
        
        #check if it has all the must_have terms
        if not first_col:
            continue
        
        if all(term in first_col.text for term in table_must_haves):
            month_str = f"{int(month)}月"
            month_link = tr.find('a', string=lambda text: text and month_str in text)
            if month_link:
                full_link = month_link['href']
                if not full_link.startswith("http"):
                    full_link = urljoin(urls[year], full_link)
                return full_link

    return None

async def parse_page(table_label, year, month, month_page):
    '''
    async with httpx.AsyncClient() as client:
        response = await client.get(month_page)
        response.raise_for_status()
    
    
    soup = BeautifulSoup(response.content, 'html.parser')
    '''
        

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def get(table_label, yyyymm):
    
    url = "http://www.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/302277/6348926/index.html"

    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    
    _month_page = await month_page_link(table_label, year, month)

    print (_month_page)

    return await parse_page(table_label, year, month, _month_page)

if __name__ == "__main__":
    # Example usage
    
    asyncio.run(get("import_export", "202501"))