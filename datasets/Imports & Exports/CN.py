from curl_cffi import requests
from bs4 import BeautifulSoup

# Step 1: Get the URLs for the datasets

urls = {
    2025: "http://www.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/302277/6348926/index.html",
    2024: "http://www.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/302277/5668662/index.html"
}

# Step 2: Finding the correct Table for URL
'''
    HTML element with id = "con_one2025_2
'''
def get_table_id(year):
    return f"con_one{year}_2"
    
def get_data(year, month):
    url = urls[year]
    table_id = get_table_id(year)
    
    # Step 3: Fetch the HTML content
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Step 4: Find the table with the specified id
    table_div = soup.find('div', id=table_id)
    table = table_div.find('table') if table_div else None
    
    if not table:
        raise ValueError(f"Table with id {table_id} not found in the HTML content.")
    
    # Step 5: Extract data from the table
    data = []
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        if cols:
            data.append(cols)
    
    return data