import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

def update():
    url = "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"

    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')


    year = datetime.now().year
    search_term = f"Regular Press Release Calendar of NBS in {year}"

    #search term in the actual text - use regex
    link = soup.find('a', string=lambda text: text and search_term in text)

    #get the first link
    if not link:
        return
    
    full_link = urljoin(url, link['href'])
    soup = BeautifulSoup(requests.get(full_link).text, 'html.parser')

    table = soup.find('table', class_='trs_word_table')
    
    rows = []
    row = {'ticker': None, 'periods': []}
    holes = []
    for tr in table.find_all('tr'):        
        if tr.get('class') and 'firstRow' in tr['class']:
            continue
        
        multi_row = any([td.get("rowspan", None)=="2" for td in tr.find_all('td')])
        
        if multi_row:
            for i, td in enumerate(tr.find_all('td')):
                if i==0:
                    continue
                if td.get("rowspan", None)=="2":
                    holes.append(1)
                else:
                    holes.append(0)
                if i==1:                    
                    row['ticker'] = td.text.strip()
                else:
                    if td.text.strip() and td.text.strip() != "……":                        
                        row['periods'].append(td.text.strip())
        else:            
            for i, hole in enumerate(holes):
                all_tds = tr.find_all('td')
                prior_zero_holes = i - sum(holes[:i])
                #print (all_tds)
                #print (all_tds[prior_zero_holes].text.strip())                
                #print (row['periods'])
                #print (i)
                #print (row['periods'][i])
                row['periods'][i-1] += " " + all_tds[prior_zero_holes].text.strip()
                rows.append(row)
            row = {
                'ticker': None,
                'periods': []
            }                

        

    print (rows)
        


update()


