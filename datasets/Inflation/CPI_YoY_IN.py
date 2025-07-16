from tools import stubborn_browser
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
from io import StringIO
from datetime import datetime

def visit_page(url, fill_aspx=True):    
    session = stubborn_browser.seed_session(url="https://cpi.mospi.gov.in")
    res = stubborn_browser.get({"url":url, "session":session})
    
    soup = BeautifulSoup(res.text, 'html.parser')
    if fill_aspx:
        VIEWSTATE = soup.find('input', {'id': '__VIEWSTATE'})['value']
        VIEWSTATEGENERATOR = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})['value']
        EVENTVALIDATION = soup.find('input', {'id': '__EVENTVALIDATION'})['value']

        # --- There is a particular way the params are structured for the POST request ---
        params = {
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATEENCRYPTED": "",
            "__VIEWSTATE": quote_plus(VIEWSTATE),
            "__VIEWSTATEGENERATOR": VIEWSTATEGENERATOR,        
            "__EVENTVALIDATION": quote_plus(EVENTVALIDATION)
        }

        return session, soup, params
    else:
        return session, soup, None

# Get Itemised Weights - This is needed to reorganise CPI according to custom groupings
def get_item_weights():
    url = "https://cpi.mospi.gov.in/Weight_AI_Item_Combined_2012.aspx"
    session, soup, params = visit_page(url, fill_aspx=False)
    table = soup.find('table', {'id': 'Content1_GridView1'})

    table_df = pd.read_html(StringIO(str(table)))[0]
    
    #table_df.columns = ['Item_Code', 'Item Label', 'Weight']
    #rename All India Item Combined Weight(Base:2012) to Weight    
    table_df.rename(columns={
        'All India Item Combined Weight(Base:2012)': 'weight',
        'Item': 'item_label',
        'Item_Code': 'item_code'
    }, inplace=True)
    table_df['item_label'] = table_df['item_label'].astype(str).str.strip()
    table_df['item_code'] = table_df['item_code'].astype(str).str.strip()

    return table_df

def get_item_inflation(start_date, end_date):
    url = "https://cpi.mospi.gov.in/AllIndia_Item_CombinedInflation_2012.aspx"
    session, soup, params = visit_page(url)

    #Find table with id Content1_CheckBoxList1
    checkboxes_table = soup.find("table", {"id": "Content1_CheckBoxList1"})
    checkbox_inputs = checkboxes_table.find_all("input", {"type": "checkbox"})
    checkbox_params = {}
    for checkbox in checkbox_inputs:
        name = checkbox.get("name")
        value = checkbox.get("value")
        if name and value:
            checkbox_params[name] = value
    
    latest_saved_year = start_date.year if start_date else 2025
    latest_saved_month = start_date.month if start_date else 1

    if latest_saved_month == 12:
        start_from_year = latest_saved_year + 1
        start_from_month = 1
    else:
        start_from_year = latest_saved_year
        start_from_month = latest_saved_month + 1

    start_from_year = str(start_from_year)
    start_from_month = str(start_from_month).zfill(2)  # Ensure month

    current_year = datetime.now().year
    current_month = datetime.now().month
    current_year = str(current_year)    
    current_month = str(current_month).zfill(2)  # Ensure month is two digits

    query_params = {        
        "ctl00$Content1$DropDownList1":start_from_year,
        "ctl00$Content1$DropDownList2":current_year,
        "ctl00$Content1$DropDownList3":start_from_month,        
        "ctl00$Content1$DropDownList4":current_month,
        "ctl00$Content1$DropDownList8":"Group",        
        "ctl00$Content1$Button2":"View+Inflation"
    }

    quoted_checkbox_params = {quote_plus(key): value for key, value in checkbox_params.items()}
    quoted_query_params = {quote_plus(key): value for key, value in query_params.items()}
    
    params.update(checkbox_params)    
    params.update(quoted_checkbox_params)
    params.update(quoted_query_params)
    
    raw_params = "&".join([f"{key}={value}" for key, value in params.items()])
    
    addl_headers = {        
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    res = stubborn_browser.post({"url": url, "session": session, "data": raw_params, "addl_headers": addl_headers, "compression": True})

    soup = BeautifulSoup(res.text, 'html.parser')
    table_id = "Content1_GridView1"

    table = soup.find("table", {"id": table_id})
    
    if table:        
        try:
            table_df = pd.read_html(StringIO(str(table)), header=0, flavor='bs4')
            df = table_df[0]
            df.columns = df.columns.str.strip()  # Clean column names
            df = df.dropna(how='all')  # Drop rows where all elements are NaN
            df = df.reset_index(drop=True)  # Reset index        
            return df
        except ValueError as e:
            print(f"Error reading table: {e}")
            return None
    else:
        print("No data found in the table.")

def get_group_inflation(start_date, end_date, sector="Combined"):
    url = "https://cpi.mospi.gov.in/Inflation_CurrentSeries_2012_Crosstab.aspx"
    session, soup, params = visit_page(url)

    latest_saved_year = start_date.year if start_date else 2025
    latest_saved_month = start_date.month if start_date else 1

    if latest_saved_month == 12:
        start_from_year = latest_saved_year + 1        
    else:
        start_from_year = latest_saved_year        

    start_from_year = str(start_from_year)    

    current_year = datetime.now().year    
    current_year = str(current_year)  

    sector_key = "1"
    if sector == "Urban":
        sector_key = "2"
    elif sector == "Combined":
        sector_key = "3"

    query_params = {        
        "ctl00$Content1$DropDownList1":start_from_year,
        "ctl00$Content1$DropDownList2":current_year,
        "ctl00$Content1$CheckBoxList1$0":"99",
        "ctl00$Content1$DropDownList9":sector_key,
        "ctl00$Content1$DropDownList5":"27b",          
        "ctl00$Content1$Button2":"View+Inflation+Rates"
    }

    addl_headers = {        
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    quoted_query_params = {quote_plus(key): value for key, value in query_params.items()}
    params.update(quoted_query_params)

    raw_params = "&".join([f"{key}={value}" for key, value in params.items()])

    res = stubborn_browser.post({
        "url": url, 
        "session": session, 
        "data":raw_params, 
        "addl_headers": addl_headers, 
        "compression": True
    })

    soup = BeautifulSoup(res.text, 'html.parser')
    
    table_id = "Content1_GridView1"
    table = soup.find("table", {"id": table_id})
    if table:
        #wrap table in a StringIO object to read it as a DataFrame
        #do not pass string directly to pd.read_html, as it may not handle the HTML correctly        
        try:  
            clean_html = str(table).replace('&nbsp;', ' ')          
            table_df = pd.read_html(StringIO(str(clean_html)), header=0, flavor='bs4')
            df = table_df[0]
            df.columns = df.columns.str.strip()  # Clean column names
            df = df.dropna(how='all')  # Drop rows where all elements are NaN
            df = df.reset_index(drop=True)  # Reset index
            return df
        except ValueError as e:
            print(f"Error reading table: {e}")
            return None
    else:
        print("No data found in the table.")


def update():
    start_date = datetime(2020, 1, 1)
    item_weight = get_item_weights()
    item_inflation_df = get_item_inflation(start_date, datetime.now())
    group_inflation_df = get_group_inflation(start_date, datetime.now(), sector="Combined")
    #save as 3 CSV files
    item_weight.to_csv('item_weights.csv', index=False)
    item_inflation_df.to_csv('item_inflation.csv', index=False)
    group_inflation_df.to_csv('group_inflation.csv', index=False)
    print("Data updated successfully.")
    





    