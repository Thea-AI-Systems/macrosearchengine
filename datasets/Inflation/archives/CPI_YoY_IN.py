from datasets.Inflation.tools import aggregate_inflation
from tools import stubborn_browser
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
from io import StringIO
from datetime import datetime
import calendar
#from curl_cffi import requests
import httpx
import requests
import asyncio
import json
from tqdm import tqdm
import fuzzywuzzy.process

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

def get_metadata():    
    url = "https://esankhyiki.mospi.gov.in/API/CPI%20Metadata.xlsx"
    recs = []    
    try:
        #this has many worksheets, we need to read the metadata from the "Group_code", "SubGroup_code" and "Item" worksheets
        #header in every sheet is in the first row
        metadata_df = pd.read_excel(url, sheet_name=None, header=0)  # Read all sheets into a dictionary of DataFrames        
        
        #group codes are in "Group_code" worksheet        
        #group_df = metadata_df[metadata_df['Worksheet'] == 'Group_code' and metadata_df['Base_Year'] == '2012']
        group_df = metadata_df['Group_code']  # Directly access the 'Group_code' sheet
        group_df = group_df[group_df['Base_Year'] == 2012]  # Filter for Base Year 2012        
        #Description is the label, Group_code is the code
        #Sometimes group_code is a string - sometimes it is a number, so convert to string
        for row in group_df.itertuples():
            recs.append({
                "code": str(row.Group_Code).strip()+".",
                "label": str(row.Description).strip(),
                "codetype":"group_code"
            })
        
        #subgroup codes are in "SubGroup_code" worksheet
        subgroup_df = metadata_df['SubGroup_code']  # Directly access the 'SubGroup_code' sheet
        subgroup_df = subgroup_df[subgroup_df['Base_Year'] == 2012]                      
        subgroup_df = subgroup_df[subgroup_df['Subgroup Description'].notnull()]  
        for row in subgroup_df.itertuples():            
            recs.append({
                "code": str(row.SubGroup_Code).strip()+".",
                "label": str(row._3).strip(),
                "codetype":"subgroup_code"
            })        

        #item codes are in "Item" worksheet
        item_df = metadata_df['Item']  # Directly access the 'Item' sheet
        item_df = item_df[item_df['Base_Year'] == 2012]
        #Item Label is the label, Item Code is the code        
        for row in item_df.itertuples():
            recs.append({
                "code": str(row.Item_Code).strip(),
                "label": str(row._2).strip(),
                "codetype":"item_code"
            })
        
        return pd.DataFrame(recs)
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        raise e

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

def get_group_weights():
    url = "https://cpi.mospi.gov.in/Weights_2012.aspx"
    session, soup, params = visit_page(url, fill_aspx=False)
    table = soup.find('table', {'id': 'Content1_GridView1'})

    table_df = pd.read_html(StringIO(str(table)))[0]
    
    table_df = table_df.drop(columns=['State'], errors='ignore')
    
    #table_df.columns = ['Group_Code', 'Group Label', 'Weight']
    #rename All India Group Combined Weight(Base:2012) to Weight    
    table_df.rename(columns={
        'Group': 'group_code',
        'SubGroup': 'subgroup_code',
        'Item': 'label',
        'Rural': 'rural',
        'Urban': 'urban',
    }, inplace=True)
    
    table_df['label'] = table_df['label'].astype(str).str.strip()
    table_df['group_code'] = table_df['group_code'].astype(str).str.strip()
    table_df['subgroup_code'] = table_df['subgroup_code'].astype(str).str.strip()    
    table_df['codetype'] = table_df.apply(lambda x: 'group_code' if pd.isna(x['subgroup_code']) or x['subgroup_code'] == '' else 'subgroup_code', axis=1)
    table_df['code'] = table_df.apply(lambda x: x['group_code'] if pd.isna(x['subgroup_code']) or x['subgroup_code'] == '' else x['subgroup_code'], axis=1)
    
    table_df = table_df.drop(columns=['group_code', 'subgroup_code'], errors='ignore')        
    
    #melt create a new column for rural and urban
    table_df = table_df.melt(id_vars=['code', 'label', 'codetype'], 
                             value_vars=['rural', 'urban'], 
                             var_name='region', 
                             value_name='weight')    

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
            months = ['January', 'February', 'March', 'April', 'May', 'June', 
                      'July', 'August', 'September', 'October', 'November', 'December']
            month_to_num = {month: i+1 for i, month in enumerate(months)}
            #convert Month columns to month numbers in a column
            df['month_num'] = df['Month'].map(month_to_num)
            #end of period date - use calendar to get last day
            df['period_end'] = df.apply(lambda row: datetime(row['Year'], row['month_num'], calendar.monthrange(row['Year'], row['month_num'])[1]).date(), axis=1)
            #drop month and year columns
            df = df.drop(['Month', 'Year', 'month_num'], axis=1)
            #rename columns
            df.rename(columns={
                'Item-Code': 'item_code',
                'Description': 'item_label',
                'Combined Inflation': 'CPI_YoY',
                'Status': 'status'
            }, inplace=True)
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
            df = df.drop(['State'], axis=1, errors='ignore')
            #convert Jan... Dec to month numbers in a column
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            month_to_num = {month: i+1 for i, month in enumerate(months)}
            #pivot the month columns to rows  -Group	Sub Group	Description
            df = df.melt(id_vars=['Year', 'Group', 'Sub Group', 'Description'], 
                                                                var_name='Month',                                                       
                                                                value_name='CPI_YoY')
            df['month_num'] = df['Month'].map(month_to_num)       
            #end of period date - use calendar to get last day
            df['period_end'] = df.apply(lambda row: datetime(row['Year'], row['month_num'], calendar.monthrange(row['Year'], row['month_num'])[1]).date(), axis=1)

            #drop year and month columns
            df = df.drop(['Year', 'Month', 'month_num'], axis=1)

            #rename columns
            df.rename(columns={
                'Group': 'group_code',
                'Sub Group': 'subgroup_code',
                'Description': 'subgroup_label'
            }, inplace=True)
            return df
        except ValueError as e:
            print(f"Error reading table: {e}")
            return None
    else:
        print("No data found in the table.")

cpi_api = "https://api.mospi.gov.in/api/cpi"

#returns {"year":, "month":} records starting from start_from to today
def get_periods(start_from):
    today = datetime.now()
    start_year = start_from.year
    start_month = start_from.month    
    periods = []
    for year in range(start_year, today.year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == today.year and month > today.month-1:
                continue
            periods.append({"year": year, "month": str(month)})
    
    return periods

async def _one_request(url, params):    
    async with httpx.AsyncClient() as client:
        try:            
            res = await client.get(url, params=params)        
            res.raise_for_status()  # Raise an error for bad responses
            if res.status_code == 200:
                return res.json()
            return None
        except Exception as e:
            return None

async def one_request(url, params=None, extra_params=None, metadata_df=None):
    """
    Make a single request to the given URL with optional parameters.
    Returns the response object.
    """
    # loop till you get all the pages
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"] 
    data = []
    codetype = extra_params.get("codetype", None)
    #setup the index for fuzzywuzzy using metadata_df['label']
    label_list = metadata_df['label'].tolist()
    try:        
        res = requests.get(url, params)
        res.raise_for_status()  # Raise an error for bad responses        
        
        if res.status_code == 200:
            res_json = res.json()
            #data.extend(res_json.get('data', []))
            pages = res_json.get('meta_data', {}).get("totalPages", 1)
            tasks = []
            for page in range(1, pages+1):
                params['page'] = page
                tasks.append(_one_request(url, {
                    **params,
                    "page": page
                }))
            #do tqdm over tasks            
            results = []
            batch_size = 25
            for i in tqdm(range(0, len(tasks), batch_size)):
                batch = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch)
                results.extend(batch_results)

            for res in tqdm(results, desc="Processing results"):
                if res is None:
                    continue                
                data.extend(res.get('data', []))                
            recs = []
            md_copy = metadata_df.copy() if metadata_df is not None else pd.DataFrame()            
            md_copy['lower_label'] = md_copy['label'].str.lower()            
            md_copy.set_index('lower_label', inplace=True)
            for d in data:
                codetype = extra_params.get("codetype", None)
                if metadata_df is not None:                    
                    label = None
                    #does not contain label
                    if codetype == 'subgroup_code' and not metadata_df['label'].str.contains(d.get('subgroup', ''), na=False).any():
                        codetype = 'group_code'
                        label = d.get('group')
                    elif codetype == 'subgroup_code':
                        label = d.get('subgroup', '')
                    elif codetype == 'item_code':
                        label = d.get('item', '')

                    #code_filter = (metadata_df['codetype'] == codetype) & (metadata_df['label'] == label)
                    #use fuzzywuzzy to get the closest match
                    #matching_label = metadata_df['label'].apply(lambda x: fuzzywuzzy.process.extractOne(label, [x])[0] if label else None)
                    code = md_copy[(md_copy.index == label.lower()) & (md_copy['codetype'] == codetype)]['code']                    
                    code = code.iloc[0] if not code.empty else None
                    if code is None:
                        print(f"No code found for label: {label} with codetype: {codetype}")
                        continue
                    year = d.get('year')
                    month_text = d.get('month')
                    month = months.index(month_text) + 1 if month_text in months else 1
                    if not year or not month_text:
                        print(f"Missing year or month for data: {d}")
                        continue

                    weight_filter = (metadata_df['codetype'] == codetype) & (metadata_df['code'].str.startswith(code))
                    weight = metadata_df[weight_filter]['weight'].sum()
                    cpi = d.get('index', 0)
                    cpi_yoy = d.get('inflation', 0)
                    if cpi is None or cpi_yoy is None:
                        print(f"Missing CPI or CPI_YoY for data: {d}")
                        continue
                    rec = {
                        "period_end": month_end(int(year), month),
                        "label": label,
                        "codetype": codetype,
                        "code": code,
                        "CPI": float(cpi),
                        "CPI_YoY": float(cpi_yoy),
                        "weight": weight
                    }
                    
                    recs.append(rec)
            if not recs:
                print("No data found for item inflation.")
                return None
            
            # Convert recs to DataFrame
            df = pd.DataFrame(recs) 
            return df
        else:
            print(f"Request failed with status code: {res.status_code}")
            return None
    except Exception as e:
        print(f"Error during request: {e}")
        return None

def month_end(year, month):
    """
    Returns the last day of the month for the given year and month.
    """
    return datetime(year, month, calendar.monthrange(year, month)[1]).date()

### Calculations
'''
Food_And_Beverages_Ex_Alcoholic_Beverages
Food_And_Beverages
'''
async def food_and_beverages_inflation(inflation_df=None):
    # Food And Beverages is a group in CPI
    aggregates = []
    constituents = []
    normalized_dimensions = []

    inf_copy = inflation_df.copy() if inflation_df is not None else pd.DataFrame()

    # Food and Beverages excluding Alcoholic Beverages
    dim = "Food_And_Beverages_Ex_Alcoholic_Beverages"
    group_code = "1."
    food_and_beverages_ex_alcohol_filter = (inf_copy['codetype'] == 'group_code') & (inf_copy['code'] == group_code)
    _df = inf_copy[food_and_beverages_ex_alcohol_filter].copy()
    _df['dimension'] = dim    
    aggregates.append(_df)
    _constituents = inf_copy[(inf_copy['codetype'] == 'item_code') & (inf_copy['code'].str.startswith(group_code+"."))].copy()    
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()    
    #only label, code, weight    
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')


    _constituents_subgroup = inf_copy[(inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'].str.startswith(group_code+"."))].copy()
    _constituents_subgroup['weight'] = _constituents_subgroup['weight'] / _constituents_subgroup['weight'].sum()
    _constituents_subgroup = _constituents_subgroup[['code', 'label', 'weight']].copy()
    _constituents_subgroup = _constituents_subgroup.to_dict('records')
    
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False),
            "subgroups": json.dumps(_constituents_subgroup, ensure_ascii=False)
        }        
    })

    # Food and Beverages including Alcoholic Beverages
    dim = "Food_And_Beverages"
    normalized_dimensions.append(dim)    
    alc_filter = (inf_copy['codetype'] == 'item_code') & (inf_copy['code'].str.startswith("2.1.01.1."))  # Alcoholic Beverages codes start with "
    alc_item_codes = inf_copy[alc_filter]['code'].tolist()
    fb_with_alc_filter = (inf_copy['codetype'] == 'group_code') & (inf_copy['code'].str.startswith("1.")) | (inf_copy['codetype'] == 'item_code') & (inf_copy['code'].isin(alc_item_codes))
    _df = inf_copy[fb_with_alc_filter].copy()
    
    _agg_df, _constituents = aggregate_inflation.calculate(_df, inf_copy)
    _agg_df['dimension'] = dim    
    aggregates.append(_agg_df)
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Grains
    dim = "Grains"
    normalized_dimensions.append(dim)
    pulses_and_cereals_codes =["1.1.01.", "1.1.08."]
    grains_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'].isin(pulses_and_cereals_codes))  # Pulses and Cereals codes
    _df = inf_copy[grains_filter].copy()
    
    _agg_df, _constituents = aggregate_inflation.calculate(_df, inf_copy)
    _agg_df['dimension'] = dim
    aggregates.append(_agg_df)
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })  

    # Cereals
    dim = "Cereals"
    subgroup_code = "1.1.01."    
    cereals_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Cereals codes start with "1.1.01"
    _df = inf_copy[cereals_filter].copy()
    _df['dimension'] = dim
    
    aggregates.append(_df)
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    #only select label, code, weight
    _constituents = _constituents[['code', 'label', 'weight']].copy()    
    _constituents = _constituents.to_dict('records')

    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Pulses
    dim = "Pulses"
    subgroup_code = "1.1.08."
    pulses_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Pulses codes start with "1.1.08"    
    _df = inf_copy[pulses_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Meat & Fish
    dim = "Meat_and_Fish" 
    subgroup_code = "1.1.02."  # Meat and Fish codes start with "1.1.02"
    _df = inf_copy[(inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)].copy()
    _df['dimension'] = dim
    aggregates.append(_df)
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Meat
    dim = "Meat"
    normalized_dimensions.append(dim)
    meat_filter = (inf_copy['codetype'] == 'item_code') & (inf_copy['code'].str.startswith("1.1.02."))  # Meat codes start with "1.1.02"
    _df = inf_copy[meat_filter].copy()    
    _agg_df, _constituents = aggregate_inflation.calculate(_df, inf_copy)
    _agg_df['dimension'] = dim
    aggregates.append(_agg_df)
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Fish
    dim = "Seafood"
    normalized_dimensions.append(dim)
    seafood_filter = (inf_copy['codetype'] == 'item_code') & (inf_copy['code'].str.startswith("1.1.03."))  # Seafood codes start with "1.1.03"
    _df = inf_copy[seafood_filter].copy()    
    _agg_df, _constituents = aggregate_inflation.calculate(_df, inf_copy)
    _agg_df['dimension'] = dim
    aggregates.append(_agg_df)
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Egg
    dim = "Eggs"
    normalized_dimensions.append(dim)
    subgroup_code = "1.1.03."
    egg_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Eggs codes start with "1.1.03"
    _df = inf_copy[egg_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()    
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Dairy
    dim = "Dairy"
    normalized_dimensions.append(dim)
    subgroup_code = "1.1.04."
    dairy_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Dairy codes start with "1.1.04"
    _df = inf_copy[dairy_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Edible Oils
    dim = "Edible_Oils"
    normalized_dimensions.append(dim)
    subgroup_code = "1.1.05."
    edible_oils_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Edible Oils codes start with "1.1.05"
    _df = inf_copy[edible_oils_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Fruits
    dim = "Fruits"
    normalized_dimensions.append(dim)
    subgroup_code = "1.1.06."
    fruits_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Fruits codes start with "1.1.06"
    _df = inf_copy[fruits_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Vegetables
    dim = "Vegetables"
    normalized_dimensions.append(dim)
    subgroup_code = "1.1.07."
    vegetables_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Vegetables codes start with "1.1.07"
    _df = inf_copy[vegetables_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Sugarbased Products
    dim = "Sugar_Products"
    normalized_dimensions.append(dim)
    subgroup_code = "1.1.09."
    sugar_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Sugar codes start with "1.1.09"
    _df = inf_copy[sugar_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Spices
    dim = "Spices"    
    subgroup_code = "1.1.10."
    spices_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Spices codes start with "1.1.10"
    _df = inf_copy[spices_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # Packaged Foods and Beverages
    dim = "Packaged_Foods"
    subgroup_code = "1.1.12."    
    packaged_foods_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Packaged Foods codes start with "1.1.12"
    _df = inf_copy[packaged_foods_filter].copy()
    _df['dimension'] = dim
    aggregates.append(_df)    
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })

    # NAB
    dim = "Non_Alcoholic_Beverages"
    subgroup_code = "1.2.11."
    nab_filter = (inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'] == subgroup_code)  # Non-Alcoholic Beverages codes start with "1.2.11"
    _df = inf_copy[nab_filter].copy()
    
    _df['dimension'] = dim
    aggregates.append(_df)
    _constituents = inf_copy[
        (inf_copy['codetype'] == 'item_code') &
        (inf_copy['code'].str.startswith(subgroup_code))
    ].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    constituents.append({
        "dimension": dim,
        "values":{
            "items":json.dumps(_constituents, ensure_ascii=False)
        } 
    })    

    agg_df = pd.concat(aggregates, ignore_index=True)
    
    agg_df = agg_df[['period_end', 'dimension', 'CPI_YoY']]

    agg_df['CPI_YoY'] = agg_df['CPI_YoY'].astype(float)
    agg_df['CPI_YoY'] = agg_df['CPI_YoY'].round(2)
    agg_df['CPI_YoY'] = agg_df['CPI_YoY'].fillna(0.0)

    agg_df['period_end'] = pd.to_datetime(agg_df['period_end'], errors='coerce')
    agg_df = agg_df.dropna(subset=['period_end'])
    agg_df = agg_df.sort_values(by='period_end').reset_index(drop=True)

    return agg_df, constituents, normalized_dimensions
    


async def item_inflation(start_from, metadata_df=None):
    item_url = f"{cpi_api}/getItemIndex"
    periods = get_periods(start_from)  
    years = list(set([period['year'] for period in periods]))
    months = list(set([period['month'] for period in periods]))
    params = {                
                "base_year": "2012",
                "Format": "json",
                "year":",".join([str(year) for year in years]),
                "month_code":",".join([str(month) for month in months]),
            }
    extra_params = {
        "codetype": "item_code"
    }
    return await one_request(item_url, params, extra_params, metadata_df)
    
async def group_inflation(start_from, metadata_df=None):
    group_url = f"{cpi_api}/getCPIIndex"
    periods = get_periods(start_from)  
    years = list(set([period['year'] for period in periods]))
    months = list(set([period['month'] for period in periods]))
    params = {                
                "base_year": "2012",
                "series": "Current",
                "Format": "json",                
                "state_code": "99",  # All India
                "sector_code": "3",  # Combined                
                "year":",".join([str(year) for year in years]),
                "month_code":",".join([str(month) for month in months]),
            }                  
    extra_params = {
        "codetype": "subgroup_code"
    }
    return await one_request(group_url, params, extra_params, metadata_df)
    
   


async def update():      
    '''
    metadata_df = get_metadata()            

    #item_weights_df = pd.read_csv('item_weights.csv', delimiter='|')
    item_weights_df = get_item_weights()
    
    # Step 1: Update weights for item_code rows using item_weights_df
    item_mask = metadata_df['codetype'] == 'item_code'
    
    metadata_df.loc[item_mask, 'weight'] = metadata_df.loc[item_mask].merge(
        item_weights_df[['item_code', 'weight']],
        left_on='code',             # this is the item_code in metadata_df
        right_on='item_code',       # this is the item_code in item_weights_df
        how='left'
    )['weight'].values

    #iterate and calculate group_code and subgroup_code weights
    group_dict = metadata_df[metadata_df['codetype'] == 'group_code'].set_index('code')['label'].to_dict()
    subgroup_dict = metadata_df[metadata_df['codetype'] == 'subgroup_code'].set_index('code')['label'].to_dict()
    
    for group_code in group_dict.keys():
        _group_code_filter = (metadata_df['codetype'] == "group_code") & (metadata_df['code'] == group_code)        
        metadata_df.loc[_group_code_filter, 'weight'] = item_weights_df[item_weights_df['item_code'].str.startswith(group_code)]['weight'].sum()        
    
    for subgroup_code in subgroup_dict.keys():
        _subgroup_code_filter = (metadata_df['codetype'] == "subgroup_code") & (metadata_df['code'] == subgroup_code)
        metadata_df.loc[_subgroup_code_filter, 'weight'] = item_weights_df[item_weights_df['item_code'].str.startswith(subgroup_code)]['weight'].sum()
            
    metadata_df['region'] = 'combined'
    #group_weights_df = get_group_weights()
    
    #concat metadata_df and group_weights_df
    #metadata_df = pd.concat([metadata_df, group_weights_df], ignore_index=True)
    
    # Final step: Save to CSV
    metadata_df.to_csv('metadata.csv', index=False, sep='|')
    
    start_from = datetime(2025, 1, 1)
    item_inflation_df, group_inflation_df = await asyncio.gather(item_inflation(start_from, metadata_df), group_inflation(start_from, metadata_df))
    
    inflation_df = pd.concat([item_inflation_df, group_inflation_df], ignore_index=True)        
    inflation_df['lower_label'] = inflation_df['label'].str.lower()
    inflation_df['region_type'] = None  #Rural and Urban Combined
    
    inflation_df.to_parquet('inflation.parquet', index=False, engine='pyarrow')    
    '''
    
    start_from = datetime(2020, 1, 1)  # Default start date
    #metadata_df = pd.read_csv('metadata.csv', delimiter='|')
    #load from parquet file
    inflation_df = pd.read_parquet('inflation.parquet', engine='pyarrow')       
    inflation_df['lower_label'] = inflation_df['label'].str.lower() 
    
    #convert to DATE - not datetime    
    #ensure period_end is datetime
    inflation_df['period_end'] = pd.to_datetime(inflation_df['period_end'], errors='coerce')
    #remove all rows before start_from
    inflation_df = inflation_df[inflation_df['period_end'].dt.date >= start_from.date()]    

    aggregates = []
    constituents = []
    normalized_dimensions = []
    
    #calculate subgroup_code weights - by using the code as startswith filter
    
    '''
    item_inflation_df = pd.read_csv('item_inflation.csv', delimiter='|')
    group_inflation_df = pd.read_csv('group_inflation.csv', delimiter='|')
    

    #add item_weights to item_inflation_df
    #only add the weights column - item_code is the key
    item_inflation_df = item_inflation_df.merge(item_weights[['item_code', 'weight']], on='item_code', how='left')    
    item_inflation_df['period_end'] = pd.to_datetime(item_inflation_df['period_end'], errors='coerce')    
    #save this file
    item_inflation_df.to_csv('item_inflation_with_weights.csv', index=False, sep='|')    
    #print(item_inflation_df[['period_end'] == datetime(2025,6, 30)])    
    print (food_and_beverages_inflation(item_inflation_df))

    print("Data updated successfully.")
    '''
    _agg_df, _constituents, _normalized_dimensions = await food_and_beverages_inflation(inflation_df)
    
    aggregates.append(_agg_df)
    constituents.extend(_constituents)
    normalized_dimensions.extend(_normalized_dimensions)

    agg_df = pd.concat(aggregates, ignore_index=True)
    

    #save the aggregated data to a CSV file
    agg_df.to_csv('inflation_aggregates.csv', index=False, sep='|')
    
    #save the constituents to a JSON file
    with open('inflation_constituents.json', 'w', encoding='utf-8') as f:
        json.dump(constituents, f, ensure_ascii=False, indent=4)
    
    #save the normalized dimensions to a JSON file
    with open('inflation_normalized_dimensions.json', 'w', encoding='utf-8') as f:
        json.dump(normalized_dimensions, f, ensure_ascii=False, indent=4)



    #df.to_csv('food_and_beverages_inflation.csv', index=False, sep='|')
    





    