from apis import india_mospi
from tools import stubborn_browser, parquet_handler
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
from io import StringIO
from datetime import datetime
import calendar
#from curl_cffi import requests
import httpx
import os
import requests
import asyncio
import json
from tqdm import tqdm
from tools import s3ops, parquet_handler, helpers
from tools.records import DatabankRecord

dataset = "CPI"
country = "IN"




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
    extras_key = f"{dataset}/{country}/metadata"
    
    #load from s3ops load_extras
    metadata_json = s3ops.load_extras(extras_key)
    if metadata_json is not None:        
        return pd.read_json(StringIO(metadata_json), orient='records')

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
        
        _metadata_df = pd.DataFrame(recs)

        #save to s3
        s3ops.save_extras(extras_key, json.dumps(_metadata_df.to_json(orient='records')))
        return pd.DataFrame(recs)
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        raise e

# Get Itemised Weights - This is needed to reorganise CPI according to custom groupings
def get_item_weights():
    extras_key = f"{dataset}/{country}/item_weights"

    item_weights_json = s3ops.load_extras(extras_key)
    if item_weights_json is not None:
        return pd.read_json(StringIO(item_weights_json), orient='records')

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

    s3ops.save_extras(extras_key, json.dumps(table_df.to_json(orient='records')))

    return table_df

def get_aggdf(aggregates):
    agg_df = pd.concat(aggregates, ignore_index=True)
    if 'ticker' in agg_df.columns:
        agg_df = agg_df[['ticker', 'period_end', 'dimensions', 'CPI_YoY']]
    else:
        agg_df = agg_df[['period_end', 'dimensions', 'CPI_YoY']]
    
    agg_df['CPI_YoY'] = agg_df['CPI_YoY'].astype(float)
    agg_df['CPI_YoY'] = agg_df['CPI_YoY'].round(2)
    agg_df['CPI_YoY'] = agg_df['CPI_YoY'].fillna(0.0)

    agg_df['period_end'] = pd.to_datetime(agg_df['period_end'], errors='coerce')
    agg_df = agg_df.dropna(subset=['period_end'])    

    return agg_df

def month_end(year, month):
    """
    Returns the last day of the month for the given year and month.
    """
    return datetime(year, month, calendar.monthrange(year, month)[1]).date()

def process_records(data, extra_params=None, metadata_df=None):
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
            month = d.get('month')            
            if not year or not month:
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


### Calculations
async def overall_inflation(inflation_df=None):
    aggregates = []
    constituents = []    

    inf_copy = inflation_df.copy() if inflation_df is not None else pd.DataFrame()

    # No dimension - which means overall inflation
    group_code = "0."
    _filter = (inflation_df['codetype'] == 'group_code') & (inflation_df['code'] == group_code)

    _df = inf_copy[_filter].copy()
    _df['dimensions'] = None
    aggregates.append(_df)
    
    #all items
    _constituents = inf_copy[(inf_copy['codetype'] == 'item_code')].copy()
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()
    #only label, code, weight
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')

    _constituents_subgroup = inf_copy[(inf_copy['codetype'] == 'subgroup_code')].copy()
    _constituents_subgroup['weight'] = _constituents_subgroup['weight'] / _constituents_subgroup['weight'].sum()
    _constituents_subgroup = _constituents_subgroup[['code', 'label', 'weight']].copy()
    _constituents_subgroup = _constituents_subgroup.to_dict('records')

    constituents.append({
        "dimensions": None,
        "txt":{
            "items": _constituents,
            "subgroups": _constituents_subgroup
        }
    })

    aggregates.append(_df)
    _constituents = inf_copy[(inf_copy['codetype'] == 'item_code') & (inf_copy['code'].str.startswith(group_code))].copy()    
    _constituents['weight'] = _constituents['weight'] / _constituents['weight'].sum()    
    #only label, code, weight    
    _constituents = _constituents[['code', 'label', 'weight']].copy()
    _constituents = _constituents.to_dict('records')
    

    _constituents_subgroup = inf_copy[(inf_copy['codetype'] == 'subgroup_code') & (inf_copy['code'].str.startswith(group_code))].copy()
    _constituents_subgroup['weight'] = _constituents_subgroup['weight'] / _constituents_subgroup['weight'].sum()
    _constituents_subgroup = _constituents_subgroup[['code', 'label', 'weight']].copy()

    #change labels - to match dimensions being used
    label_conversions = {
        "Food and Beverages":"Food_And_Beverages_Ex_Alcoholic_Beverages",
        "Pan, Tobacco and Intoxicants":"Intoxicants_And_Alcoholic_Beverages",
        "Clothing and Footwear":"Apparel",
        "Housing":"Housing_Ex_Electricity",
        "Fuel and Light":"Electricity_And_Household_Fuel",
        "Miscellaneous":"Miscellaneous"
    }
    
    _constituents_subgroup['label'] = _constituents_subgroup['label'].replace(label_conversions)
    _constituents_subgroup = _constituents_subgroup.to_dict('records')

    constituents.append({
        "dimensions": None,
        "txt":{
            "items": _constituents,
            "subgroups": _constituents_subgroup
        }
    })

    agg_df = get_aggdf(aggregates)    
    
    return agg_df, constituents

def aggregate_inflation(df_orig):
    df = df_orig.copy()
    period_ends = df['period_end'].unique()
    labels = df['label'].unique()    
    
    delete_periods = []
    
    for label in labels:
        for period in period_ends:
            if period in delete_periods:
                continue
            # Check if the label and period combination exists in the dataframe
            if not ((df['label'] == label) & (df['period_end'] == period)).any():
                # If it doesn't exist, add the period to the delete list
                delete_periods.append(period)                
    
    # Remove periods that are not present in the dataframe
    df = df[~df['period_end'].isin(delete_periods)]        

    # Calculate past year CPI - use that and the weights to calculate the inflation 
    df['CPI_Past_Year'] = df['CPI']/((100+df['CPI_YoY'])/100)
    df['basket_weights'] = df['weight']/df['weight'].sum()

    # Group by period_end and calculate aggregate index for CPI_Past_Year and CPI using weights    
    # Using Laspeyres index formula    
    agg_df = df.groupby('period_end').apply(
        lambda x: pd.Series({
            'CPI_Past_Year': (x['CPI_Past_Year'] * x['basket_weights']).sum(),
            'CPI': (x['CPI'] * x['basket_weights']).sum(),
            'weight': x['weight'].sum(),
            'period_end': x['period_end'].iloc[0],
        })).reset_index(drop=True)   
    
    agg_df['CPI_YoY'] = agg_df['CPI']/agg_df['CPI_Past_Year'] * 100 - 100

    #drop CPI_Past_Year
    agg_df.drop(columns=['CPI_Past_Year'], inplace=True)
    
    agg_df['period_end'] = pd.to_datetime(agg_df['period_end'])
    
    return agg_df

async def _dimension(item, inflation_df):    
    label = item['label']
    as_reported = item.get('as_reported', False)
    inter_country_comparison = item.get('inter_country_comparison', False)
    filters = item.get('filters', [])
    exclude_filters = item.get('exclude_filters', [])
    category = item.get("category",None)
    inf_copy = inflation_df.copy()    

    # each filter is a tuple of (codetype, code) --- each of them should be an | 
    _filter = None    
    
    # Apply inclusion filters
    for codetype, code in filters:
        cond = (inf_copy['codetype'] == codetype) & (inf_copy['code'].str.startswith(code))
        _filter = cond if _filter is None else (_filter | cond)

    # Apply exclusion filters
    for codetype, code in exclude_filters:
        cond = (inf_copy['codetype'] == codetype) & (~inf_copy['code'].str.startswith(code))
        _filter = cond if _filter is None else (_filter & cond)

    _df = inf_copy[_filter].copy()
    
    records = []
    if not as_reported:
        _df = aggregate_inflation(_df)

    for index, row in _df.iterrows():
        rec = DatabankRecord(
            ticker="CPI",
            country="IN",
            metric="YoY",
            unit='PERCENT',
            value=row["CPI_YoY"],
            period_end=row["period_end"],
            dimensions=[label] if label else None,
            categories={label:category} if category else None,
            period_span="M",
            as_reported=as_reported,
            inter_country_comparison=inter_country_comparison,
            created_by="MacroSearchEngine"
        )
        rec.prep_for_insert()
        records.append(rec.rec)    
    return records

'''
constituents
- ConsumptionCategory_AsReported
- ConsumptionCategory_Adjusted
- SubCategoryOf<xxx>

recs
- ticker: CPI
- dimensions - [<ConsumptionCategory] or [<ConsumptionSubCategory>]

if its not reported - value is calculated
'''

async def calculate_dimensions(inflation_df=None):    
    #These are all the values that will be calculated
    items = [
        # All reported inflations go here - Main Groups
        {
            #This is the overall inflation
            "label":None,
            "as_reported": True,
            "inter_country_comparison": True,
            "category":None,
            "filters":[
                ("group_code", "0.")
            ]            
        },
        {
            #This is the CFPI
            "label":"CFPI",
            "as_reported": True,
            "inter_country_comparison": True,
            "category":None,
            "filters":[
                ("group_code", "7.")
            ]            
        },
        {            
            # Ex Food, Energy & Housing
            "label":"Core_CPI",            
            "inter_country_comparison": True,
            "category":None,
            "exclude_filters":[
                ("item_code","1."), 
                ("item_code", "6.1.03.2.1."),
                ("item_code", "2.1.01.1.")
            ]            
        },
        {            
            # Ex Food, Energy & Housing
            "label":"Supercore_CPI",
            "inter_country_comparison": True,
            "category":None,            
            "exclude_filters":[
                ("item_code","1."), 
                ("item_code", "6.1.03.2.1."),
                ("item_code", "2.1.01.1."),
                ("item_code", "4."),
                ("item_code", "5.1.01.1.")
            ]            
        },
        {
            "label":"Food_And_Beverages_Ex_Alcoholic_Beverages",
            "as_reported": True,
            "category":"ConsumptionCategory",
            "filters":[
                ("group_code", "1.")
            ]
        },
        {
            "label":"Intoxicants_And_Alcoholic_Beverages",
            "as_reported": True,
            "category":"ConsumptionCategory",
            "filters":[
                ("group_code", "2."),
            ]
        },
        {
            "label":"Apparel",
            "as_reported": True,
            "inter_country_comparison": True,
            "category":"ConsumptionCategory",
            "filters":[
                ("group_code", "3.")
            ]
        },
        {
            "label":"Housing_Ex_Electricity",
            "as_reported": True,
            "category":"ConsumptionCategory",
            "filters":[
                ("group_code", "4.")
            ]
        },
        {
            "label":"Electricity_And_Household_Fuel",
            "as_reported": True,            
            "category":"ConsumptionCategory",
            "filters":[
                ("group_code", "5.")
            ]
        },
        {
            "label":"Miscellaneous",
            "category":"ConsumptionCategory",            
            "as_reported": True,
            "filters":[
                ("group_code", "6.")
            ]
        },        
        {            
            "label":"Cereals",            
            "as_reported": True,
            "category":"ConsumptionCategory",
            "filters":[
                ("subgroup_code", "1.1.01.")
            ]
        },        
        {            
            "label":"Meat_And_Fish",            
            "as_reported": True,
            "category":"ConsumptionCategory",
            "filters":[
                ("subgroup_code", "1.1.02.")
            ]
        },
        {
            "label":"Eggs",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.03.")]
        },
        {
            "label":"Dairy",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.04.")]
        },
        {
            "label":"Edible_Oils",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.05.")]
        },
        {
            "label":"Fruits",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.06.")]
        },
        {
            "label":"Vegetables",            
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.07.")]
        },
        {
            "label":"Pulses",            
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[
                ("subgroup_code", "1.1.08.")
            ]
        },
        {           
            "label":"Sugar_Products",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.09.")]
        },
        {
            "label":"Spices",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.10.")]
        },
        {
            "label":"Packaged_Foods",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[("subgroup_code", "1.1.12.")]
        },
        {            
            "label":"Non_Alcoholic_Beverages",
            "category":"ConsumptionCategory",
            "as_reported": True,            
            "filters":[
                ("subgroup_code", "1.2.11.")
            ]
        },
        {
            "label":"Clothing",
            "category":"ConsumptionCategory",
            "as_reported": True,            
            "filters":[
                ("subgroup_code", "3.1.01.")
            ]
        },
        {
            "label":"Footwear",
            "category":"ConsumptionCategory",
            "as_reported": True,            
            "filters":[
                ("subgroup_code", "3.1.02.")
            ]
        },
        {
            "label":"Household_Goods_And_Services",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[
                ("subgroup_code", "6.1.01.")
            ]            
        },
        {
            "label":"Healthcare",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "inter_country_comparison": True,
            "filters":[                
                ("subgroup_code", "6.1.02.")
            ]
        },
        {
            "label":"Transport_And_Communication",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[
                ("subgroup_code", "6.1.03.")
            ]            
        },
        {
            "label":"Recreation_And_Entertainment",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "filters":[
                ("subgroup_code", "6.1.04.")
            ]
        },
        {
            "label":"Education",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "inter_country_comparison": True,
            "filters":[
                ("subgroup_code", "6.1.05.")
            ]
        },        
        {
            "label":"Personal_Care",
            "category":"ConsumptionCategory",
            "as_reported": True,            
            "filters":[
                ("subgroup_code", "6.1.06.")
            ]
        },        
        {
            "label":"Grains",
            "category":"ConsumptionCategory",
            "inter_country_comparison": True,
            "filters":[
                ("subgroup_code", "1.1.01."),
                ("subgroup_code", "1.1.08.")
            ]
        },
        {
            "label":"Meat",
            "category":"ConsumptionCategory",
            "inter_country_comparison": True,           
            "filters":[
                ("item_code", "1.1.02.1.")
            ]
        },
        {
            "label":"Seafood",
            "category":"ConsumptionCategory",
            "inter_country_comparison": True,
            "filters":[("item_code", "1.1.02.2.")]
        },        
        {
            "label":"Housing",
            "category":"ConsumptionCategory",
            "inter_country_comparison": True,
            "filters":[
                ("group_code", "4."),
                ("item_code", "5.1.01.1.")
            ]
        },
        {
            "label":"Home_Appliances",            
            "category":"ConsumptionCategory",
            "inter_country_comparison": True,
            "filters":[
                ("subgroup_code", "6.1.01.3."),
                ("subgroup_code", "6.1.01.4."),
                ("subgroup_code", "6.1.01.5."),
            ]            
        },
        {
            "label":"Transport",            
            "category":"ConsumptionCategory",
            "inter_country_comparison": True,
            "filters":[
                ("subgroup_code", "6.1.03.3.")
            ]
        },
        {
            "label":"Energy",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "inter_country_comparison": True,
            "filters":[
                ("item_code", "6.1.03.2.1.")
            ]
        },
        {
            "label":"Communication",
            "category":"ConsumptionCategory",
            "as_reported": True,
            "inter_country_comparison": True,
            "filters":[                
                ("item_code", "6.1.03.5.")
            ]
        }
    ]

    records = []
    for item in items:
        recs = await _dimension(item, inflation_df)
        records.extend(recs)

#async def update():
async def update(overwrite_history=False, start_from=None, update_weights=True):   
    config = await helpers.load_config(os.path.dirname(__file__), dataset)
    # Get codes for groups, subgroups and items
    metadata_df = get_metadata()
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
    
    updated_dates = []

    if not start_from:
        start_from = config.get("start_date")
        start_from = datetime.strptime(start_from, "%Y-%m-%d")
    
    if not overwrite_history:
        updated_dates = await helpers.get_updated_dates(config["s3_prefix"], dataset)
    
    item_inflation_records, group_inflation_records = await asyncio.gather(india_mospi.get("CPI_Items", start_from, datetime.now(), updated_dates), india_mospi.get("CPI", start_from, datetime.now(), updated_dates))

    item_inflation_df = process_records(item_inflation_records, extra_params={"codetype": "item_code"}, metadata_df=metadata_df)    
    group_inflation_df = process_records(group_inflation_records, extra_params={"codetype": "group_code"}, metadata_df=metadata_df)

    inflation_df = pd.concat([item_inflation_df, group_inflation_df], ignore_index=True)        
    inflation_df['lower_label'] = inflation_df['label'].str.lower()
    inflation_df['period_end'] = pd.to_datetime(inflation_df['period_end'], errors='coerce')
    #remove all rows before start_from
    inflation_df = inflation_df[inflation_df['period_end'].dt.date >= start_from.date()]    
    inflation_df['region_type'] = None  #Rural and Urban Combined

        
    # DEBUG
    inflation_df.to_parquet('inflation.parquet', index=False, engine='pyarrow')        
    inflation_df = pd.read_parquet('inflation.parquet', engine='pyarrow')           

    
    
    #convert to DATE - not datetime    
    #ensure period_end is datetime    
    
    
    
    #calculate subgroup_code weights - by using the code as startswith filter
    


    #where there are no dimensions in constituents
    
    constituents_df = pd.DataFrame(constituents+_constituents)
    constituents_df['region'] = 'IN'
    constituents_df['ticker'] = 'CPI'
    constituents_df['unit'] = 'JSON'
    constituents_df['metric'] = 'Constituents'
    print (constituents_df.columns)
    #fillna for dimensinos    
    constituents_df['dimensions'] = constituents_df['dimensions'].apply(lambda x: f"[{x}]" if x is not None else None)
    #max
    constituents_df['period_end'] = agg_df['period_end'].max() if not agg_df.empty else pd.NaT
    #jsonify
    constituents_df['txt'] = constituents_df['txt'].apply(lambda x: json.dumps(x, ensure_ascii=False))
    
    final_df = pd.concat([agg_df, constituents_df], ignore_index=True)

    #save the aggregated data to a CSV file
    #agg_df.to_csv('inflation_aggregates.csv', index=False, sep='|')
    #load config.json
    with open('datasets/Inflation/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

    parquet_handler.save(final_df, config.get("s3_path"))




    