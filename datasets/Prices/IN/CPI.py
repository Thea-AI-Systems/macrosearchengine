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

async def visit_page(url, fill_aspx=True):    
    session = await stubborn_browser.seed_session(url="https://cpi.mospi.gov.in")
    print (f"Created a session: {session}")
    res = await stubborn_browser.get({"url":url, "session":session})
    print (f"Got response: {res.status_code} for {url}")
    
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
        return pd.DataFrame(json.loads(json.loads(metadata_json)))

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
async def get_item_weights():
    extras_key = f"{dataset}/{country}/item_weights"

    item_weights_json = s3ops.load_extras(extras_key)
    if item_weights_json is not None:
        return pd.DataFrame(json.loads(json.loads(item_weights_json)))

    print ("Fetching item weights from MOSPI...")
    url = "https://cpi.mospi.gov.in/Weight_AI_Item_Combined_2012.aspx"    
    session, soup, params = await visit_page(url, fill_aspx=False)
    print ("Fetched item weights from MOSPI...")
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

def month_end(year, month):
    """
    Returns the last day of the month for the given year and month.
    """
    return datetime(year, month, calendar.monthrange(year, month)[1]).date()

def publishing_date(year, month):
    """
    Returns the publishing date for the given year and month.
    12th of next month
    """
    if month == 12:
        return datetime(year + 1, 1, 12).date()
    return datetime(year, month + 1, 12).date()

def process_records(data, extra_params=None, metadata_df=None):
    if metadata_df is None:
        return None

    recs = []
    md_copy = metadata_df.copy()
    md_copy['lower_label'] = md_copy['label'].str.lower()            
    md_copy.set_index('lower_label', inplace=True)
    for d in data:
        codetype = extra_params.get("codetype", None)        
        label = None
        #does not contain label        
        if codetype == 'subgroup_code' and not metadata_df['label'].str.contains(d.get('subgroup', ''), na=False).any():
            codetype = 'group_code'
            label = d.get('group')
        elif codetype == 'subgroup_code':
            label = d.get('subgroup', '')
        elif codetype == 'item_code':
            label = d.get('item', '')

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
            "updated_on": publishing_date(int(year), month),
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


def aggregate_inflation(df_orig):
    df = df_orig.copy()

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
    label = item.get("dimension", None)    
    as_reported = item.get('as_reported', False)    
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
        ticker = "CPI"
        if category == 'Index':
            ticker = label

        rec = DatabankRecord(
            dataset=dataset,
            ticker=ticker,
            country="IN",
            metric="YoY",
            unit='PERCENT',
            value=row["CPI_YoY"],
            period_end=row["period_end"],            
            period_span="M",
            as_reported=as_reported,
            inter_country_comparison=item.get('inter_country_comparison', False),
            created_by="MacroSearchEngine"
        )
        
        if label is not None and category != 'Index':
            rec.add_dimension(label, category)

        rec.prep_for_insert()
        
        records.append(rec.rec)    
    return records

def get_item_inflation(inflation_df):
    #for all items by item_code
    item_inflation = inflation_df[inflation_df['codetype'] == 'item_code']
    for index, row in item_inflation.iterrows():
        rec = DatabankRecord(
            dataset=dataset,
            ticker="CPI",
            country="IN",
            metric="YoY",
            unit='PERCENT',
            value=row["CPI_YoY"],
            period_end=row["period_end"],
            updated_on=row["updated_on"],
            period_span="M",
            as_reported=True,
            inter_country_comparison=False,
            created_by="MacroSearchEngine"
        )
        
        rec.add_dimension(row['label'], "ConsumptionItem")
        rec.prep_for_insert()        
        yield rec.rec
    

def read_meta_file(filename):
    items = pd.read_csv(filename, delimiter='\t', quotechar="'", dtype=str)
    
    #convert nan to None
    items = items.where(pd.notnull(items), None)
    items = items.to_dict(orient='records')    
    
    #convert subgroup_code:1.1.01.;subgroup_code:1.1.08. to tuples in filters
    for i, item in enumerate(items):
        _filters = str(item.get("filters",""))        
        if _filters is not None and ':' in _filters:
            _filters = _filters.split(";")
            for j, f in enumerate(_filters):
                f = f.strip()
                _filters[j] = (f.split(":")[0], f.split(":")[1])
            items[i]['filters'] = _filters

        _filters = str(item.get("exclude_filters", ""))
        if _filters is not None and ':' in _filters:
            _filters = _filters.split(";")            
            for j, f in enumerate(_filters):
                f = f.strip()
                _filters[j] = (f.split(":")[0], f.split(":")[1])
            items[i]['exclude_filters'] = _filters

        _filters = str(item.get("parent_filters", ""))
        if _filters is not None and ':' in _filters:
            _filters = _filters.split(";")            
            for j, f in enumerate(_filters):
                f = f.strip()
                _filters[j] = (f.split(":")[0], f.split(":")[1])
            items[i]['parent_filters'] = _filters

        _filters = str(item.get("parent_filters_exclude", ""))
        if _filters is not None and ':' in _filters:
            _filters = _filters.split(";")            
            for j, f in enumerate(_filters):
                f = f.strip()
                _filters[j] = (f.split(":")[0], f.split(":")[1])
            items[i]['parent_filters_exclude'] = _filters
        
        if items[i]['filters'] is None:
            items[i]['filters'] = []

        if items[i].get('exclude_filters', None) is None:
            items[i]['exclude_filters'] = []

        if items[i].get('parent_filters', None) is None:
            items[i]['parent_filters'] = []

        if items[i].get('parent_filters_exclude', None) is None:
            items[i]['parent_filters_exclude'] = []

    return items

async def get_weights():
    metadata_df = get_metadata()    
    item_weights_df = await get_item_weights()
    
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
    return metadata_df 

def get_constituent_records(inflation_df=None):
    items = read_meta_file('datasets/Prices/IN/cpi_constituents.csv')
    #collect ticker, parent_dimension, breakdown_dimension tuples
    tpb_tuples = []
    for item in items:
        ticker = item.get('ticker', None)
        parent_dimension = item.get('parent_dimension', None)
        breakdown_dimension = item.get('breakdown_dimension', None)
        if ticker and parent_dimension and breakdown_dimension:
            tpb_tuples.append((ticker, parent_dimension, breakdown_dimension))
    
    #uniquify
    tpb_tuples = list(set(tpb_tuples))
    
    inf_copy = inflation_df.copy() if inflation_df is not None else pd.DataFrame()
    baserec = DatabankRecord(
                dataset=dataset,                
                country="IN",
                metric="Constituents",
                unit='JSON',
                value=None,                
                created_by="MacroSearchEngine"
            )
    constituent_records = []
    for tpb_tuple in tpb_tuples:
        ticker, parent_dimension, breakdown_dimension = tpb_tuple
        filtered_items = [item for item in items if item.get('ticker') == ticker and item.get('parent_dimension') == parent_dimension and item.get('breakdown_dimension') == breakdown_dimension]
        all_dates = inf_copy['period_end'].unique() if not inf_copy.empty else []
        for date in all_dates:
            constituent_rec = baserec.clone()
            constituent_rec.update(ticker=ticker, period_end=date, updated_on=publishing_date(date.year, date.month))
            overall_cpi = inf_copy[(inf_copy['codetype'] == 'group_code') & (inf_copy['code'].str.startswith('0.')) & (inf_copy['period_end'] == date)]['CPI']

            for _item in filtered_items:                
                #add dimension and category for the first time only
                if constituent_rec.rec.get('dimensions', None) is None:
                    constituent_rec.add_dimension(_item.get('breakdown_dimension'), _item.get('breakdown_category', None))
                    if _item.get('parent_dimension', None) is not None:
                        constituent_rec.add_dimension(_item.get('parent_dimension'), _item.get('parent_category', None))
                
                parent_weight = 1
                if _item.get('parent_filters', None) is not None or _item.get('parent_filters_exclude', None) is not None:
                    _filter = None
                    for codetype, code in _item.get('parent_filters', []):
                        cond = (inf_copy['codetype'] == codetype) & (inf_copy['code'].str.startswith(code)) & (inf_copy['period_end'] == date)
                        _filter = cond if _filter is None else (_filter | cond)

                        # Apply exclusion filters
                    for codetype, code in _item.get('parent_filters_exclude', []):
                        cond = (inf_copy['codetype'] == codetype) & (~inf_copy['code'].str.startswith(code)) & (inf_copy['period_end'] == date)
                        _filter = cond if _filter is None else (_filter & cond)                        
                        
                        
                    filtered_df = inf_copy[_filter] if _filter is not None else inf_copy
                    #if filtered_df is not empty, calculate parent_weight
                    if not filtered_df.empty:                        
                        parent_weight = (filtered_df['CPI'] * filtered_df['weight']).sum() / overall_cpi.iloc[0] if not overall_cpi.empty else 1
                        parent_weight = parent_weight/100
                        
                
                filters = _item.get('filters', [])                    
                # Apply inclusion filters    
                weight = 0
                for codetype, code in filters:                    
                    cond = (inf_copy['codetype'] == codetype) & (inf_copy['code'].str.startswith(code)) & (inf_copy['period_end'] == date)
                    filtered_df = inf_copy[cond]
                    if not filtered_df.empty:
                        #laspeyres index - multiple index weight with index level
                        weight += (filtered_df['CPI'] * filtered_df['weight']).sum()
                # scale_weight based on either the category or overall weights
                if not overall_cpi.empty and weight != 0:
                    weight = weight / overall_cpi.iloc[0]                    
                    constituent_rec.add_constituent(ticker=ticker, value=weight, dimensions=[_item.get('constituent_label', None)])

            constituent_rec.prep_for_insert()
            constituent_records.append(constituent_rec.rec)
    
    #also add a constituent for all itemcodes
    itemcodes = inf_copy[inf_copy['codetype'] == 'item_code']
    #for each date    
    for date in itemcodes['period_end'].unique():
        constituent_rec = baserec.clone()
        constituent_rec.update(ticker="CPI", period_end=date)
        constituent_rec.add_dimension("ByConsumptionItem", "BreakdownType")
        overall_cpi = inf_copy[(inf_copy['codetype'] == 'group_code') & (inf_copy['code'].str.startswith('0.')) & (inf_copy['period_end'] == date)]['CPI']

        for index, row in itemcodes[itemcodes['period_end'] == date].iterrows():
            #add dimension and category for the first time only
            if constituent_rec.rec.get('dimensions', None) is None:
                constituent_rec.add_dimension(row['label'], "ConsumptionItem")
            
            weight = (row['CPI'] * row['weight']) / overall_cpi.iloc[0] if not overall_cpi.empty else 0
            constituent_rec.add_constituent(ticker="CPI", value=weight, dimensions=[row['label']])

        constituent_rec.prep_for_insert()
        constituent_records.append(constituent_rec.rec)

    return constituent_records

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

#async def update():
async def update(overwrite_history=False, start_from=None):          
    calculations = read_meta_file('datasets/Prices/IN/cpi_dimensions.csv')
    weights_df = await get_weights()
    
    config = await helpers.load_config(os.path.dirname(__file__), dataset)
    
    
    updated_dates = []

    if not start_from:
        start_from = config.get("start_date")
        start_from = datetime.strptime(start_from, "%Y-%m-%d")
    
    if not overwrite_history:
        updated_dates = await helpers.get_updated_dates(config["s3_prefix"], dataset)

    print (updated_dates)
    input("Press Enter to continue...")
    
    #process for each month
    monthly_dates = pd.date_range(start=start_from, end=datetime.now(), freq='M').tolist()
    #delete dates that are in the month of updated_dates - go by year month
    delete_monthly_dates = []
    for i, m in enumerate(monthly_dates):        
        for updated_date in updated_dates:
            if m.year == updated_date.year and m.month == updated_date.month:
                delete_monthly_dates.append(i)
                break
        
    #remove those dates
    monthly_dates = [m for i, m in enumerate(monthly_dates) if i not in delete_monthly_dates]

    for dt in monthly_dates:        
        month_start = datetime(dt.year, dt.month, 1)
        month_end = month_start.replace(day=calendar.monthrange(month_start.year, month_start.month)[1])
        #update start_from to the month_end
        item_inflation_records, group_inflation_records = await asyncio.gather(
            india_mospi.get("CPI_Items", month_start, month_end, updated_dates), 
            india_mospi.get("CPI", month_start, month_end, updated_dates)
        )
        if not item_inflation_records or not group_inflation_records:
            print(f"No data found for {dataset} for {dt}. Skipping...")
            continue        

        item_inflation_df = process_records(item_inflation_records, extra_params={"codetype": "item_code"}, metadata_df=weights_df)    
        group_inflation_df = process_records(group_inflation_records, extra_params={"codetype": "subgroup_code"}, metadata_df=weights_df)
        inflation_df = pd.concat([item_inflation_df, group_inflation_df], ignore_index=True)        

        inflation_df['lower_label'] = inflation_df['label'].str.lower()
        inflation_df['period_end'] = pd.to_datetime(inflation_df['period_end'], errors='coerce')
        #remove all rows before start_from
        inflation_df = inflation_df[inflation_df['period_end'].dt.date >= start_from.date()]    
        inflation_df['region'] = None  #Rural and Urban Combined
            
        # DEBUG
        inflation_df.to_parquet('inflation.parquet', index=False, engine='pyarrow')
        inflation_df.to_csv('inflation.csv', index=False)    

        inflation_df = pd.read_parquet('inflation.parquet', engine='pyarrow')
        
        records = []
        for item in calculations:
            recs = await _dimension(item, inflation_df)
            records.extend(recs)    
        records.extend(get_item_inflation(inflation_df))
        records.extend(get_constituent_records(inflation_df))

        #save as .csv
        df = pd.DataFrame(records)
        if df.empty:
            print(f"No valid records found for {dataset} from {start_from} to now.")
            return       
        
        await parquet_handler.save(df, f'{config["s3_prefix"]}/{country}')




    