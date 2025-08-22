import pandas as pd
from apis import india_rbi
from datetime import datetime
from tqdm import tqdm
from dateutil import parser
from tools import helpers, parquet_handler
from tools.records import DatabankRecord
import re

country = "IN"

def get_period_end_by_header_cell_text(table):
    period_end_search_terms = ["Outstanding as on", "Outstandingas on ", 'As on']    
    
    try:
        #find date by date_split_on
        all_cells = table.find_all(["th", "td"])        
        for cell in all_cells:
            for q in period_end_search_terms:
                if cell.text and q.lower() in helpers.adj_text(cell.text).lower():
                    table_dt_el = cell
                    text_cleaned = helpers.adj_text(table_dt_el.get_text(separator=" "))  # <-- this handles <br>
                    period_end = helpers.adj_text(text_cleaned).split(q)[1].strip()  
                    period_end = period_end.replace("#", "").replace(",", ", ")                                        
                    period_end = parser.parse(period_end.strip())                               
                    return period_end
    except Exception as e:        
        print (f"Cannot find date in table: {e}")
        raise e
    
    raise ValueError("Cannot find date in table")

def get_period_end_last_col(table, offset_from_end):
    yyyy_row = 1
    mmm_dd_row = 2

    non_empty_tr = [tr for tr in table.find_all("tr") if len(tr.find_all("td")) > 0]            
    #dt_year = non_empty_tr[yyyy_row].find_all("td")[val_col].text.strip()
    #dt_month = non_empty_tr[mmm_dd_row].find_all("td")[val_col].text.strip()
    
    dt_year = non_empty_tr[yyyy_row].find_all("td")[offset_from_end].text.strip()
    dt_month = non_empty_tr[mmm_dd_row].find_all("td")[offset_from_end].text.strip()
    

    if "." in dt_month:
        [month, day] = dt_month.split(".")
    else:
        try:
            [month, day] = dt_month.split(" ")
        except Exception as e:                   
            raise e
    
    return parser.parse(f"{dt_year} {month} {day}")


def get_last_col_value(table_row):
    all_cells = table_row.find_all("td")
    val_col = None
    offset_from_end = 0
    #take only the last value column
    for i in range(len(all_cells)-1, 0, -1):
        try:
            val_range = all_cells[i].text.strip()
            if "/" in val_range:                        
                value = helpers.to_numeric(val_range.split("/")[-1].strip())     
            else:
                value = helpers.to_numeric(val_range)               
        except Exception as e:
            print(f"Error converting value to numeric: {e}")
            print (all_cells, i)
            raise e

        if value is not None:
            val_col = i
            break

    if value is None:        
        return None

    offset_from_end = val_col - len(all_cells)
    return value, offset_from_end


async def process_table(table_rec, recs, dataset, processing_options):
    try:
        table = table_rec['table']
        source = table_rec['source']
        release_date = table_rec['release_date']    
    except Exception as e:
        print(f"Missing key in table record: {e}")
        print (table_rec)
        return []
    
    data_recs = []

    table = helpers.unmerge_rowcol_span(table)

    for rec in recs:
        '''
            Search for metric using search_term
        '''        
        search_string = rec["search_term"]
        ticker = rec["ticker"]        
        categories = rec.get("categories", {})
        dimensions = list(categories.keys()) if categories else None        
        table_cell = None        
        pattern = re.compile(helpers.adj_text(search_string), re.IGNORECASE)

        #search for table row
        for td in table.find_all("td"):
            if td.text:
                if pattern.search(td.text):
                    table_cell = td
                    break
        if table_cell is None:
            print(f"Cannot find search term '{search_string}' in table")
            print (source)
            continue      
        try:
            table_row = table_cell.find_parent("tr")        
            value = None
            period_end = None
            if processing_options.get("value_col")=="last":
                value, offset_from_end = get_last_col_value(table_row)    
                period_end = get_period_end_last_col(table, offset_from_end)
            if processing_options.get("value_col")=="first":
                value = helpers.to_numeric(table_row.find_all("td")[1].text.strip().split(" ")[0])
                if processing_options.get("period_end_cell", None)=="keyword_search":
                    period_end = get_period_end_by_header_cell_text(table)                    
                else:
                    raise e
            
            baserec = DatabankRecord(
                dataset=dataset,
                ticker=ticker,
                country='IN',
                period_end=period_end,
                period_span=None,
                source=source,
                updated_on=release_date,
                dimensions=dimensions,
                categories=categories,
                inter_country_comparison=rec.get("inter_country_comparison", False)
            )

            if processing_options.get("yoy", False):
                #this is usually in the last column
                yoy_value = helpers.to_numeric(table_row.find_all("td")[-1].text.strip().split(" ")[0])        
                yoy = value/(value - yoy_value) - 1
                yoy_rec = baserec.clone()
                yoy_rec.update(
                    metric="YoY",
                    value=yoy,
                    unit="PERCENT"
                )
                yoy_rec.prep_for_insert()            
                data_recs.append(yoy_rec.rec)

            value = value * processing_options.get("value_multiplier", 1)  # Apply multiplier if specified

            value_rec = baserec.clone()
            value_rec.update(
                metric="Value",                
                value=value,                
                unit="INR"
            )

            value_rec.prep_for_insert()
            data_recs.append(value_rec.rec)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error processing ticker {ticker}: {e}")                
            print (table_rec.get("source"))
            continue        
    
    return data_recs


'''
    constituent_recs are in the format {ticker:xx, dimensions:xx} - we need to calculate the weight for each date
'''

async def updater(overwrite_history=False, start_from=None, config=None, dataset=None, recs=None, constituent_recs=None, processing_options={}):
    updated_dates = []

    if not start_from:
        start_from = config.get("start_date")
        start_from = datetime.strptime(start_from, "%Y-%m-%d")
    
    if not overwrite_history:
        updated_dates = await helpers.get_updated_dates(config["s3_prefix"], dataset)

    tables = await india_rbi.get(dataset, start_from, datetime.now(), updated_dates)
    
    all_df = []
    for table_rec in tqdm(tables, desc="Processing tables"):
        if not table_rec:
            continue
        _recs = await process_table(table_rec, recs, dataset, processing_options)
        if not recs:
            continue
        _df = pd.DataFrame(_recs)
        all_df.append(_df)
    
    if not all_df:
        print("No data found for the given date range.")
        return
    
    all_df = pd.concat(all_df, ignore_index=True)
    #convert period_end to date - just date - not datetime
    all_df['period_end'] = pd.to_datetime(all_df['period_end']).dt.date
    
    add_constituent_recs = []
    if (constituent_recs is not None):
        #group by period_end - and iterate over each date
        for period_end, group in all_df.groupby('period_end'):
            group_source = group['source'].iloc[0] if not group['source'].isnull().all() else None
            group_updated_on = group['updated_on'].iloc[0] if not group['updated_on'].isnull().all() else None
            group_period_span = group['period_span'].iloc[0] if not group['period_span'].isnull().all() else None
            
            
            for _constituent_rec in constituent_recs:
                rec = DatabankRecord(
                    dataset=dataset,
                    ticker=_constituent_rec['parent'].get('ticker', ''),
                    country='IN',
                    period_end=period_end,
                    period_span=group_period_span,
                    source=group_source,
                    updated_on=group_updated_on,
                    metric='Constituents',
                    unit='JSON'
                )
                parent_filter = (group['ticker'] == _constituent_rec['parent'].get('ticker', '')) & (group['metric'] == 'Value')
                parent_dims = _constituent_rec['parent'].get("dimensions", None)
                if parent_dims is None:
                    parent_filter = parent_filter & group['dimensions'].isna()
                else:
                    parent_filter = parent_filter & (group['dimensions'] == parent_dims)

                parent_rec = group[parent_filter]
                parent_value = parent_rec['value'].iloc[0] if not parent_rec['value'].isnull().all() else None

                for item in _constituent_rec.get('value_txt'):
                    item_filter = (group['ticker'] == item.get('ticker', '')) & (group['metric'] == 'Value')
                    item_dims = item.get("dimensions", None)
                    if item_dims is not None:
                        item_filter = item_filter & (group['dimensions'] == item_dims)
                    else:
                        item_filter = item_filter & group['dimensions'].isna()

                    item_rec = group[item_filter]
                    item_value = item_rec['value'].iloc[0] if not item_rec['value'].isnull().all() else None
                    if not item_rec.empty:
                        item_weight = item_value / parent_value if parent_value else None
                        if item_weight is not None:
                            rec.add_constituent(item.get('ticker', ''), item_weight, item.get('dimensions', None))
            
                rec.prep_for_insert()
                add_constituent_recs.append(rec.rec)
    
    if len(add_constituent_recs)>0:
        _df = pd.DataFrame(add_constituent_recs)        
        _df['period_end'] = pd.to_datetime(_df['period_end']).dt.date
        all_df = pd.concat([all_df, _df], ignore_index=True)

    await parquet_handler.save(all_df, f'{config["s3_prefix"]}/{country}')