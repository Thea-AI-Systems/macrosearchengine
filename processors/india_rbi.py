import pandas as pd
from apis import india_rbi
from datetime import datetime
from tqdm import tqdm
from dateutil import parser
from tools import helpers, parquet_handler
from tools.records import DatabankRecord


country = "IN"


def get_period_end_by_header_cell_text(table):
    period_end_search_terms = ["Outstanding as on", "Outstandingas on "]    
    
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
        print (rec)
        search_string = rec["search_term"]
        ticker = rec["ticker"]        
        categories = rec.get("categories", {})
        dimensions = list(categories.keys()) if categories else None        
        table_cell = None        

        #search for table row
        for td in table.find_all("td"):
            if td.text:                
                if helpers.adj_text(search_string).lower() in helpers.adj_text(td.text).lower():
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
                    #if period end is gretaer than today - wake me up
                    if period_end > datetime.now():
                        print(f"Period end {period_end} is greater than today. Please check the source.")
                        print (source)
                        input("Press Enter to continue...")                    
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
            print(f"Error processing ticker {ticker}: {e}")                
            print (table_rec.get("source"))
            continue        
    
    return data_recs


async def updater(overwrite_history=False, start_from=None, config=None, dataset=None, recs=None, processing_options={}):
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
        #print (_df)
        #input("Press Enter to continue..."  )
        all_df.append(_df)
    
    if not all_df:
        print("No data found for the given date range.")
        return
    
    all_df = pd.concat(all_df, ignore_index=True)
    
    await parquet_handler.save(all_df, f'{config["s3_prefix"]}/{country}')