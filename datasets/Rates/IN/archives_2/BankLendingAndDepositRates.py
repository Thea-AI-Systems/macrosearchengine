from apis import india_rbi
from datetime import datetime
from tools import helpers, parquet_handler
from tools.records import DatabankRecord
from dateutil import parser

import pandas as pd
from tqdm import tqdm
import os

dataset = "BankLendingAndDepositRates"
country = "IN"

recs = [               
        {"ticker":"SavingsDepositRate", "inter_country_comparison": True, "search_term":"Savings Deposit Rate"},
        {"ticker":"TermDepositRate", "inter_country_comparison": True, "search_term":"Term Deposit Rate"},
        {"ticker":"MCLR", "inter_country_comparison": False, "search_term":"MCLR"}
    ]

async def process_table(table_rec): 
    try:
        table = table_rec['table']
        source = table_rec['source']
        release_date = table_rec['release_date']    
    except Exception as e:
        print(f"Missing key in table record: {e}")
        print (table_rec)
        return []
    
    data_recs = []

    yyyy_row = 1
    mmm_dd_row = 2  

    table = helpers.unmerge_rowcol_span(table)

    for rec in recs:
        search_string = rec["search_term"]
        ticker = rec["ticker"]        
        categories = {}
        dimensions = None
        table_cell = None        
        for td in table.find_all("td"):
            if td.text:
                #replace more than one space with a single space                                
                if helpers.adj_text(search_string).lower() in helpers.adj_text(td.text).lower():
                    table_cell = td
                    break
        if table_cell is None:
            print(f"Cannot find search term '{search_string}' in table")
            print (source)
            continue            
        try:
            table_row = table_cell.find_parent("tr")
            #find the latest value  from end
            all_cells = table_row.find_all("td")

            value = None
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

                if value is None:
                    continue
                else:
                    val_col = i
                    break

            if value is None:
                print (f"Skipping {ticker} in {rec['link']} as value not found")
                continue

            offset_from_end = val_col - len(all_cells)

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
                    print (f"Error: {e}")
                    print (dt_month)
                    print (rec['dt'], rec['link'])            
                    raise e
            
            val_dt = parser.parse(f"{dt_year} {month} {day}")
            
            
            baserec = DatabankRecord(
                dataset=dataset,
                ticker=ticker,
                country='IN',
                period_end=val_dt,
                period_span=None,
                source=source,
                updated_on=release_date,
                dimensions=dimensions,
                categories=categories,
                inter_country_comparison=rec.get("inter_country_comparison", False)
            )

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

async def update(overwrite_history=False, start_from=None):
    config = await helpers.load_config(os.path.dirname(__file__), dataset)

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
        recs = await process_table(table_rec)   #['table'], table['table_date'], table['source'], table['release_date'])
        if not recs:
            continue
        _df = pd.DataFrame(recs)        
        all_df.append(_df)
    
    if not all_df:
        print("No data found for the given date range.")
        return
    
    all_df = pd.concat(all_df, ignore_index=True)
    
    await parquet_handler.save(all_df, f'{config["s3_prefix"]}/{country}')