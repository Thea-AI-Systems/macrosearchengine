from tools.parquet_handler import get_presigned_url
from bs4 import BeautifulSoup
from datetime import datetime
import duckdb
import os
import duckdb
import json
import re
import calendar

def get_last_day_of_month(year, month):    
    return datetime(year, month, calendar.monthrange(year, month)[1])
    

def build_query(dataset = None, ticker=None, metric=None, country=None, dimensions=None, dimensions_list=None):
    query = []
    if dataset:
        query.append(f"dataset = '{dataset}'")
    if ticker:
        query.append(f"ticker = '{ticker}'")        
    if metric:
        query.append(f"metric = '{metric}'")
    if country:
        query.append(f"country = '{country}'")
    
    query = " AND ".join(query)    
    return query

def adj_text(text):
    #replace more than one space with a single space
    #replace [–, —, .] with ' '
    text = text.replace("–", " ")
    text = text.replace("-", " ")
    text = text.replace("—", " ")
    text = text.replace(".", " ")
    text = text.replace("*", " ")
    text = " ".join(text.split())

    return text

def to_numeric(value):
    #check it has atleast one digit
    if not re.search(r'\d', value):
        return None
    #remove commas and convert to float    
    value = re.sub(r'[^\d.-]', '', value)  # Remove non-numeric characters
    #if there are more than one . return None
    if value.count('.') > 1:
        return None
    if value == '':
        return None
    
    try:
        value = float(value)
    except Exception as e:
        print (f"Error: {e}")
        print (value)
        return None
    return value


def unmerge_rowcol_span(soup_table):
    grid = []
    rowspan_map = {}

    for row_idx, row in enumerate(soup_table.find_all('tr')):
        grid_row = []
        col_idx = 0

        cells = row.find_all(['td', 'th'])
        cell_idx = 0

        while cell_idx < len(cells) or col_idx in rowspan_map:
            if col_idx in rowspan_map:
                # Insert cell from previous rowspan
                grid_row.append(rowspan_map[col_idx]['text'])
                rowspan_map[col_idx]['rows_left'] -= 1
                if rowspan_map[col_idx]['rows_left'] == 0:
                    del rowspan_map[col_idx]
                col_idx += 1
            else:
                cell = cells[cell_idx]
                cell_text = cell.get_text(strip=True)

                colspan = int(cell.get('colspan', 1)) if cell.get('colspan') else 1
                rowspan = int(cell.get('rowspan', 1)) if cell.get('rowspan') else 1

                for i in range(colspan):
                    grid_row.append(cell_text)
                    if rowspan > 1:
                        rowspan_map[col_idx] = {'text': cell_text, 'rows_left': rowspan - 1}
                    col_idx += 1
                cell_idx += 1
        
        grid.append(grid_row)

    # Now, rebuild the soup table with no rowspan/colspan
    html = '<table>\n'
    for row in grid:
        html += '  <tr>\n'
        for cell in row:
            html += f'    <td>{cell}</td>\n'
        html += '  </tr>\n'
    html += '</table>'
    
    new_soup = BeautifulSoup(html, 'html.parser')
    return new_soup.find('table')
    
async def get_latest_date(parquet_loc, dataset, ticker=None, country=None):
    presigned_url = await get_presigned_url(parquet_loc)
    if not presigned_url:        
        return None
    presigned_url = presigned_url[0]  # Get the first URL if there are multiple 
    
    query = build_query(dataset, ticker=ticker, country=country)    

    query = f"SELECT MAX(period_end) FROM read_parquet('{presigned_url}') WHERE {query}"        
    
    dt = duckdb.query(query).fetchone()
    if dt and dt[0]:
        return dt[0]
    return None

async def get_updated_dates(parquet_loc, dataset, ticker=None, country=None):
    presigned_url = await get_presigned_url(parquet_loc)    
    if not presigned_url:        
        return []
    presigned_url = presigned_url[0]  # Get the first URL if there are multiple

    query = build_query(dataset, ticker=ticker, country=country)

    query = f"SELECT DISTINCT updated_on FROM read_parquet('{presigned_url}') WHERE {query}"        

    updated_dates = duckdb.query(query).fetchall()
    
    if updated_dates:
        updated_dates = [dt[0] for dt in updated_dates if dt[0]]    
        return updated_dates
    return []   



async def load_config(dirpath, dataset):
    #2 levels higher
    config_path = os.path.abspath(os.path.join(dirpath, 'config.json'))

    with open(config_path, 'r') as f:
        config = json.load(f)
        config = config.get("datasets", {}).get(dataset, {})

    return config
