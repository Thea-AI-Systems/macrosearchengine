from apis import india_mospi
from datetime import datetime
import pandas as pd
from tools.records import DatabankRecord, label_to_topic
from tools.helpers import get_last_day_of_month
from tools import helpers, parquet_handler
import os
#use this file to update the weights - this is a one off and does not need to be run every time
weights_from_file = "datasets/IIP/IN/IndicesIIP2011-12Monthly_annual_Jun25.xlsx"

dataset = "IIP"
country = "IN"


def process_records(records):
    baserec = DatabankRecord(
                dataset=dataset,
                ticker='IIP',
                country='IN',                
                period_span=None,
                source='https://www.esankhyiki.mospi.gov.in/macroindicators-main/macroindicators?product=iip',                
            )
    

    recs = []
    for rec in records:
        _rec = baserec.clone()
        yoy = rec['growth_rate']
        unit = 'PERCENT'
        _rec.update(
            metric="YoY",
            value=yoy,
            unit=unit,
            period_end=get_last_day_of_month(rec['year'], rec['month']),
            period_span='M',
            updated_on=datetime.now()
        )

        if rec.get('sub_category', '') != '' and rec.get('category', '') == 'Manufacturing':
            _val = label_to_topic(rec['sub_category'])
            _rec.add_dimension(_val, 'SubcategoryOfManufacturing')
        if rec.get('category', 'General') != 'General':
            _val = label_to_topic(rec['category'])            
            if rec.get('type')=='Sectoral':
                _rec.add_dimension(_val, 'SectoralClassification')
            elif rec.get('type')=='Use-based category':                
                _rec.add_dimension(_val, 'UseBasedClassification')        
        
        _rec.prep_for_insert()
        recs.append(_rec.rec)

    return recs

async def update_constituent_weights():
    base_year = '2011-12'
    sectoral_weights_df = pd.read_excel(weights_from_file, sheet_name='NIC 2d, sectoral monthly', skiprows=5)    
    ubc_weights_df = pd.read_excel(weights_from_file, sheet_name='UBC monthly', skiprows=5)

    baserec = DatabankRecord(
        dataset=dataset,
        ticker='IIP',
        country='IN',        
        period_end=datetime(2012, 3, 31),
        period_span=None,
        unit='JSON',
        source='https://www.mospi.gov.in/iip',
        updated_on=datetime.now()
    )
    baserec.add_dimension(base_year, 'BaseYear')        
    recs = []
    #convert sectoral weights to records
    manufacturing_constituents = baserec.clone()
    manufacturing_constituents.update(ticker='Manufacturing')
    #iterate sectoral_weights sheet till you hit manufacturing in the Description column
    for index, row in sectoral_weights_df.iterrows():        
        if pd.isna(row['Description']):
            break        
        manufacturing_constituents.add_constituent(
            label_to_topic(row['Description']),
            float(row['Weights'])
        )
    manufacturing_constituents.prep_for_insert()
    recs.append(manufacturing_constituents.rec)
    
    sectoral_constituents = baserec.clone()
    sectoral_constituents.add_dimension('SectoralClassification', 'ClassificationType')
    for index, row in sectoral_weights_df.iterrows():
        #first item in row
        if pd.isna(row['Description']):            
            _description = list(row.items())[0][1]
            if _description in ['Manufacturing', 'Mining', 'Electricity']:
                sectoral_constituents.add_constituent(
                    label_to_topic(_description),
                    float(row['Weights'])
                )                
            if _description == 'General':
                break
    sectoral_constituents.prep_for_insert()
    recs.append(sectoral_constituents.rec)
            
    
    ubc_constituents = baserec.clone()
    ubc_constituents.add_dimension('UseBasedClassification', 'ClassificationType')
    #iterate till empty row
    for index, row in ubc_weights_df.iterrows():
        if pd.isna(row['Use-based category']):
            break
        ubc_constituents.add_constituent(
            label_to_topic(row['Use-based category']),
            float(row['Weight'])
        )
    ubc_constituents.prep_for_insert()
    recs.append(ubc_constituents.rec)

    constituents_df = pd.DataFrame(recs)
    if constituents_df.empty:
        print(f"No valid records found for {dataset} constituents.")
        return None
    
    return constituents_df
    
async def update(overwrite_history=False, start_from=None, update_weights=True):    
    config = await helpers.load_config(os.path.dirname(__file__), dataset)
    
    if update_weights:
        constituents_df = await update_constituent_weights()
        if constituents_df is not None:
            await parquet_handler.save(constituents_df, f'{config["s3_prefix"]}/{country}')

    updated_dates = []

    if not start_from:
        start_from = config.get("start_date")
        start_from = datetime.strptime(start_from, "%Y-%m-%d")
    
    if not overwrite_history:
        updated_dates = await helpers.get_updated_dates(config["s3_prefix"], dataset)

    recs = await india_mospi.get(dataset, start_from, datetime.now(), updated_dates)
    if not recs:
        print(f"No records found for {dataset} from {start_from} to now.")
        return
    recs = process_records(recs)
    _df = pd.DataFrame(recs)
    if _df.empty:
        print(f"No valid records found for {dataset} from {start_from} to now.")
        return
    
    await parquet_handler.save(_df, f'{config["s3_prefix"]}/{country}')