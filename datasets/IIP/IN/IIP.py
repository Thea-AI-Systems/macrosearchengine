from apis import india_mospi
from datetime import datetime
import pandas as pd
from tools import helpers, parquet_handler
import os
from tools.records import DatabankRecord, label_to_topic, get_last_day_of_month


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
            period_span='M'
        )        
        if rec.get('sub_category', '') == '':
            _val = label_to_topic(rec['category'])
            _rec.add_dimension(_val, 'CategoryOfManufacturing')
        if rec.get('category', 'General') != 'General':
            _val = label_to_topic(rec['category'])
            if rec.get('type')=='Sectoral':
                _rec.add_dimension(_val, 'SectoralClassification')
            elif rec.get('type')=='Use-based category':
                _rec.add_dimension(_val, 'UseBasedClassification')

        rec.prep_for_insert()
        recs.append(_rec.rec)

    return recs


async def update(overwrite_history=False, start_from=None):
    config = await helpers.load_config(os.path.dirname(__file__), dataset)
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
    
    
    
        
        






    
