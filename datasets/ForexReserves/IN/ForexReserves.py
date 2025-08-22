from processors import india_rbi
from tools import helpers
import os

dataset = 'ForexReserves'

'''
segments = {
        "ForexReserves": "Total Reserves",
        "ForeignCurrencyAssets": "Foreign Currency Assets",
        "GoldReserves": "Gold",
        "SpecialDrawingRights": "SDR",
        "ReservePortionInIMF": "Reserve Position in the IMF"
    }
'''

recs = [
        {"ticker":"ForexReserves", "inter_country_comparison": True, "search_term":"Total Reserves"},
        {"ticker":"ForexReserves", "categories": {"ForeignCurrencyAssets":"ForexReservesCategory"}, "search_term":"Foreign Currency Assets"},
        {"ticker":"ForexReserves", "categories": {"GoldReserves":"ForexReservesCategory"}, "search_term":"Gold"},
        {"ticker":"ForexReserves", "categories": {"SpecialDrawingRights":"ForexReservesCategory"}, "search_term":"Special Drawing Rights|SDR"},
        {"ticker":"ForexReserves", "categories": {"ReservePortionInIMF":"ForexReservesCategory"}, "search_term":"Reserve Position in the IMF"}
    ]

constituent_recs = [
    {
        "ticker":"ForexReserves",
        "value_txt":[
            {
                "ticker":"ForexReserves",
                "dimensions":"[ForeignCurrencyAssets]"
            },
            {
                "ticker":"ForexReserves",
                "dimensions":"[GoldReserves]"
            },
            {
                "ticker":"ForexReserves",
                "dimensions":"[SpecialDrawingRights]"
            },
            {
                "ticker":"ForexReserves",
                "dimensions":"[ReservePortionInIMF]"
            }
        ],
        "parent":{
            "ticker":"ForexReserves",
            "dimensions":None
        }
    }
]

async def update(overwrite_history=False, start_from=None):
    config = await helpers.load_config(os.path.dirname(__file__), dataset)
    await india_rbi.updater(
        overwrite_history=overwrite_history, 
        start_from=start_from, 
        config=config, 
        dataset=dataset,
        recs=recs,
        constituent_recs=constituent_recs,
        processing_options={
            "value_col": "first",
            "period_end_cell": "keyword_search",
            "value_multiplier": 10000000,  # Convert to millions
            "yoy": True
        }
    )