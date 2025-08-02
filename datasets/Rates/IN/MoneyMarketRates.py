from tools import helpers
import os
from processors import india_rbi

dataset = "MoneyMarketRates"

recs = [       
        {"ticker":"TBill_91D", "inter_country_comparison": True, "search_term":"91-Day Treasury Bill "},
        {"ticker":"TBill_182D", "inter_country_comparison": True, "search_term":"182-Day Treasury Bill "},
        {"ticker":"TBill_364D", "inter_country_comparison": True, "search_term":"364-Day Treasury Bill "},
        {"ticker":"CallMoneyRate", "inter_country_comparison": False, "search_term":"Call Money Rate"}
    ]

async def update(overwrite_history=False, start_from=None):
    config = await helpers.load_config(os.path.dirname(__file__), dataset)

    await india_rbi.updater(
        overwrite_history=overwrite_history, 
        start_from=start_from, 
        config=config, 
        dataset=dataset,
        recs=recs,
        processing_options={
            "value_col": "last",
            "value_multiplier": 1
        }
    )