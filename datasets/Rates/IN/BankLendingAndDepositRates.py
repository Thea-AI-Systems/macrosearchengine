from tools import helpers
import os
from processors import india_rbi

dataset = "BankLendingAndDepositRates"

recs = [               
        {"ticker":"SavingsDepositRate", "inter_country_comparison": True, "search_term":"Savings Deposit Rate"},
        {"ticker":"TermDepositRate", "inter_country_comparison": True, "search_term":"Term Deposit Rate"},
        {"ticker":"MCLR", "inter_country_comparison": False, "search_term":"MCLR"}
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