from processors import india_rbi
from tools import helpers
import os

dataset = 'BankRatios'

recs = [
        {"ticker":"CashReserveRatio", "inter_country_comparison": False, "search_term":"Cash Reserve Ratio"},
        {"ticker":"StatutoryLiquidityRatio", "inter_country_comparison": False, "search_term":"Statutory Liquidity Ratio"},
        {"ticker":"CashDepositRatio", "inter_country_comparison": False, "search_term":"Cash-Deposit Ratio"},
        {"ticker":"CreditDepositRatio", "inter_country_comparison": False, "search_term":"Credit-Deposit Ratio"},
        {"ticker":"IncrementalCreditDepositRatio", "inter_country_comparison": False, "search_term":"Incremental Credit-Deposit Ratio"},
        {"ticker":"InvestmentDepositRatio", "inter_country_comparison": False, "search_term":"Investment-Deposit Ratio"},        
        {"ticker":"IncrementalInvestmentDepositRatio", "inter_country_comparison": False, "search_term":"Incremental Investment-Deposit Ratio"}        
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