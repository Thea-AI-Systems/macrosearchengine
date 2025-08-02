from processors import india_rbi
from tools import helpers
import os

dataset = 'BankCreditAndDeposits'

recs = [
        {"ticker":"BankDeposits", "inter_country_comparison": True, "search_term":"Aggregate Deposits"},
        {"ticker":"BankDeposits", "categories": {"DemandDeposits":"SubcategoryOfBankDeposits"}, "search_term":"2.1.1 Demand"},
        {"ticker":"BankDeposits", "categories": {"TimeDeposits":"SubcategoryOfBankDeposits"}, "search_term": "2.1.2 Time"},
        {"ticker":"BankCredit", "inter_country_comparison": True, "search_term":"Bank Credit"},
        {"ticker":"BankCredit", "categories": {"FoodCredit":"SubcategoryOfBankCredit"}, "search_term":"Food Credit"},
        {"ticker":"BankCredit", "categories": {"NonFoodCredit":"SubcategoryOfBankCredit"}, "search_term":"Non-Food Credit"},
        {"ticker":"BankCredit", "categories": {"LoansCashCreditAndOverdraft":"SubcategoryOfBankCredit"}, "search_term":"Loans, Cash credit and Overdraft"},
        {"ticker":"BankCredit", "categories": {"InlandBillsPurchased":"SubcategoryOfBankCredit"}, "search_term":"Inland Bills Purchased"},
        {"ticker":"BankCredit", "categories": {"InlandBillsDiscounted":"SubcategoryOfBankCredit"}, "search_term":"7b.3 Discounted"},
        {"ticker":"BankCredit", "categories": {"ForeignBillsPurchased":"SubcategoryOfBankCredit"}, "search_term":"Foreign Bills Purchased"},
        {"ticker":"BankCredit", "categories": {"ForeignBillsDiscounted":"SubcategoryOfBankCredit"}, "search_term":"7b.5 Discounted"}
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
            "value_col": "first",
            "period_end_cell": "keyword_search",
            "value_multiplier": 10000000,  # Convert to millions
            "yoy": True
        }
    )