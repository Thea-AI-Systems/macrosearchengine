from tools import helpers
import os
from processors import india_rbi

dataset = "PolicyRates"

recs = [       
        {"ticker":"RepoRate", "inter_country_comparison": False, "search_term":"Policy Repo Rate"},
        {"ticker":"ReverseRepoRate", "inter_country_comparison": False, "search_term":"Reverse Repo Rate"},
        {"ticker":"SDFRate", "inter_country_comparison": False, "search_term":"Standing Deposit Facility (SDF) Rate*"},
        {"ticker":"MSFRate", "inter_country_comparison": False, "search_term":"Marginal Standing Facility (MSF) Rate"},
        {"ticker":"BankRate", "inter_country_comparison": False, "search_term":"Bank Rate"}
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