import requests
def _one_request(params=None):
    url = "https://data.stats.gov.cn/english/easyquery.htm"
    import requests
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

baseparams = {
        'm': 'QueryData',
        'dbcode': 'hgyd',
        'rowcode': 'zb',
        'colcode': 'sj',
        'wds': '[]'
        #'dfwds': '[{"wdcode":"sj","valuecode":"201001"}]',        
    }

async def food_and_beverages_inflation():
    # This function is a placeholder for food inflation calculation
    # It should be implemented based on the specific requirements
    params = baseparams.copy()
    params['dfwds'] = '[{"wdcode":"sj","valuecode":"202101,202102"},{"wdcode":"zb","valuecode":"A010103"}]'
    res = _one_request(params=params)
    print(res)
    pass

async def update():
    #test    

    print(await food_and_beverages_inflation())











