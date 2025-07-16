from curl_cffi import requests
from tenacity import retry, stop_after_attempt, wait_random_exponential

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
def seed_session(url=None, verify=False):
    session = requests.Session()
    if url is not None:        
        session.get(url, timeout=30, impersonate='chrome', verify=verify)
    return session

def update_session(params):
    session = params.get('session', None)    
    compression = params.get('compression', False)
    proxies = params.get('proxies', False)
    addl_headers = params.get('addl_headers', {})    

    if session is None:
        session = requests.Session()
    
    if compression:
        session.headers.update({'Accept-Encoding': 'gzip, deflate, br, zstd'})    
    
    if proxies:        
        session.proxies = proxies
    else:
        session.proxies = {}    

    if addl_headers:
        session.headers.update(addl_headers)

    return session

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
def invoke(params):
    session = update_session(params)
    
    url = params.get('url')
    data = params.get('data', None)
    timeout = params.get('timeout', 30)

    requests_kwargs = {
        'url': url,
        'timeout': timeout,
        'impersonate': 'chrome',
        'verify': False
    }   

    if data is not None:
        requests_kwargs['data'] = data    
    
    try:
        if params.get("invoke_type") == "get":
            response = session.get(**requests_kwargs)
        else:
            response = session.post(**requests_kwargs)        
        response.raise_for_status()
        return response
    except Exception as e:
        print(f'Error with request {url}')
        print (e)        
        raise e
    
def post(params):
    #add params with invoke_type = 'post'
    adjusted_params = params.copy()
    adjusted_params['invoke_type'] = 'post'
    return invoke(adjusted_params)

def get(params):
    #add params with invoke_type = 'get'
    adjusted_params = params.copy()
    adjusted_params['invoke_type'] = 'get'
    return invoke(adjusted_params)
