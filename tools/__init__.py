from curl_cffi import requests
from tenacity import retry, stop_after_attempt, wait_random_exponential

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
def session(url=None, s3client=None, use_proxy=False):
    session = requests.Session()
    if url is not None:
        if use_proxy:        
            session.proxies = get_proxies(s3client)
        session.get(url, timeout=30, impersonate='chrome', verify=False)
    return session

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
def post(params):
    url = params.get('url')
    session = params.get('session')
    s3client = params.get('s3client')
    compression = params.get('compression', False)
    use_proxy = params.get('use_proxy', False)
    addl_headers = params.get('addl_headers', {})
    data = params.get('data', None)
    timeout = params.get('timeout', 15)

    if session is None:
        session = requests.Session()
    
    if compression:
        session.headers.update({'Accept-Encoding': 'gzip, deflate, br, zstd'})    
    
    if use_proxy:        
        session.proxies = get_proxies(s3client)    
    else:
        session.proxies = {}    

    if addl_headers:
        session.headers.update(addl_headers)
    
    response = None
    if data is not None:                
        response = session.post(url, timeout=timeout, data=data, impersonate='chrome', verify=False)
    else:    
        response = session.post(url, timeout=timeout, impersonate='chrome', verify=False)   
    
    
    #compression is automatically handled by curl_cffi        
    if (100 <= response.status_code < 300) or response.status_code in [304, 307]:        
        return response
    
    print (f'Error downloading file {url}')
    print (response.status_code, (100 <= response.status_code < 300))        
    raise Exception(f'Error downloading file {url}')        

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
def get(params):

    url = params.get('url')
    session = params.get('session')
    s3client = params.get('s3client')
    compression = params.get('compression', False)
    use_proxy = params.get('use_proxy', False)
    addl_headers = params.get('addl_headers', {})
    json = params.get('data', None)
    timeout = params.get('timeout', 15)
    query_params = params.get('query_params', None)

    if session is None:
        session = requests.Session()
    
    if compression:
        session.headers.update({'Accept-Encoding': 'gzip, deflate, br, zstd'})    
    
    if use_proxy:        
        session.proxies = get_proxies(s3client)    
    else:
        session.proxies = {}    

    if addl_headers:
        session.headers.update(addl_headers)       

    try:

        request_kwargs = {
            'url':url,
            'timeout': timeout,
            'impersonate': 'chrome',
            'verify': False
        }

        if json is not None:
            request_kwargs['json'] = json
        if query_params:
            request_kwargs['params'] = query_params            

        response = session.get(**request_kwargs)    
        response.raise_for_status()
        return response
    except Exception as e:
        print (f'Error with {url}')        
        