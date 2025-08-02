from curl_cffi import requests
import os
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def seed_session(url=None, verify=False):
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

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(multiplier=1, max=10))
async def invoke(params):
    url = params["url"]
    data = params.get("data")
    timeout = params.get("timeout", 30)
    headers = params.get("addl_headers", {})
    proxies = params.get("proxies", None)

    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        try:
            if params.get("invoke_type") == "get":
                response = await client.get(url)
            else:
                response = await client.post(url, data=data)            
            response.raise_for_status()
            return response
        except Exception as e:
            #print(f"Error with request {url}: {e}")
            raise

async def get(params): return await invoke({**params, "invoke_type": "get"})
async def post(params): return await invoke({**params, "invoke_type": "post"})

async def antibot_get(params):
    scrapfly_api_key = os.environ.get("SCRAPFLY_API_KEY")
    if not scrapfly_api_key:
        raise ValueError("SCRAPFLY_API_KEY environment variable is not set")
    url = "https://api.scrapfly.io/scrape"
    params['key'] = scrapfly_api_key
    params['url'] = params.get('url', None)    
    params['asp'] = True
    params['render_js'] = True
    params['tags'] = "player,project:default"

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response