from curl_cffi import requests
import os
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential

'''
@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def seed_session(url=None, verify=False):    
    session = requests.Session()
    if url is not None:        
        session.get(url, timeout=30, impersonate='chrome', verify=verify)
    return session
''' 


def get_transport(proxies):
    if not proxies:
        return None
    return httpx.AsyncHTTPTransport(proxies=proxies)

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
async def seed_session(url=None, verify=False, proxies=None, addl_headers=None):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),
    }
    if addl_headers:
        headers.update(addl_headers)

    transport = get_transport(proxies)

    client = httpx.AsyncClient(verify=verify, headers=headers, transport=transport)

    if url is not None:
        await client.get(url, timeout=30)
    return client

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
    session = params.get("session", None)
    url = params["url"]
    data = params.get("data")
    timeout = params.get("timeout", 30)    
    headers = {
        # Pretend to be Chrome on Windows
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),        
        **params.get("addl_headers", {}),
    }
    proxies = params.get("proxies", None)

    
    if session is None:        
        session = requests.AsyncSession(
                        timeout=timeout,
                        headers=headers,
                        verify=False,       # disables TLS verification
                        proxies=proxies     # e.g. {"http": "http://...", "https": "http://..."}
                    )
        close_session = True  # we should close after use
    else:
        # If session is passed, we trust it has needed headers/proxies/etc.
        close_session = False

    try:
        if params.get("invoke_type") == "get":
            response = await session.get(url, timeout=timeout)
        else:
            response = await session.post(url, data=data, timeout=timeout)
        response.raise_for_status()
        return response
    finally:
        if close_session:
            await session.close()
    

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