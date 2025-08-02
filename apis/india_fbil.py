from tenacity import retry, stop_after_attempt, wait_fixed
from datetime import datetime

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get_daily(dataset, period_from, period_to=None):