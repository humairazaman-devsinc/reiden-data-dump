import requests
from requests.adapters import HTTPAdapter, Retry
from config import Config

def get_http_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=Config.RETRIES,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(Config.get_api_headers())
    return session
