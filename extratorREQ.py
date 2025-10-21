import os
import json
import time
import logging
from typing import Dict, List, Optional, Set
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")
ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

def erp_request(method: str, path: str, params: Dict = None, payload: Dict = None, timeout=60) -> requests.Response:
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    if not path.startswith("api/resource/"): url = f"{ERPNext_URL.rstrip('/')}/api/resource/{path.lstrip('/')}"
    try: return requests.request(method, url, params=params, json=payload, timeout=timeout)
    except requests.exceptions.RequestException as e:
        r = requests.Response(); r.status_code = 503; return r

payload_lancamento = {"doctype": "Stock Entry", "stock_entry_type": "Material Receipt", "posting_date": "2025-01-01" , "docstatus": 1, "company": "CDC", "custom_id_ongsys": "teste", "items": []}

payload_lancamento["items"].append({"item_code": "78", "qty": "4", "t_warehouse": "teste", "cost_center": "teste"})
         
response = erp_request("POST", "api/resource/Stock Entry", payload=payload_lancamento)

print(response.status_code)


