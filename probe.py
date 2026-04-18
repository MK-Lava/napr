# probe.py
import requests
import json

url = "https://naprweb.reestri.gov.ge/api/search"
payload = {
    "page": 1,
    "search": "",
    "regno": "",
    "datefrom": None,
    "dateto": None,
    "person": "",
    "address": "წინამძღვრიანთკარი",
    "cadcode": "",
}
headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://naprweb.reestri.gov.ge",
    "Referer": "https://naprweb.reestri.gov.ge/_dea/",
    "User-Agent": "Mozilla/5.0",
}

r = requests.post(url, json=payload, headers=headers)
print("Status:", r.status_code)
print("---")
# Print first ~3000 chars of pretty-printed JSON so we can see the structure
print(json.dumps(r.json(), ensure_ascii=False, indent=2)[:3000])