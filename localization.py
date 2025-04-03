import requests
import json


def search(module):
    url = f"http://localhost:8082/localization/messages/v1/_search?module={module}&locale=en_IN&tenantId=pg"

    payload = "{\"RequestInfo\":{\"apiId\":\"Rainmaker\",\"authToken\":\"0950e4e6-c08d-4f37-9e36-902b1f32d558\",\"msgId\":\"1739426312010|en_IN\",\"plainAccessRequest\":{}}}"
    headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'http://localhost:3000',
            'Pragma': 'no-cache',
            'Referer': 'http://localhost:3000/digit-ui/employee/im/incident/create',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not(A:Brand";v="99", "Brave";v="133", "Chromium";v="133"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"'
            }

    response = requests.request("POST", url, headers=headers, data=payload)

    messages = response.json()["messages"]
    return messages


def upsert_localization(tenant_id, module, messages):
    url = "http://localhost:8082/localization/messages/v1/_upsert"
    payload = json.dumps({
        "RequestInfo": {
            "apiId": "Rainmaker",
            "ver": ".01",
            "ts": "",
            "action": "_create",
            "did": "1",
            "key": "",
            "msgId": "20170310130900|en_IN",
            "authToken": "0950e4e6-c08d-4f37-9e36-902b1f32d558",
            "userInfo": {
                "id": 100,
                "uuid": "80ab5b3b-87d0-44b1-b050-8b44a53e9456",
                "userName": "admin",
                "name": "admin",
                "mobileNumber": "1002335567",
                "emailId": None,
                "locale": None,
                "type": "EMPLOYEE",
                "roles": [
                    {
                        "name": "EMPLOYEE ADMIN",
                        "code": "EMPLOYEE_ADMIN",
                        "tenantId": "nl"
                        },
                    {
                        "name": "MDMS ADMIN",
                        "code": "MDMS_ADMIN",
                        "tenantId": "nl"
                        },
                    {
                        "name": "Super User",
                        "code": "SUPERUSER",
                        "tenantId": "nl"
                        },
                    {
                        "name": "Localisation admin",
                        "code": "LOC_ADMIN",
                        "tenantId": "nl"
                        },
                    {
                        "name": "Asset Administrator",
                        "code": "ASSET_ADMINISTRATOR",
                        "tenantId": "nl"
                        },
                    {
                        "name": "HRMS Admin",
                        "code": "HRMS_ADMIN",
                        "tenantId": "nl"
                        },
                    {
                        "name": "National Dashboard Admin",
                        "code": "NATADMIN",
                        "tenantId": "nl"
                        }
                    ],
                "active": True,
                "tenantId": "nl",
                "permanentCity": None
                }
      },
      "tenantId": tenant_id,
      "module": module,
      "locale": "en_IN",
      "messages": messages
    })
    headers = {
            'Content-Type': 'application/json'
            }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.json())





modules = [
        "rainmaker-hrms",
        # "rainmaker-pg", # needs to be done separately as module name itself changes
        "rainmaker-im",
        "rainmaker-common",
        "rainmaker-hr",
        ]

for module in modules:
    messages = search(module)
    upsert_localization("nl", module, messages)
