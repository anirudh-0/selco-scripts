import json
import requests
import re

with open("tenants.json", "r") as f:
    data = json.load(f)


def update_user_password(tenant_id, mobile_number, username):
    url = "http://localhost:8081/user/_search"

    payload = json.dumps({"tenantId": tenant_id, "mobileNumber": mobile_number})
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)

    users = response.json()["user"]
    if len(users) == 0:
        raise ValueError("User not created")

    user = users[0]
    user["password"] = "Health@2026"
    user["dob"] = re.sub("-", "/", user["dob"])
    user["userName"] = username

    url = "http://localhost:8081/user/users/_updatenovalidate"

    payload = json.dumps(
        {
            "user": user,
            "RequestInfo": {
                "apiId": "Rainmaker",
                "msgId": "1739459158584|en_IN",
                "authToken": "a9f2b95c-8afe-457f-a0bd-e442112d7957",
                "plainAccessRequest": {},
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
                            "tenantId": "nl",
                        },
                        {"name": "MDMS ADMIN", "code": "MDMS_ADMIN", "tenantId": "nl"},
                        {"name": "Super User", "code": "SUPERUSER", "tenantId": "nl"},
                        {
                            "name": "Localisation admin",
                            "code": "LOC_ADMIN",
                            "tenantId": "nl",
                        },
                        {
                            "name": "Asset Administrator",
                            "code": "ASSET_ADMINISTRATOR",
                            "tenantId": "nl",
                        },
                        {"name": "HRMS Admin", "code": "HRMS_ADMIN", "tenantId": "nl"},
                        {
                            "name": "National Dashboard Admin",
                            "code": "NATADMIN",
                            "tenantId": "nl",
                        },
                    ],
                    "active": True,
                    "tenantId": "nl",
                    "permanentCity": None,
                },
            },
        }
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)
    print(f"{tenant_id} | {response.json()["user"][0]["userName"]} | Health@2026")


for tenant in data["tenants"]:
    mobile_number = tenant["contactNumber"]
    tenant_id = tenant["code"]
    username = re.sub(r"^NL-", "", tenant["city"]["code"])
    if mobile_number is not None and tenant_id is not None:
        update_user_password(tenant_id, mobile_number, username)
