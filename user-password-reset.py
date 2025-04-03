import json
import requests
import re
import sys
import copy


with open("vendor-tenants-map.json", "r") as f:
    vendor_data = json.load(f)


def update_user_password(tenant_id, mobile_number, username):
    url = "http://localhost:8081/user/v1/_search"

    payload = json.dumps(
        {"tenantId": tenant_id, "mobileNumber": mobile_number, "username": username}
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)

    users = response.json()["user"]

    if len(users) == 0:
        print(tenant_id)
        raise ValueError("User not created")

    user = users[0]
    print(user)
    sys.exit()
    user["password"] = "Health@2026"
    user["dob"] = re.sub("-", "/", user["dob"])
    # new_roles = []
    # for vendor_tenant_id in vendor_data[user["userName"]]:
    #     for role in user["roles"]:
    #         role_copy = copy.copy(role)
    #         if vendor_tenant_id == "nl" and role_copy["code"] == "COMPLAINT_RESOLVER":
    #             continue
    #         if (
    #             role_copy["code"] != "COMPLAINT_RESOLVER"
    #             and role_copy["code"] != "EMPLOYEE"
    #         ) or (role_copy["tenantId"] is not None and role_copy["tenantId"] == "nl"):
    #             print(role)
    #             sys.exit()
    #         role_copy["tenantId"] = vendor_tenant_id
    #         new_roles.append(role_copy)

    # user["roles"] = new_roles
    # print(new_roles)
    # # print(len(new_roles))
    # return

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
                            "tenantId": "pg",
                        },
                        {"name": "MDMS ADMIN", "code": "MDMS_ADMIN", "tenantId": "pg"},
                        {"name": "Super User", "code": "SUPERUSER", "tenantId": "pg"},
                        {
                            "name": "Localisation admin",
                            "code": "LOC_ADMIN",
                            "tenantId": "pg",
                        },
                        {
                            "name": "Asset Administrator",
                            "code": "ASSET_ADMINISTRATOR",
                            "tenantId": "pg",
                        },
                        {"name": "HRMS Admin", "code": "HRMS_ADMIN", "tenantId": "pg"},
                        {
                            "name": "National Dashboard Admin",
                            "code": "NATADMIN",
                            "tenantId": "pg",
                        },
                    ],
                    "active": True,
                    "tenantId": "pg",
                    "permanentCity": None,
                },
            },
        }
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)
    print(f"{tenant_id} | {response.json()["user"][0]["userName"]} | Health@2026")


# update_user_password("nl", "1111111111", "a")
# update_user_password("nl", "1111111112", "a")
# update_user_password("nl", "1111111113", "a")
# update_user_password("nl", "1111111114", "a")
update_user_password("pg.dummy", None, "selcoindia")


# with open("output/pg/state-data/karnataka-user-data-phase-3-1.csv/tenants.json", "r") as f:
#     data = json.load(f)

# for tenant in data["tenants"]:
#     mobile_number = tenant["contactNumber"]
#     tenant_id = tenant["code"]
#     username = re.sub(r"^PG-", "", tenant["city"]["code"])
#     if mobile_number is not None and tenant_id is not None:
#         update_user_password(tenant_id, mobile_number, None)
