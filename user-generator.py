import sys
import pandas as pd
import chardet
import json
import re
import math
import requests

if len(sys.argv) < 2:
    print("Usage python tenant-generator.py <users-data-file-path>")
    sys.exit(1)

users_file = sys.argv[1]


df = pd.read_excel(
    users_file,
    usecols=[
        "Employees__tenantId",
        "Employees__user__name",
        "Employees__user__mobileNumber",
        "Employees__user__fatherOrHusbandName",
        "Employees__code",
    ],
)

df = df.rename(
    columns={
        "Employees__tenantId": "tenant_id",
        "Employees__user__name": "name",
        "Employees__user__mobileNumber": "mobileNumber",
        "Employees__user__fatherOrHusbandName": "fatherName",
        "Employees__code": "username",
    }
)

df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

print(df)


def save_user(user):
    print(user)
    url = f"http://localhost:8089/egov-hrms/employees/_create?tenantId={user["tenant_id"]}"

    payload = json.dumps(
        {
            "RequestInfo": {
                "apiId": "Rainmaker",
                "ver": ".01",
                "action": "",
                "did": "1.0",
                "key": "",
                "msgId": "20170310130902|en_IN",
                "requesterId": None,
                "authToken": "2500b335-8865-4e7a-953c-098ef189a61c  #Modify this",
                "userInfo": {
                    "name": None,
                    "mobileNumber": None,
                    "fatherOrHusbandName": None,
                    "relationship": None,
                    "gender": None,
                    "dob": None,
                    "roles": None,
                    "uuid": "2500b335-8865-4e7a-953c-098ef189a61c",
                    "tenantId": None,
                },
            },
            "Employees": [
                {
                    "tenantId": user["tenant_id"],
                    "employeeStatus": "EMPLOYED",
                    "dateOfAppointment": 1617215400000,
                    "employeeType": "PERMANENT",
                    "user": {
                        "name": user["name"],
                        "mobileNumber": user["mobileNumber"],
                        "fatherOrHusbandName": user["fatherName"],
                        "relationship": "FATHER",
                        "gender": "MALE",
                        "dob": 760645800001,
                        "roles": [
                            {"code": "EMPLOYEE", "name": "EMPLOYEE ADMIN"},
                            {"code": "COMPLAINANT", "name": "COMPLAINANT"},
                        ],
                        "uuid": None,
                        "tenantId": user["tenant_id"],
                    },
                    "code": user["username"],
                    "jurisdictions": [
                        {
                            "hierarchy": "ADMIN",
                            "roles": [
                                {"value": "EMPLOYEE", "label": "EMPLOYEE ADMIN"},
                                {"value": "COMPLAINANT", "label": "COMPLAINANT"},
                            ],
                            "boundaryType": "City",
                            "boundary": user["tenant_id"],
                            "furnishedRolesList": "EMPLOYEE ADMIN, COMPLAINANT",
                            "tenantId": user["tenant_id"],
                        }
                    ],
                    "assignments": [
                        {
                            "fromDate": 1617215400000,
                            "toDate": None,
                            "isCurrentAssignment": True,
                            "department": "DEPT_1",
                            "designation": "DESIG_01",
                        }
                    ],
                    "serviceHistory": [],
                    "education": [],
                    "tests": [],
                }
            ],
        }
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.json())


for index, row in df.iterrows():
    save_user(row)
