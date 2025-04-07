import os
import sys
import pandas as pd
import chardet
import json
import re
import requests

if len(sys.argv) < 3:
    print("Usage python tenant-generator.py <tenant-id> <state-data-file-path>")
    sys.exit(1)

tenant_id = sys.argv[1]
print(f"processing for tenant id: {tenant_id}")

with open(sys.argv[2], "rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)


# columns
health_center_col_name_old = "Health Centre Name"
health_center_col_name_localized = "ಆರೋಗ್ಯ ಕೇಂದ್ರದ ಹೆಸರು"
type_of_hc_col_name_old = "Type of HC"
district_col_name_old = "District"
block_col_name_old = "Block"
# contact_col_name_old = "HC PoC Contact number"
contact_col_name_old = "Contact"
# username_col_name_old = "HFR ID / NIN (username)"
username_col_name_old = "NIN"
# poc_name_col_name_old = "HC PoC Name"
poc_name_col_name_old = "POC Name"
vendor_name_col_name_old = "Vendor name"

# renamed
health_center_col_name = "health_center_name"
type_of_hc_col_name = "type_of_hc"
district_col_name = "district"
block_col_name = "block"
contact_col_name = "contact"
username_col_name = "username"
poc_name_col_name = "poc_name"
vendor_name_col_name = "vendor_name"

# additional
health_center_code = "health_center_code"
district_code = "district_code"
block_code = "block_code"

# read csv
df = pd.read_csv(
    sys.argv[2],
    # usecols=[
    #     health_center_col_name_old,
    #     type_of_hc_col_name_old,
    #     district_col_name_old,
    #     block_col_name_old,
    #     contact_col_name_old,
    # ],
    encoding=result["encoding"],
)
df.columns = df.columns.str.strip()

df = df.filter(
    [
        health_center_col_name_old,
        type_of_hc_col_name_old,
        district_col_name_old,
        block_col_name_old,
        contact_col_name_old,
        username_col_name_old,
        poc_name_col_name_old,
        vendor_name_col_name_old,
        health_center_col_name_localized,
    ]
)

df = df.rename(
    columns={
        health_center_col_name_old: health_center_col_name,
        type_of_hc_col_name_old: type_of_hc_col_name,
        district_col_name_old: district_col_name,
        block_col_name_old: block_col_name,
        contact_col_name_old: contact_col_name,
        username_col_name_old: username_col_name,
        poc_name_col_name_old: poc_name_col_name,
        vendor_name_col_name_old: vendor_name_col_name,
    }
)

df[health_center_col_name] = df[health_center_col_name].str.strip()
df[type_of_hc_col_name] = df[type_of_hc_col_name].str.strip().str.upper()
df[district_col_name] = df[district_col_name].str.strip()
df[block_col_name] = df[block_col_name].str.strip()
df[contact_col_name] = df[contact_col_name].astype(str).str.strip()
df[username_col_name] = df[username_col_name].astype(str).str.strip()
df[poc_name_col_name] = df[poc_name_col_name].astype(str).str.strip()
# df[vendor_name_col_name] = df[vendor_name_col_name].astype(str).str.strip()


df[health_center_code] = (
    df[health_center_col_name]
    .str.lower()
    .str.replace(r"\s+", "", regex=True)
    .str.replace("\u00fc", "u")
    .str.replace(r"[\/\-\.]", "", regex=True)
    .apply(lambda x: f"{tenant_id}.{x}")
)


# print(df[vendor_name_col_name].unique().tolist())


# vendor_tenant_map = (
#     df.groupby(vendor_name_col_name)[health_center_code]
#     .agg(list)
#     .to_json("vendor-tenants-map.json", indent=2)
# )


# with open("vendor-tenants-map.json", "w") as f:
#     json.dump(vendor_tenant_map, f, indent=2)


# sys.exit()


df[district_code] = (
    df[district_col_name].str.upper().str.replace(r"\s+", "", regex=True)
)
df[block_code] = df[[district_col_name, block_col_name]].apply(
    lambda x: re.sub(
        "/",
        "",
        f"{re.sub(r"\s+", "", x[district_col_name].lower())}.{re.sub(r"\s+", "", x[block_col_name].lower())}",
    ),
    axis=1,
)

# Validate unique values in a single column
if df[health_center_col_name].duplicated().any():
    raise ValueError("Duplicate values found in Health Center Name column!")

# if not df[type_of_hc_col_name].isin({"HWC", "SC", "PHC"}).all():
#     raise ValueError("Invalid HC Type")

if df[username_col_name].isna().any():
    raise ValueError("username is mandatory")

state_df = pd.DataFrame(
    {
        health_center_col_name: ["State"],
        type_of_hc_col_name: ["CITY"],
        district_col_name: [None],
        block_col_name: [None],
        contact_col_name: [None],
        health_center_code: [tenant_id],
        district_code: [None],
        block_code: [None],
        username_col_name: [None],
        poc_name_col_name: [None],
    }
)

full_df = pd.concat([state_df, df], ignore_index=True)

block_code_filtered_df = df[~df[block_code].str.match(r"^[a-zA-Z.]+$")]

# comment below if you have to manually fix post generation
if len(block_code_filtered_df) > 0:
    print(block_code_filtered_df)
    raise ValueError("Invalid block codes")

tenant_module_mdms = {
    "tenantId": tenant_id,
    "moduleName": "tenant",
    "tenants": full_df.apply(
        lambda x: {
            "code": x[health_center_code],
            "name": x[health_center_col_name],
            "description": x[health_center_col_name],
            "centreType": x[type_of_hc_col_name],
            "pincode": None,
            "domainUrl": "https://e4h-dev.selcofoundation.org",
            "type": x[type_of_hc_col_name],
            "logoId": None,
            "imageId": None,
            "twitterUrl": None,
            "facebookUrl": None,
            "OfficeTimings": {"Mon - Fri": "9.00 AM - 6.00 PM"},
            "city": {
                "name": x[health_center_col_name],
                "localName": None,
                "districtCode": x[district_code],
                "districtName": x[district_col_name],
                "blockCode": x[block_code],
                "districtTenantCode": x[health_center_code],
                "regionName": None,
                "ulbGrade": None,
                "longitude": None,
                "latitude": None,
                "shapeFileLocation": None,
                "captcha": None,
                "code": (
                    f"{tenant_id.upper()}-{x[username_col_name]}"
                    if x[username_col_name] is not None
                    else "0"
                ),
                "ddrName": None,
            },
            "address": "Nagaland",
            "contactNumber": x[contact_col_name],
        },
        axis=1,
    ).tolist(),
}


tenantsFileName = f"output/{tenant_id}/{sys.argv[2]}/tenants.json"
os.makedirs(os.path.dirname(tenantsFileName), exist_ok=True)


with open(tenantsFileName, "w") as f:
    json.dump(tenant_module_mdms, f, indent=2)


df["Employees__employeeStatus"] = "EMPLOYED"
df["Employees__user__relationship"] = "FATHER"
df["Employees__user__gender"] = "MALE"
df["Employees__user__dob"] = "760645800001"
df["Employees__user__roles__code"] = "EMPLOYEE,COMPLAINANT"
df["Employees__dateOfAppointment"] = "1617215400001"
df["Employees__employeeType"] = "PERMANENT"
df["Employees__assignments__toDate"] = "null"
df["Employees__assignments__isCurrentAssignment"] = "TRUE"
df["Employees__assignments__department"] = "DEPT_1"
df["Employees__assignments__designation"] = "DESIG_01"
df["Employees__assignments__isHOD"] = "false"

users_df = df.rename(
    columns={
        health_center_code: "Employees__tenantId",
        username_col_name: "Employees__code",
        poc_name_col_name: "Employees__user__name",
        contact_col_name: "Employees__user__mobileNumber",
        health_center_col_name: "Employees__user__fatherOrHusbandName",
    }
)

cols_to_keep = [
    "Employees__tenantId",
    "Employees__employeeStatus",
    "Employees__user__name",
    "Employees__user__mobileNumber",
    "Employees__user__fatherOrHusbandName",
    "Employees__user__relationship",
    "Employees__user__gender",
    "Employees__user__dob",
    "Employees__user__roles__code",
    "Employees__code",
    "Employees__dateOfAppointment",
    "Employees__employeeType",
    "Employees__assignments__toDate",
    "Employees__assignments__isCurrentAssignment",
    "Employees__assignments__department",
    "Employees__assignments__designation",
    "Employees__assignments__isHOD",
]

users_df = users_df[cols_to_keep]


usersFileName = f"output/{tenant_id}/{sys.argv[2]}/users.xlsx"
os.makedirs(os.path.dirname(usersFileName), exist_ok=True)
users_df.to_excel(usersFileName, sheet_name="jsonxsl", index=False)

districts = df[district_col_name].unique()

district_mdms = {
    "tenantId": tenant_id,
    "moduleName": "Incident",
    "District": list(
        map(lambda x: {"code": x.upper(), "name": x.title(), "active": True}, districts)
    ),
}

districtsFileName = f"output/{tenant_id}/{sys.argv[2]}/District.json"
os.makedirs(os.path.dirname(districtsFileName), exist_ok=True)

with open(districtsFileName, "w") as f:
    json.dump(district_mdms, f, indent=2)


assert block_code in df.columns
assert block_col_name in df.columns
assert district_code in df.columns


filtered_df = df.drop_duplicates(subset=[block_code], keep="first")

blocks = (
    filtered_df[[block_code, block_col_name, district_code]]
    .apply(
        lambda x: (
            {
                "code": x[block_code],
                "name": x[block_col_name],
                "districtCode": x[district_code],
                "active": True,
            }
        ),
        axis=1,
    )
    .tolist()
)


block_mdms = {
    "tenantId": tenant_id,
    "moduleName": "Incident",
    "Block": blocks,
}

blockFileName = f"output/{tenant_id}/{sys.argv[2]}/Block.json"
os.makedirs(os.path.dirname(blockFileName), exist_ok=True)

with open(blockFileName, "w") as f:
    json.dump(block_mdms, f, indent=2)


def upsert_localization(tenant_id, module, messages):
    url = "http://localhost:8082/localization/messages/v1/_upsert"
    payload = json.dumps(
        {
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
            "tenantId": tenant_id,
            "module": module,
            "locale": "en_IN",
            "messages": messages,
        }
    )
    headers = {"Content-Type": "application/json"}

    print(payload)

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.json())


modules = [
    # "rainmaker-hrms",
    # "rainmaker-pg", # needs to be done separately as module name itself changes
    # "rainmaker-im",
    "rainmaker-common",
    # "rainmaker-hr",
]

for module in modules:
    messages = (
        df[[health_center_code, health_center_col_name]]
        .apply(
            lambda x: (
                {
                    "code": f"TENANT_TENANTS_{x[health_center_code].upper().replace(".","_")}",
                    "message": x[health_center_col_name],
                    "module": "rainmaker-common",
                    "locale": "en_IN",
                }
                # re.sub("\.","_",x[health_center_code].upper())
            ),
            axis=1,
        )
        .tolist()
    )
    # messages = (
    #     df[[health_center_code, health_center_col_name_localized]]
    #     .apply(
    #         lambda x: (
    #             {
    #                 "code": f"TENANT_TENANTS_{x[health_center_code].upper().replace(".","_")}",
    #                 "message": x[health_center_col_name_localized],
    #                 "module": "rainmaker-common",
    #                 "locale": "ka_IN",
    #             }
    #             # re.sub("\.","_",x[health_center_code].upper())
    #         ),
    #         axis=1,
    #     )
    #     .tolist()
    # )
    print(messages[0])
    with open("messages.json", "w") as f:
        json.dump(messages, f, indent=2)
    upsert_localization("pg", module, messages)
