import os
import shutil
import pandas as pd
import chardet
import json
import re
import requests
import argparse

parser = argparse.ArgumentParser(description="Generate tenant-specific data.")
parser.add_argument("--tenant-id", required=True, help="ID of the tenant (e.g., 'mz')")
parser.add_argument(
    "--language-code",
    action="append",
    required=True,
    help="Repeatable language code flag (e.g., --language-code en_IN --language-code kn_IN)",
)
parser.add_argument("file", help="Path to the input file")
args = parser.parse_args()

print(f"Tenant ID: {args.tenant_id}")
print(f"Languages: {args.language_code}")
print(f"Input File: {args.file}")

tenant_id = args.tenant_id
language_codes = args.language_code
health_centre_file_name = args.file


# Columns
sl_no_col = "Sl. No"
health_centre_name_col = "Health Centre Name"
health_centre_type_col = "Health Centre Type"
district_col = "District"
block_col = "Block"
hfr_id_col = "HFR ID"
nin_id_col = "NIN ID"
password_col = "Password"
poc_name_col = "POC Name"
designation_col = "Designation"
contact_col = "Contact"
latitude_col = "Latitude"
longitude_col = "Longitude"
vendor_username_col = "VENDOR.Username"

# additional
health_center_code_col = "Health Centre Code"
district_code_col = "District Code"
block_code_col = "Block Code"
username_col = "Username"

# localized columns
localized_columns = []
for language in language_codes:
    if language == "en_IN":
        continue
    for col in [
        health_centre_name_col,
        health_centre_type_col,
        district_col,
        block_col,
    ]:
        localized_columns.append(f"{col}-{language}")

# read csv
with open(health_centre_file_name, "rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)

df = pd.read_csv(
    health_centre_file_name,
    usecols=[
        sl_no_col,
        health_centre_name_col,
        health_centre_type_col,
        district_col,
        block_col,
        hfr_id_col,
        nin_id_col,
        password_col,
        poc_name_col,
        designation_col,
        contact_col,
        latitude_col,
        longitude_col,
        vendor_username_col,
        *localized_columns,
    ],
    dtype=str,
    keep_default_na=False,
    encoding=result["encoding"],
)

# clean up cell data
df.columns = df.columns.str.strip()
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

df[username_col] = df[hfr_id_col].where(
    df[hfr_id_col].str.strip() != "", df[nin_id_col]
)


# Validation
def validate_columns(df, allowed_pattern, columns):
    df_to_check = df[columns]
    has_invalid_data = False
    for col in df_to_check.columns:
        mask = ~df_to_check[col].str.fullmatch(allowed_pattern)
        bad_values = df.loc[mask, [sl_no_col, col]]

        if not bad_values.empty:
            print(f"\nInvalid values in column: {col}")
            print(bad_values.to_string(index=False))
            has_invalid_data = True
    return has_invalid_data


## Validate data present
has_invalid_data = False
has_invalid_data = validate_columns(
    df,
    r"^[a-zA-Z\s]+$",
    [district_col, block_col],
)
has_invalid_data = validate_columns(
    df,
    r"^[a-zA-Z\s\-.]+$",
    [health_centre_name_col, health_centre_type_col],
)

has_invalid_data = validate_columns(
    df,
    r"^[a-zA-Z0-9\-.]+$",
    [username_col],
)
if has_invalid_data:
    raise ValueError("Invalid Data found")

df[health_centre_type_col] = df[health_centre_type_col].str.upper()
df[health_center_code_col] = (
    df[health_centre_name_col]
    .str.lower()
    .str.replace(r"\s+", "", regex=True)
    .str.replace(r"[\/\-\.]", "", regex=True)
    .apply(lambda x: f"{tenant_id}.{x}")
)

# Uniqueness validation
has_invalid_data = False
df_to_check_uniqueness = df[[health_center_code_col, username_col, contact_col]]

for col in df_to_check_uniqueness.columns:
    duplicates = df[df.duplicated(subset=[col], keep=False)]
    if not duplicates.empty:
        dup_values = duplicates[[sl_no_col, col]].sort_values(by=col)

        print(f"\nDuplicate values in column: {col}")
        print(dup_values.to_string(index=False))
        has_invalid_data = True
if has_invalid_data:
    raise ValueError("Invalid Data found")


df[district_code_col] = (
    df[district_col].str.upper().str.replace(r"\s+", "", regex=True)
)


def block_code_gen(x):
    district_lower = re.sub(r"\s+", "", x[district_col].lower())
    block_lower = re.sub(r"\s+", "", x[block_col].lower())
    return f"{district_lower}.{block_lower}"


df[block_code_col] = df[[district_col, block_col]].apply(
    block_code_gen,
    axis=1,
)

state_df = pd.DataFrame(
    {
        health_centre_name_col: ["State"],
        health_centre_type_col: ["CITY"],
        district_col: [None],
        block_col: [None],
        contact_col: [None],
        health_center_code_col: [tenant_id],
        district_code_col: [None],
        block_code_col: [None],
        username_col: [None],
        poc_name_col: [None],
    }
)

full_df = pd.concat([state_df, df], ignore_index=True)

block_code_filtered_df = df[~df[block_code_col].str.match(r"^[a-zA-Z.]+$")]

# comment below if you have to manually fix post generation
if len(block_code_filtered_df) > 0:
    print(block_code_filtered_df)
    raise ValueError("Invalid block codes")

city_codes = (
    full_df[health_center_code_col]
    .apply(lambda x: {"code": x if x else tenant_id})
    .tolist()
)

city_module_mdms = {
    "tenantId": tenant_id,
    "moduleName": "tenant",
    "citymodule": [
        {
            "module": "HRMS",
            "code": "HRMS",
            "active": True,
            "order": 2,
            "tenants": city_codes,
        },
        {
            "module": "IM",
            "code": "IM",
            "bannerImage": "https://egov-uat-assets.s3.amazonaws.com/PGR.png",
            "active": True,
            "order": 2,
            "tenants": city_codes,
        },
    ],
}

tenant_module_mdms = {
    "tenantId": tenant_id,
    "moduleName": "tenant",
    "tenants": full_df.apply(
        lambda x: {
            "code": x[health_center_code_col],
            "name": x[health_centre_name_col],
            "description": x[health_centre_name_col],
            "centreType": x[health_centre_type_col],
            "pincode": None,
            "domainUrl": "https://e4h-dev.selcofoundation.org",
            "type": x[health_centre_type_col],
            "logoId": None,
            "imageId": None,
            "twitterUrl": None,
            "facebookUrl": None,
            "OfficeTimings": {"Mon - Fri": "9.00 AM - 6.00 PM"},
            "city": {
                "name": x[health_centre_name_col],
                "localName": None,
                "districtCode": x[district_code_col],
                "districtName": x[district_col],
                "blockCode": x[block_code_col],
                "districtTenantCode": x[health_center_code_col],
                "regionName": None,
                "ulbGrade": None,
                "longitude": None,
                "latitude": None,
                "shapeFileLocation": None,
                "captcha": None,
                "code": (
                    f"{tenant_id.upper()}-{x[username_col]}"
                    if x[username_col] is not None
                    else "0"
                ),
                "ddrName": None,
            },
            "address": "Nagaland",
            "contactNumber": x[contact_col],
        },
        axis=1,
    ).tolist(),
}

if os.path.exists(f"output/{tenant_id}"):
    shutil.rmtree(f"output/{tenant_id}")

vendor_tenants_map_file = f"output/{tenant_id}/vendor-tenants-map.json"
os.makedirs(os.path.dirname(vendor_tenants_map_file), exist_ok=True)
df.groupby(vendor_username_col)[health_center_code_col].agg(list).to_json(
    vendor_tenants_map_file, indent=2
)

tenants_file_name = f"output/{tenant_id}/tenants.json"
os.makedirs(os.path.dirname(tenants_file_name), exist_ok=True)

with open(tenants_file_name, "w") as f:
    json.dump(tenant_module_mdms, f, indent=2)

city_module_file_name = f"output/{tenant_id}/citymodule.json"
os.makedirs(os.path.dirname(city_module_file_name), exist_ok=True)

with open(city_module_file_name, "w") as f:
    json.dump(city_module_mdms, f, indent=2)

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
        health_center_code_col: "Employees__tenantId",
        username_col: "Employees__code",
        poc_name_col: "Employees__user__name",
        contact_col: "Employees__user__mobileNumber",
        health_centre_name_col: "Employees__user__fatherOrHusbandName",
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


usersFileName = f"output/{tenant_id}/users.xlsx"
os.makedirs(os.path.dirname(usersFileName), exist_ok=True)
users_df.to_excel(usersFileName, sheet_name="jsonxsl", index=False)

districts = df[district_col].unique()

district_mdms = {
    "tenantId": tenant_id,
    "moduleName": "Incident",
    "District": list(
        map(lambda x: {"code": x.upper(), "name": x.title(), "active": True}, districts)
    ),
}

districtsFileName = f"output/{tenant_id}/District.json"
os.makedirs(os.path.dirname(districtsFileName), exist_ok=True)

with open(districtsFileName, "w") as f:
    json.dump(district_mdms, f, indent=2)


assert block_code_col in df.columns
assert block_col in df.columns
assert district_code_col in df.columns


filtered_df = df.drop_duplicates(subset=[block_code_col], keep="first")

blocks = (
    filtered_df[[block_code_col, block_col, district_code_col]]
    .apply(
        lambda x: (
            {
                "code": x[block_code_col],
                "name": x[block_col],
                "districtCode": x[district_code_col],
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

blockFileName = f"output/{tenant_id}/Block.json"
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
        df[[health_center_code_col, health_centre_name_col]]
        .apply(
            lambda x: (
                {
                    "code": f"TENANT_TENANTS_{x[health_center_code_col].upper().replace(".","_")}",
                    "message": x[health_centre_name_col],
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
    message_file_name = f"output/{tenant_id}/messages.json"
    os.makedirs(os.path.dirname(message_file_name), exist_ok=True)
    with open(message_file_name, "w") as f:
        json.dump(messages, f, indent=2)
    # upsert_localization("pg", module, messages)
