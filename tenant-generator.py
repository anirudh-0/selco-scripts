import sys
import pandas as pd
import chardet
import json
import re
import math

if len(sys.argv) < 3:
    print("Usage python tenant-generator.py <tenant-id> <state-data-file-path>")
    sys.exit(1)

tenant_id = sys.argv[1]

with open(sys.argv[2], "rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)

print(f"processing for tenant id: {tenant_id}")

# columns
health_center_col_name_old = "Health Centre Name"
type_of_hc_col_name_old = "Type of HC"
district_col_name_old = "District"
block_col_name_old = "Block"
contact_col_name_old = "HC PoC Contact number"
username_col_name_old = "HFR ID / NIN (username)"

# renamed
health_center_col_name = "health_center_name"
type_of_hc_col_name = "type_of_hc"
district_col_name = "district"
block_col_name = "block"
contact_col_name = "contact"
username_col_name = "username"
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
    }
)

df[health_center_col_name] = df[health_center_col_name].str.strip()
df[type_of_hc_col_name] = df[type_of_hc_col_name].str.strip().str.upper()
df[district_col_name] = df[district_col_name].str.strip()
df[block_col_name] = df[block_col_name].str.strip()
df[contact_col_name] = df[contact_col_name].astype(str).str.strip()
df[username_col_name] = df[username_col_name].astype(str).str.strip()

df[health_center_code] = (
    df[health_center_col_name]
    .str.lower()
    .str.replace(r"\s+", "-", regex=True)
    .apply(lambda x: f"{tenant_id}.{x}")
)
df[district_code] = (
    df[district_col_name].str.upper().str.replace(r"\s+", "-", regex=True)
)
df[block_code] = df[[district_col_name, block_col_name]].apply(
    lambda x: f"{re.sub(r"\s+", "-", x[district_col_name].lower())}.{re.sub(r"\s+", "-", x[block_col_name].lower())}",
    axis=1,
)

# Validate unique values in a single column
if df[health_center_col_name].duplicated().any():
    raise ValueError("Duplicate values found in Health Center Name column!")

if not df[type_of_hc_col_name].isin({"HWC", "SC", "PHC"}).all():
    raise ValueError("Invalid HC Type")

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
    }
)

df = pd.concat([state_df, df], ignore_index=True)

tenant_module_mdms = {
    "tenantId": tenant_id,
    "moduleName": "tenant",
    "tenants": df.apply(
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


with open("tenants.json", "w") as f:
    json.dump(tenant_module_mdms, f, indent=4)
