import os
import sys
import pandas as pd
import chardet
import json

if len(sys.argv) < 3:
    print("Usage python citymodule-generator.py <tenant-id> <state-data-file-path>")
    sys.exit(1)

tenant_id = sys.argv[1]
print(f"processing for tenant id: {tenant_id}")

with open(sys.argv[2], "rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)

healthCenterColName = "health_center_name"
df = pd.read_csv(
    sys.argv[2], usecols=["Health Centre Name", "District"], encoding=result["encoding"]
)
df = df.rename(columns={"Health Centre Name": healthCenterColName})

df["old_name"] = df[healthCenterColName]

df[healthCenterColName] = (
    df[healthCenterColName]
    .str.strip()
    .str.lower()
    .str.replace(r"\s+", "", regex=True)
    .str.replace(r"[\/\-\.]", "", regex=True)
)
city_codes = (
    df[healthCenterColName].apply(lambda x: {"code": f"{tenant_id}.{x}"}).tolist()
)


filtered_df = df[~df[healthCenterColName].str.match(r"^[a-zA-Z.]+$")]


# comment below if you have to manually fix post generation

if len(filtered_df) > 0:
    print(filtered_df)
    filtered_df.to_csv("invalid names.csv")
    # raise ValueError("Invalid PHC names")


# Validate unique values in a single column
if df[healthCenterColName].duplicated().any():
    print(df[df[healthCenterColName].duplicated()])
    df[df[healthCenterColName].duplicated()].to_csv(
        "duplicated health centre names.csv"
    )
    raise ValueError("Duplicate values found in Health Center Name column!")


cityModuleMdms = {
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

cityModuleFileName = f"output/{tenant_id}/{sys.argv[2]}/citymodule.json"
os.makedirs(os.path.dirname(cityModuleFileName), exist_ok=True)

with open(cityModuleFileName, "w") as f:
    json.dump(cityModuleMdms, f, indent=2)
