import sys
import pandas as pd
import chardet

if len(sys.argv) < 3:
    print("Usage python citymodule-generator.py <tenant-id> <state-data-file-path>")
    sys.exit(1)

tenant_id = sys.argv[1]

with open(sys.argv[2], "rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)

print(f"processing for tenant id: {tenant_id}")
healthCenterColName = "health_center_name"
df = pd.read_csv(sys.argv[2], usecols=["Health Centre Name"], encoding=result["encoding"])
df = df.rename(columns={"Health Centre Name": healthCenterColName })


df[healthCenterColName] = df[healthCenterColName].str.strip().str.lower().str.replace(r"\s+", "-", regex=True)
city_codes = df[healthCenterColName].apply(lambda x: {"code": f"{tenant_id}.{x}"}).tolist()

# Validate unique values in a single column
if df[healthCenterColName].duplicated().any():
    raise ValueError("Duplicate values found in Health Center Name column!")



