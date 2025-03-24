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
print(f"processing for tenant id: {tenant_id}")

with open(sys.argv[2], "rb") as f:
    result = chardet.detect(f.read(100000))
    print(result)

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

df = df.rename(columns={"": ""})
