import pandas as pd

df = pd.read_excel("mappings.xlsx", sheet_name="SupplyLookupMappings")
print("Raw columns:", list(df.columns))