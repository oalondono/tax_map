# %%
import sqlite3
import pandas as pd

# %%
con = sqlite3.connect("/home/ol/Repositories/tax_map/data/parcel_tax/CA_OC_parcel_tax_202508.db")
query = """
SELECT *
FROM parcels p
JOIN special_assessments pt ON p.apn = pt.parcel_apn
"""
df_westminster = pd.read_sql_query(query, con)
con.close()