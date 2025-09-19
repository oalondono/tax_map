# %%
import sqlite3
import pandas as pd
import geopandas as gpd
import pyarrow
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.utils import ROOT_DIR
import os

# %%  Extract parcel and tax data from the database
db_path = os.path.join(ROOT_DIR,"data/parcel_tax/CA_OC_parcel_tax_202508.db")

with sqlite3.connect(db_path) as con:
	query = """
	SELECT *
	FROM parcels p
	"""
	parcels = pd.read_sql_query(query, con)

with sqlite3.connect(db_path) as con:
	query = """
	SELECT *
	FROM parcels p
	JOIN special_assessments sa ON p.apn = sa.parcel_apn
	"""
	special_assessments = pd.read_sql_query(query, con)

with sqlite3.connect(db_path) as con:
	query = """
	SELECT *
	FROM parcels p
	JOIN property_taxes pt ON p.apn = pt.parcel_apn
	"""
	property_taxes = pd.read_sql_query(query, con)
	
# %% Aggregate total taxes and assessments per parcel
property_taxes_total = property_taxes.groupby('parcel_apn')['amount_usd'].sum().reset_index()
special_assessments_total = special_assessments.groupby('parcel_apn')['amount_usd'].sum().reset_index()

# %% Merge totals back to parcels
parcel_tax = parcels.merge(property_taxes_total, left_on='apn', right_on='parcel_apn', how='left', suffixes=('', '_property_tax'))
parcel_tax.pop('parcel_apn')
parcel_tax.rename(columns={'amount_usd': 'property_tax_total_usd'}, inplace=True)

parcel_tax = parcel_tax.merge(special_assessments_total, left_on='apn', right_on='parcel_apn', how='left', suffixes=('', '_special_assessment'))
parcel_tax.pop('parcel_apn')
parcel_tax.rename(columns={'amount_usd': 'special_assessment_total_usd'}, inplace=True)
parcel_tax['total_tax_usd'] = parcel_tax['property_tax_total_usd'].fillna(0) + parcel_tax['special_assessment_total_usd'].fillna(0)
# %% read parcel map
parcel_map_path = os.path.join(ROOT_DIR,"data/parcel_tax/CA_OC_parcel_map_202508.parquet")
parcel_map = gpd.read_parquet(parcel_map_path)
# %% Read census maps
place_map = gpd.read_parquet(os.path.join(ROOT_DIR,"data/maps/CA_OC_place_2020.parquet"))
place_map = place_map.set_crs(4269, allow_override=True)
place_map = place_map.to_crs(parcel_map.crs)

tract_map = gpd.read_parquet(os.path.join(ROOT_DIR,"data/maps/CA_OC_tract_2020.parquet"))
tract_map = tract_map.set_crs(4269, allow_override=True)
tract_map = tract_map.to_crs(parcel_map.crs)

block_group_map = gpd.read_parquet(os.path.join(ROOT_DIR,"data/maps/CA_OC_block_group_2020.parquet"))
block_group_map = block_group_map.set_crs(4269, allow_override=True)
block_group_map = block_group_map.to_crs(parcel_map.crs)

block_map = gpd.read_parquet(os.path.join(ROOT_DIR,"data/maps/CA_OC_block_2020.parquet"))
block_map = block_map.set_crs(4269, allow_override=True)
block_map = block_map.to_crs(parcel_map.crs)
# %%
parcel_map_census = parcel_map.sjoin(place_map[['BASENAME', 'geometry']], how='left', predicate='intersects')
parcel_map_census = parcel_map.sjoin(tract_map[['TRACT', 'geometry']], how='left', predicate='intersects')
parcel_map_census = parcel_map.sjoin(tract_map[['GEOID', 'BLKGRP', 'geometry']], how='left', predicate='intersects').rename(columns={'GEOID': 'GEOID_BLOCK_GROUP'})
parcel_map_census = parcel_map.sjoin(tract_map[['GEOID', 'BLOCK' 'geometry']], how='left', predicate='intersects').rename(columns={'GEOID': 'GEOID_BLOCK'})
# %%
