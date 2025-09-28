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
import matplotlib.pyplot as plt
import mapclassify
import pysal
import numpy as np
import seaborn as sns

# %%
boundary_types = ['place', 'tract', 'blkgrp', 'block']
agg_cols = ['total_taxable_value', 'property_tax_total_usd', 'special_assessment_total_usd', 'total_tax_usd']

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
land_map = gpd.read_file(os.path.join(ROOT_DIR,"data/ne_10m_land/ne_10m_land.shp")).set_crs(4269, allow_override=True).to_crs(parcel_map.crs)
# %%
census_maps = {}
for boundary in boundary_types:
	census_maps[boundary] = gpd.read_parquet(os.path.join(ROOT_DIR,f"data/maps/CA_OC_{boundary}_2020.parquet")).set_crs(4269, allow_override=True).to_crs(parcel_map.crs)

census_maps['place'] = gpd.clip(census_maps['place'], land_map)
census_maps['place'].rename(columns={'BASENAME': 'PLACE'}, inplace=True)

# %%
parcel_map_census =  parcel_map
for boundary in boundary_types:
	parcel_map_census = parcel_map_census.sjoin(census_maps[f"{boundary}"][['GEOID', boundary.upper(), 'geometry']], how='left', predicate='intersects').drop(columns=['index_right']).rename(columns={'GEOID': f'GEOID_{boundary.upper()}'})

# group by boundaries and sum total tax, then get density by area, then merge that onto the census maps
parcel_map_census = parcel_map_census.merge(parcel_tax, how='left', left_on='AssessmentNo', right_on='apn')
# %%
parcel_map_agg = {}
for boundary in boundary_types:
	parcel_map_agg[boundary] = parcel_map_census[[f"GEOID_{boundary.upper()}"] + agg_cols].groupby(by=f"GEOID_{boundary.upper()}").sum()
	parcel_map_agg[boundary] = parcel_map_agg[boundary].merge(census_maps[boundary], how='left', left_on=f"GEOID_{boundary.upper()}", right_on='GEOID').set_geometry('geometry')
# %%
for boundary in boundary_types:
	parcel_map_agg[boundary]['AREALAND_ACRE'] = parcel_map_agg[boundary]['AREALAND']/(4046.86)  # convert square meters to acres
	for col in agg_cols:
		parcel_map_agg[boundary][f'{col}_PER_ACRE'] = parcel_map_agg[boundary][col] / parcel_map_agg[boundary]['AREALAND_ACRE']
# %%
for boundary in boundary_types:
	for col in agg_cols:
		fig, ax = plt.subplots(figsize=(10, 10))
		parcel_map_agg[boundary].plot(
			ax=ax,
			column=f'{col}_PER_ACRE',
			scheme='quantiles',
			k=10,
			legend=True,
			legend_kwds={
				'loc': 'upper left',
				'bbox_to_anchor': (1.05, 1),
				'title': f'{col}_per_acre',
				'fmt': '${:,.0f}'
			},
		)
		parcel_map_agg['place'].boundary.plot(ax=ax, color='white', linewidth=2)
		plt.title(f'{col} per acre by {boundary}')
		plt.show()


# %%
