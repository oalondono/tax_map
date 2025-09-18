# %%
import requests
import json
import geopandas as gpd
import pyarrow
import pandas as pd
import os
import time
from dotenv import load_dotenv
load_dotenv()

# %% Get GEOIDs of incorporated places
census_api_key = os.environ.get("CENSUS_API_KEY")
state_FIPS = "06" # California
county_FIPS = "059" # Orange County
url = f'https://api.census.gov/data/2020/geoinfo?get=NAME&for=place%20(or%20part):*&in=state:{state_FIPS}%20county:{county_FIPS}&key={census_api_key}'
response = requests.get(url)
data = response.json()
places = pd.DataFrame(data[1:], columns=data[0])
places['GEOID'] = places['state'] + places['place (or part)']

# %% # get county map of interest
census_2020_state_county = "https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/State_County/MapServer"
'''
Layers:

    States (0)
    Counties (1) 
'''
params = {
    'where': f'GEOID=\'{state_FIPS}{county_FIPS}\'', 
    'outFields': '*',                   
    'orderByFields': 'NAME',          
    'f': 'geojson'                    
}
response = requests.get(f"{census_2020_state_county}/1/query", params=params)
data = response.json()
county_gdf = gpd.GeoDataFrame.from_features(data["features"])

# %% Get map of incorporated places in county of interest
census_2020_places = "https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/Places_CouSub_ConCity_SubMCD/MapServer/"
'''
Layers:
    Estates (0)
    County Subdivisions (1)
    Subbarrios (2)
    Consolidated Cities (3)
    Incorporated Places (4)
    Census Designated Places (5) 
'''
params = {
    'where': f'GEOID IN ({",".join([f"'{geoid}'" for geoid in places["GEOID"]])})',                  
    'outFields': '*',
    'f': 'geojson'                    
}
response = requests.post(f"{census_2020_places}/4/query", data=params)
data = response.json()
place_gdf = gpd.GeoDataFrame.from_features(data["features"])

# %% get tracks and block groups maps
census_2020_tracts_blocks = "https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/"
'''
Layers:
    Census Tracts (0)
    Census Block Groups (1)
    Census Blocks (2)
    Census 2010 (3)
        Census Tracts (4)
        Census Block Groups (5)
        Census Blocks (6)
    Census 2000 (7)
        Census Tracts (8)
        Census Block Groups (9)
        Census Blocks (10)
'''

params = {
    'where': f'STATE=\'{state_FIPS}\' AND COUNTY=\'{county_FIPS}\'',
    'outFields': '*',                  
    'f': 'geojson'                    
}
response = requests.get(f"{census_2020_tracts_blocks}/0/query", params=params)
data = response.json()
tract_gdf = gpd.GeoDataFrame.from_features(data["features"])

response = requests.get(f"{census_2020_tracts_blocks}/1/query", params=params)
data = response.json()
block_group_gdf = gpd.GeoDataFrame.from_features(data["features"])

# %% Get map for all blocks in county, takes a while
all_blocks_features = []
for tract in tract_gdf['TRACT']:
    params = {
        'where': f'STATE=\'{state_FIPS}\' AND COUNTY=\'{county_FIPS}\' AND TRACT=\'{tract}\'',
        'outFields': '*',                  
        'f': 'geojson'                    
    }  
    while True:
        try:
            response = requests.get(f"{census_2020_tracts_blocks}/2/query", params=params)
        except:
            time.sleep(60)
            continue
        break
    data = response.json()
    all_blocks_features.extend(data["features"]) 
    time.sleep(10) 

block_gdf = gpd.GeoDataFrame.from_features(all_blocks_features)

# %% export files to parquet
county_gdf.to_parquet('/home/ol/Repositories/tax_map/data/maps/CA_OC_county_2020.parquet')
place_gdf.to_parquet('/home/ol/Repositories/tax_map/data/maps/CA_OC_place_2020.parquet')
tract_gdf.to_parquet('/home/ol/Repositories/tax_map/data/maps/CA_OC_tract_2020.parquet')
block_group_gdf.to_parquet('/home/ol/Repositories/tax_map/data/maps/CA_OC_block_group_2020.parquet')
block_gdf.to_parquet('/home/ol/Repositories/tax_map/data/maps/CA_OC_block_2020.parquet')
# %%
