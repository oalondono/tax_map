# %%
import requests
import json
import geopandas as gpd

# %%
state_name = "California"
county_name = "Orange County"

# %% Get state list & GEOID
census_2020_state_county = "https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/State_County/MapServer"
'''
Layers:

    States (0)
    Counties (1) 
'''


params = {
    'where': '1=1',                
    'outFields': 'NAME,GEOID',     
    'orderByFields': 'NAME',       
    'returnGeometry': 'false',     
    'f': 'json'                    
}

response = requests.get(f"{census_2020_state_county}/0/query", params=params)
data = response.json()
state_list = []
for feature in data.get('features', []):
    attributes = feature.get('attributes', {})
    state_list.append(attributes)

state_geoid = [item["GEOID"] for item in state_list if item["NAME"] == state_name][0]

# %% # get county map of interest
params = {
    'where': f'STATE=\'{state_geoid}\' AND NAME=\'{county_name}\'',                    
    'orderByFields': 'NAME',          
    'f': 'json'                    
}
response = requests.get(f"{census_2020_state_county}/1/query", params=params)
data = response.json()
county_geometry = data['features'][0]['geometry']

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
    'where': '1=1',
    'geometry' : json.dumps(county_geometry),
    'geometryType' : 'esriGeometryPolygon', 
    'outFields': '*',
    'spatialRel': 'esriSpatialRelIntersects',                     
    'f': 'geojson'                    
}
response = requests.post(f"{census_2020_places}/4/query", data=params)
data = response.json()
places_gpd = gpd.GeoDataFrame.from_features(data["features"])


# %%'outFields': '*'
census_2020_tracts_blocks = "https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/"
'''
    Labels 'outFields': '*'(0)
        Census Tracts 500K (1)
        Census Block Groups 500K (2)
    Census Tracts 500K (3)
    Census Block Groups 500K (4)

'''

'''
    Labels (0)
        County Subdivisions 500K (1)
        Subbarrios 500K (2)
        Consolidated Cities 500K (3)
        Incorporated Places 500K (4)
        Census Designated Places 500K (5)
    Subbarrios 500K (8)
    County Subdivisions 500K (7)
    Consolidated Cities 500K (9)
    Incorporated Places 500K (10)
    Census Designated Places 500K (11)

'''

# %%