# tax_map
Over 70 American municipalities ([1](https://marroninstitute.nyu.edu/projects/debt-and-urban-poverty) have declared bankruptcy since 2007. A city is motivated to maximize the revenue generated within its borders (property tax, sales tax) in order to meet its financial obligations. 
This is an implementation of value per acre maps for Orange County, CA which is intended to help visualize the spatial distribution of property tax revenue. These are similar to those made by [Urban3](https://www.urbanthree.com/) ([example](https://www.strongtowns.org/journal/2018-10-19-value-per-acre-analysis-a-how-to-for-beginners)).  

In these maps, we are normalizing property tax revenue by land area. This makes the implicit assumption for any given acre of land, we can assume similar level of infrastructure investment based on miles of road, pipes, etc. that span near or across the property. 
This is likely a reasonable asusmption within municipal/incorporated land boundaries.

The results of this map will highlight areas which provide a large property tax return relative to their size. The type of land use in these exceptional areas may provide insight into the type of developments a city wants to promote.
<img width="1067" height="772" alt="tax_blkgrp" src="https://github.com/user-attachments/assets/2d40e3a1-3683-44dc-8333-e5b171b814d8" />

## Data Sources

To obtain the parcel property tax data:
1. A parcel boundary map is downloaded from the OC Public Works [Open Data Portal](https://data-ocpw.opendata.arcgis.com/datasets/OCPW::parcels-with-attributes-1/about) (9/9/2024 version). This includes 985,905 parcels.
2. The tax data was scraped from the [OC Tax Map](https://taxmap.octreasurer.gov/#!/search), using a list of the parcel numbers obtained above.
3. The U.S. Census Boundary maps were obtained using the Census API. These maps aggregate to the block, block group, tract, and place (municipalities) boundaries.

## Methodology
### Tax data scraping
[...]

### Aggregation  
[...]
Aggregation is done to reduce loading time of the map by minimizing the geographic features and to increase the ability for the naked eye to visualize trends.

### Visualization
Static maps were generated using geopandas.  
Interactive maps will also be made so members of the public can more easily identify areas they are familiar with.  
[...]

