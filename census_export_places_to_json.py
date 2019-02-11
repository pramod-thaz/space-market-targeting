import math
import json
import os.path
import tempfile

import requests
import topojson
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from census import Census
from us import states
from shapely import geometry


census_client = Census(open('API.txt').readline().strip())


# census package uses old endpoints for years before 2015
# it's a workaround monkeypatch
def _switch_endpoints(year):
    census_client.acs5.endpoint_url = 'https://api.census.gov/data/%s/acs/%s'
    census_client.acs5.definitions_url = 'https://api.census.gov/data/%s/acs/%s/variables.json'
    census_client.acs5.definition_url = 'https://api.census.gov/data/%s/acs/%s/variables/%s.json'
    census_client.acs5.groups_url = 'https://api.census.gov/data/%s/acs/%s/groups.json'


census_client.acs5._switch_endpoints = _switch_endpoints

state = states.lookup('Texas')

total_population = 'B01001_001E'
household_income = 'B19001_001E'
median_home_value = 'B25077_001E'
median_income_value = 'B06011_001E'

# all data below in only for "in labor force"
male_below_poverty = 'B17005_004E'
unemployed_male_below_poverty = 'B17005_006E'
female_below_poverty = 'B17005_009E'
unemployed_female_below_poverty = 'B17005_011E'
male_above_poverty = 'B17005_015E'
unemployed_male_above_poverty = 'B17005_017E'
female_above_poverty = 'B17005_020E'
unemployed_female_above_poverty = 'B17005_022E'

# ethnic mix
white_population = 'B01001A_001E'
black_population = 'B01001B_001E'
american_indian_population = 'B01001C_001E'
asian_population = 'B01001D_001E'
native_hawaiian_population = 'B01001E_001E'
hispanic_population = 'B01001I_001E'
other_race_population = 'B01001F_001E'

print('Requesting 2017 year data for places')
tract_raw_data_2017 = census_client.acs5.state_place(
    ('NAME', total_population, household_income, median_home_value, median_income_value,
     male_below_poverty, unemployed_male_below_poverty, female_below_poverty, unemployed_female_below_poverty,
     male_above_poverty, unemployed_male_above_poverty, female_above_poverty, unemployed_female_above_poverty,
     white_population, black_population, american_indian_population, asian_population, native_hawaiian_population,
     hispanic_population, other_race_population),
    state.fips, Census.ALL, year=2017
)
data_2017 = pd.DataFrame.from_records(tract_raw_data_2017)
for column in data_2017:
    if column in ('NAME', 'state', 'place'):
        continue
    data_2017[column] = data_2017[column].astype(float, errors='ignore')

data_2017['unemployment_rate_pct_2017'] = (
    (data_2017[unemployed_male_below_poverty] + data_2017[unemployed_male_above_poverty] +
     data_2017[unemployed_female_below_poverty] + data_2017[unemployed_female_above_poverty]) /
    (data_2017[male_below_poverty] + data_2017[male_above_poverty] +
     data_2017[female_below_poverty] + data_2017[female_above_poverty])
) * 100

races_sum = (
    data_2017[white_population] + data_2017[black_population] + data_2017[american_indian_population] +
    data_2017[asian_population] + data_2017[native_hawaiian_population] + data_2017[hispanic_population] +
    data_2017[other_race_population]
)
data_2017['white_race_pct'] = data_2017[white_population] / races_sum * 100
data_2017['black_race_pct'] = data_2017[black_population] / races_sum * 100
data_2017['asian_race_pct'] = data_2017[asian_population] / races_sum * 100
data_2017['hispanic_race_pct'] = data_2017[hispanic_population] / races_sum * 100
data_2017['other_race_pct'] = data_2017[other_race_population] / races_sum * 100

data_2017 = data_2017.rename(columns={
    total_population: 'population_2017',
    household_income: 'household_income_2017',
    median_home_value: 'median_home_value_2017',
    median_income_value: 'median_income_value_2017',
    'NAME': 'place_name'
})

print('Requesting 2010 year data for place')
place_raw_data = census_client.acs5.state_place(
    ('NAME', total_population, household_income, median_home_value),
    state.fips, Census.ALL, year=2010
)

data_2010 = pd.DataFrame.from_records(place_raw_data)
data_2010 = data_2010.rename(columns={
    total_population: 'population_2010',
    household_income: 'household_income_2010',
    median_home_value: 'median_home_value_2010'
})


data = pd.merge(left=data_2010, right=data_2017, on=['state', 'place'])
data['population_growth_pct'] = ((data['population_2017'].astype(float) /
                                  data['population_2010'].astype(float)) - 1) * 100
data['household_income_growth_pct'] = ((data['household_income_2017'].astype(float) /
                                        data['household_income_2010'].astype(float)) - 1) * 100
data['median_home_value_growth_pct'] = ((data['median_home_value_2017'].astype(float) /
                                         data['median_home_value_2010'].astype(float)) - 1) * 100
data = data[['place_name', 'place', 'population_growth_pct', 'household_income_growth_pct',
             'median_home_value_growth_pct', 'median_income_value_2017', 'unemployment_rate_pct_2017',
             'white_race_pct', 'black_race_pct', 'asian_race_pct', 'hispanic_race_pct', 'other_race_pct']]


def download_shapefile(url, bbox=None):
    with tempfile.TemporaryDirectory() as tmp_dir:
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        archive_path = os.path.join(tmp_dir, 'archive.zip')
        with open(archive_path, 'wb') as f:
            for data_chunk in tqdm(response.iter_content(block_size),
                                   total=math.ceil(total_size//block_size),
                                   unit='KB', unit_scale=True):
                f.write(data_chunk)
        boundaries = gpd.read_file(f'zip:{archive_path}', bbox=bbox)
        return boundaries


print('Downloading boundaries file')
# there is no shape files for places in us library, so hardcode the url
place_boundaries = download_shapefile(
    f'https://www2.census.gov/geo/tiger/GENZ2017/shp/cb_2017_{state.fips}_place_500k.zip'
)
place_boundaries = place_boundaries[['geometry', 'PLACEFP']]
result = pd.merge(place_boundaries, data, left_on=['PLACEFP'], right_on=['place'])
print('Saving to GeoJSON')

upcast_dispatch = {geometry.Point: geometry.MultiPoint,
                   geometry.LineString: geometry.MultiLineString,
                   geometry.Polygon: geometry.MultiPolygon}


# needed to prevent from error
def maybe_cast_to_multigeometry(geom):
    caster = upcast_dispatch.get(type(geom), lambda x: x[0])
    return caster([geom])


result.geometry = result.geometry.apply(maybe_cast_to_multigeometry)
# this one generates a warning, need to update geopandas
result.to_file('census_place_data.geojson', driver="GeoJSON")

print('Saving to TopoJSON')
tj_data = topojson.topology(result)
with open('census_place_data.topojson', 'w') as fp:
    json.dump(tj_data, fp)
