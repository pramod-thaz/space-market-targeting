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

total_population = 'B00001_001E'
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


tract_raw_data_2017 = census_client.acs5.state_county(
    ('NAME', total_population, household_income, median_home_value, median_income_value,
     male_below_poverty, unemployed_male_below_poverty, female_below_poverty, unemployed_female_below_poverty,
     male_above_poverty, unemployed_male_above_poverty, female_above_poverty, unemployed_female_above_poverty),
    state.fips, Census.ALL, year=2017
)
data_2017 = pd.DataFrame.from_records(tract_raw_data_2017)
data_2017 = data_2017.rename(columns={
    total_population: 'population_2017',
    household_income: 'household_income_2017',
    median_home_value: 'median_home_value_2017',
    median_income_value: 'median_income_value_2017'
})
data_2017['unemployment_rate_pct_2017'] = (
    (data_2017[unemployed_male_below_poverty] + data_2017[unemployed_male_above_poverty] +
     data_2017[unemployed_female_below_poverty] + data_2017[unemployed_female_above_poverty]) /
    (data_2017[male_below_poverty] + data_2017[male_above_poverty] +
     data_2017[female_below_poverty] + data_2017[female_above_poverty])
) * 100

tract_raw_data_2010 = census_client.acs5.state_county(
    ('NAME', total_population, household_income, median_home_value),
    state.fips, Census.ALL, year=2010
)
data_2010 = pd.DataFrame.from_records(tract_raw_data_2010)
data_2010 = data_2010.rename(columns={
    total_population: 'population_2010',
    household_income: 'household_income_2010',
    median_home_value: 'median_home_value_2010'
})


data = pd.merge(left=data_2010, right=data_2017, on=['NAME', 'county', 'state'])
data['population_growth_pct'] = ((data['population_2017'].astype(float) /
                                  data['population_2010'].astype(float)) - 1) * 100
data['household_income_growth_pct'] = ((data['household_income_2017'].astype(float) /
                                        data['household_income_2010'].astype(float)) - 1) * 100
data['median_home_value_growth_pct'] = ((data['median_home_value_2017'].astype(float) /
                                         data['median_home_value_2010'].astype(float)) - 1) * 100
data = data[['NAME', 'county', 'population_growth_pct', 'household_income_growth_pct',
             'median_home_value_growth_pct', 'median_income_value_2017', 'unemployment_rate_pct_2017']]


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


county_boundaries = download_shapefile(state.shapefile_urls()['county'])
county_boundaries = county_boundaries[['geometry', 'COUNTYFP10']]
result = pd.merge(county_boundaries, data, left_on=['COUNTYFP10'], right_on=['county'])
# this one generates a warning, need to update geopandas
result.to_file('census_county_data.geojson', driver="GeoJSON")

tj_data = topojson.topology(result)
with open('census_county_data.topojson', 'w') as fp:
    json.dump(tj_data, fp)
