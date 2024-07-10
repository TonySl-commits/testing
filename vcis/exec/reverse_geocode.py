import reverse_geocoder as rg
import pandas as pd
from vcis.utils.utils import CDR_Properties

properties = CDR_Properties()

def reverseGeocode(coordinates):
    result = rg.search(coordinates,mode=2)
    return result

def add_reverseGeocode_columns():
    df = pd.read_csv(properties.passed_filepath_temp_geocode)
    location_latitdue_list = list(df['location_latitude'].values)
    location_longitude_list = list(df['location_longitude'].values)
    choords = list(zip(location_latitdue_list, location_longitude_list))    

    reverse_geo_jason = reverseGeocode(choords)
    provinance_list = [d['admin1'] for d in reverse_geo_jason]
    country_list = [d['cc'] for d in reverse_geo_jason]
    city_list = [d['name'] for d in reverse_geo_jason]
    lat_long = [[d['lat'],d['lon']] for d in reverse_geo_jason]

    df['provinance'] = provinance_list
    df['country'] = country_list
    df['city'] = city_list
    df['lat_long'] = lat_long

    return df

if __name__=="__main__":
    df = pd.read_csv(properties.passed_filepath_temp_geocode)
    df = add_reverseGeocode_columns()
    df.to_csv(properties.passed_filepath_temp_geocode)
