import datetime

import pandas as pd
from google.cloud import bigquery

from constants import GCP_PROJECT_ID, NE_MAP_PATH

LOOKBACK_PERIOD = 1  # in days


def get_mozilla_data(country_name, end_time=datetime.datetime.now(), start_time=None):
    if not isinstance(end_time, datetime.datetime):
        # if end_time is passed in as unix timestamp
        end_time = datetime.datetime.fromtimestamp(end_time)
    if start_time:
        start_time = datetime.datetime.fromtimestamp(start_time)
    else:
        start_time = end_time - datetime.timedelta(days=LOOKBACK_PERIOD)

    end_time = end_time.astimezone(datetime.timezone.utc)
    start_time = start_time.astimezone(datetime.timezone.utc)

    end_time_fmt = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_time_fmt = start_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
      SELECT *
      FROM `moz-fx-data-shared-prod.internet_outages.global_outages_v2`
      WHERE country = '{country_name}'
      AND datetime BETWEEN TIMESTAMP('{start_time_fmt}')
                       AND TIMESTAMP('{end_time_fmt}')
      """

    mozilla_df = None
    ne_mapping = pd.read_csv(NE_MAP_PATH)

    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        job = client.query(query)
        mozilla_df = job.to_dataframe()
    except Exception as e:
        print(e)

    mozilla_with_ioda_id_df = mozilla_df.merge(ne_mapping,
                                               on=['country', 'geo_subdivision1', 'geo_subdivision2', 'city'])

    assert len(mozilla_df) == len(mozilla_with_ioda_id_df), \
        (f"Length mismatch: Original Mozilla DataFrame has {len(mozilla_df)} rows, "
         f"DataFrame of Mozilla data merged with IODA ids has {len(mozilla_with_ioda_id_df)} rows")

    region_agg_df = mozilla_with_ioda_id_df.groupby(["ioda_id", "datetime"]).agg({
        "country": lambda country: list(set(country)),
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "city": lambda city: list(set(city))
    }).reset_index()

    print(f'Total no. of region-aggregated datapoints in country {country_name}: {len(region_agg_df)}')
    print(f'IODA regions present in data: {region_agg_df.ioda_id.unique()}')

    country_agg_df = mozilla_with_ioda_id_df.groupby(["datetime"]).agg({
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "city": lambda city: list(set(city)),
        "geo_subdivision1": lambda subdiv1: list(set(subdiv1)),
        "ioda_id": lambda id: list(set(id)),
        "country": lambda country: list(set(country)),
    }).reset_index()

    # unpack lists and transform into string data for columns with list data
    # in addition, count number of cities in aggregated datapoint
    region_df_string_cols = ['country', 'city']
    country_df_string_cols = ['country', 'city', 'ioda_id', 'geo_subdivision1']
    region_agg_df = transform_list_data_and_add_city_count(region_df_string_cols, region_agg_df)
    country_agg_df = transform_list_data_and_add_city_count(country_df_string_cols, country_agg_df)

    mozilla_with_ioda_id_df.to_csv('./data/merged.csv')
    region_agg_df.to_csv('./data/test_region.csv')
    country_agg_df.to_csv('./data/test_country.csv')

    return region_agg_df, country_agg_df


def transform_list_data_and_add_city_count(cols, df):
    df['city_count'] = df['city'].apply(lambda city_list: len(city_list))
    for col in cols:
        df[col] = df[col].apply(lambda col_data: ", ".join(map(str, col_data)))
    return df


if __name__ == "__main__":
    get_mozilla_data()
