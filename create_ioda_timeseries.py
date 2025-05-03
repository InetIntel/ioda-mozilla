import datetime

import pandas as pd
from google.cloud import bigquery

from constants import GCP_PROJECT_ID, NE_MAP_PATH, MOZILLA_BQ_TABLE_NAME

DEFAULT_LOOKBACK_PERIOD = 2  # in days


def get_mozilla_data(country=None, region=None, end_time=datetime.datetime.now(), start_time=None):
    ne_mapping = pd.read_csv(NE_MAP_PATH)

    if not isinstance(end_time, datetime.datetime):
        # if end_time is passed in as unix timestamp
        end_time = datetime.datetime.fromtimestamp(end_time)
    if start_time:
        start_time = datetime.datetime.fromtimestamp(start_time)
    else:
        start_time = end_time - datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)

    end_time = end_time.astimezone(datetime.timezone.utc)
    start_time = start_time.astimezone(datetime.timezone.utc)

    end_time_fmt = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_time_fmt = start_time.strftime("%Y-%m-%d %H:%M:%S")

    if region in ne_mapping.ioda_id:
        country = ne_mapping.loc[region == ne_mapping.ioda_id, "country"].unique()[0]
    if country:
        query = f"""
            SELECT *,
                CASE 
                    WHEN city = 'unknown' 
                        AND (geo_subdivision1 IS NOT NULL AND geo_subdivision1 != '')
                    THEN CONCAT(
                        'unknown (',
                        geo_subdivision1,
                        IF(geo_subdivision2 IS NOT NULL AND geo_subdivision2 != '', CONCAT(', ', geo_subdivision2), ''),
                        ')'
                    )
                    WHEN city = 'unknown' 
                        AND (geo_subdivision2 IS NOT NULL AND geo_subdivision2 != '')
                    THEN CONCAT(
                        'unknown (',
                        geo_subdivision2,
                        ')'
                    )
                    ELSE city
                END AS adjusted_city
            FROM {MOZILLA_BQ_TABLE_NAME}
            WHERE country = '{country}' 
            AND datetime BETWEEN TIMESTAMP('{start_time_fmt}')
                            AND TIMESTAMP('{end_time_fmt}')
        """
    else:
        # no country name or region specified, return data from all countries and region for the specified timeframe.
        query = f"""
            SELECT *,
                CASE 
                    WHEN city = 'unknown' 
                        AND (geo_subdivision1 IS NOT NULL AND geo_subdivision1 != '')
                    THEN CONCAT(
                        'unknown (',
                        geo_subdivision1,
                        IF(geo_subdivision2 IS NOT NULL AND geo_subdivision2 != '', CONCAT(', ', geo_subdivision2), ''),
                        ')'
                    )
                    WHEN city = 'unknown' 
                        AND (geo_subdivision2 IS NOT NULL AND geo_subdivision2 != '')
                    THEN CONCAT(
                        'unknown (',
                        geo_subdivision2,
                        ')'
                    )
                    ELSE city
                END AS adjusted_city
            FROM {MOZILLA_BQ_TABLE_NAME}
            WHERE datetime BETWEEN TIMESTAMP('{start_time_fmt}')
                            AND TIMESTAMP('{end_time_fmt}')
        """

    print(query)

    mozilla_df = None

    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        job = client.query(query)
        mozilla_df = job.to_dataframe()
    except Exception as e:
        print(e)

    mozilla_with_ioda_id_df = mozilla_df.merge(ne_mapping,
                                               on=['country', 'geo_subdivision1', 'geo_subdivision2', 'city'])

    if len(mozilla_df) != len(mozilla_with_ioda_id_df):
        merge_on_mozilla = mozilla_df.merge(ne_mapping,
                                  on=['country', 'geo_subdivision1', 'geo_subdivision2', 'city'],
                                  how='left',
                                  indicator=True)

        unmatched = merge_on_mozilla[merge_on_mozilla['_merge'] == 'left_only']

        # unmatched.to_csv('./data/unmatched.csv')

    mozilla_with_ioda_id_df.to_csv('./data/merged.csv')

    # assert len(mozilla_df) == len(mozilla_with_ioda_id_df), \
    #     (f"Length mismatch: Original Mozilla DataFrame has {len(mozilla_df)} rows, "
    #      f"DataFrame of Mozilla data merged with IODA ids has {len(mozilla_with_ioda_id_df)} rows")

    region_agg_df = mozilla_with_ioda_id_df

    if region:
        region_agg_df = region_agg_df[mozilla_with_ioda_id_df["ioda_id"] == region]

    region_agg_df = region_agg_df.groupby(["ioda_id", "datetime"]).agg({
        "country": "first",
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "adjusted_city": lambda city: list(set(city))
    }).reset_index()

    print(f'Total no. of region-aggregated datapoints: {len(region_agg_df)}')
    print(f'IODA regions present in data: {region_agg_df.ioda_id.unique()}')

    country_agg_df = mozilla_with_ioda_id_df.groupby(["datetime", "country"]).agg({
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "adjusted_city": lambda city: list(set(city)),
    }).reset_index()
    print(f'Total no. of country-aggregated datapoints: {len(country_agg_df)}')
    print(f'Countries present in data: {country_agg_df.country.unique()}')

    # unpack lists and transform into string data for columns with list data
    # in addition, count number of cities in aggregated datapoint
    city_col_debugging = ['adjusted_city']
    region_agg_df = transform_list_data_and_add_city_count(city_col_debugging, region_agg_df)
    country_agg_df = transform_list_data_and_add_city_count(city_col_debugging, country_agg_df)

    region_agg_df.to_csv('./data/test_region.csv')
    country_agg_df.to_csv('./data/test_country.csv')

    return region_agg_df, country_agg_df


def transform_list_data_and_add_city_count(cols, df):
    df['city_count'] = df['adjusted_city'].apply(lambda city_list: len(city_list))
    for col in cols:
        df[col] = df[col].apply(lambda col_data: ", ".join(map(str, col_data)))
    return df


if __name__ == "__main__":
    # get_mozilla_data(country='NL', region=4416)
    # get_mozilla_data(region=4416)
    # get_mozilla_data(country='NL')
    # get_mozilla_data()
    pass