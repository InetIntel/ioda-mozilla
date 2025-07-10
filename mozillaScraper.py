import logging, datetime, time, argparse

import pandas as pd
import requests
# import pytimeseries

from google.cloud import bigquery

from constants import GCP_PROJECT_ID, NE_MAP_PATH, DEFAULT_LOOKBACK_PERIOD, CONTINENT_MAP, BASEKEY, MOZILLA_TABLE_NAME, \
    IODA_API_COUNTRY_ENTITY_QUERY

NE_MAPPING = pd.read_csv(NE_MAP_PATH)

# besides returning code, also return time, or take in dict & update
# calculate total time taken outside function
def fetchData(projectid, starttime, endtime, country_code, saved):
    """
     Parameters:
          mozilla_table_name -- the table name to be queried that contains the Mozilla telemetry data
          start_time -- the start of the time period to query for (as a
                        Datetime object)
          end_time -- the end of the time period to query for (as a
                        Datetime object)
          country_code -- the ISO 2-letter country code for the country to query
                    for
          saved -- the dictionary to save the fetched data into
    """
    # IODA uses a "continent.country" format to hierarchically structure
    # geographic time series so we need to add the appropriate continent
    # for our requested country to the time series label.
    if country_code not in CONTINENT_MAP:
        logging.error("No continent mapping for %s." % (country_code))
        contcode = "??"
    else:
        contcode = CONTINENT_MAP[country_code]

    start_fetch = time.time()
    client = bigquery.Client(project=projectid)

    if country_code:
        try:
            check_country_exists_mozilla(country_code)
            query = get_query_string(starttime, endtime, country_code)
        except Exception as e:
            logging.error("Country %s not found in Mozilla Telemetry data." % (country_code))
            return -1
    else:
        # no country specified, obtain data for all countries.
        query = get_query_string(starttime, endtime)

    try:
        job = client.query(query)
        result_df = job.to_dataframe()
    except bigquery.exceptions.BigQueryError as e:
        logging.error("Failed to get telemetry data for country %s from %s to %s: %s", country_code, str(starttime), str(endtime), str(e))
        return -1
    except Exception as e:
        logging.error("An unexpected error occurred from %s to %s: %s", str(starttime), str(endtime), str(e))
        return -1

    time.sleep(0.1)
    if result_df.empty:
        logging.error("No telemetry data for country %s from %s to %s.", country_code, str(starttime), str(endtime))
        return 0

    fetched_country, fetched_region = process_mozilla_df(result_df)
    print(f'Time taken to fetch and process data: {time.time() - start_fetch}s')

    start_process_for_kafka = time.time()
    # pytimeseries works best if we write all datapoints for a given timestamp
    # in a single batch, so we will save our fetched data into a dictionary
    # keyed by timestamp. Once we've fetched everything, then we can walk
    # through that dictionary to emit the data in timestamp order.
    for k, all_metrics in fetched_country.items():
        for country_code, metric_data in all_metrics.items():
            if country_code not in CONTINENT_MAP:
                logging.error("No continent mapping for %s." % (country_code))
                contcode = "??"
            else:
                contcode = CONTINENT_MAP[country_code]
            ts = int(k.timestamp())

            if ts not in saved:
                saved[ts] = []

            for metric, metric_value in metric_data.items():
            # This is the key that we're going to write into kafka for this
            # country + product. They key must be encoded because pytimeseries
            # expects a bytes object for the key, not a string.
                key = "%s.%s.%s.%s.%s" % (BASEKEY, contcode, 'country', country_code, metric)
                key = key.encode()

                # The traffic data is stored as a normalised float (with 10 d.p. of
                # precision -- we'd rather deal with integers so scale it up
                # 26 Jun - maybe do not multiply city count with factor.
                if metric != 'city_count':
                    saved[ts].append((key, int(10000000000 * metric_value)))
                else:
                    saved[ts].append((key, int(metric_value)))

    for timestamp, region_data in fetched_region.items():
        for ioda_id, all_metrics in region_data.items():
            ts = int(timestamp.timestamp())

            if ts not in saved:
                saved[ts] = []

            for metric, metric_value in all_metrics.items():
                key = "%s.%s.%s.%s.%s" % (BASEKEY, contcode, 'region', ioda_id, metric)
                key = key.encode()

                # 26 Jun - maybe do not multiply city count with factor.
                if metric != 'city_count':
                    saved[ts].append((key, int(10000000000 * metric_value)))
                else:
                    saved[ts].append((key, int(metric_value)))
    print(f'Time taken to process data for kafka: {time.time() - start_process_for_kafka}s')
    return 1


def get_query_string(start_time, end_time, country_code=None):
    ioda_countries = ""
    response = requests.get(IODA_API_COUNTRY_ENTITY_QUERY)
    if response.status_code == 200:
        data = response.json()['data']
        country_codes = [dictionary['code'] for dictionary in data]
        ioda_countries = ", ".join(f'"{country}"' for country in country_codes)
    print(ioda_countries)

    unknown_city_case = """
        CASE 
            WHEN city = 'unknown' AND (geo_subdivision1 IS NOT NULL AND geo_subdivision1 != '')
            THEN CONCAT(
                'unknown (',
                geo_subdivision1,
                IF(geo_subdivision2 IS NOT NULL AND geo_subdivision2 != '', CONCAT(', ', geo_subdivision2), ''),
                ')'
            )
            WHEN city = 'unknown' AND (geo_subdivision2 IS NOT NULL AND geo_subdivision2 != '')
            THEN CONCAT('unknown (', geo_subdivision2, ')')
            ELSE city
        END AS adjusted_city
    """

    base_query = f"""
    SELECT *,
           {unknown_city_case}
    FROM {MOZILLA_TABLE_NAME}
    WHERE datetime BETWEEN TIMESTAMP('{start_time}') AND TIMESTAMP('{end_time}')
    AND country in ({ioda_countries})
    """

    # if country_code:
    #     return base_query + f"\nAND country = '{country_code}'"
    return base_query


def check_country_exists_mozilla(country_code):
    if not (NE_MAPPING['country'].isin([country_code]).any()):
        raise ValueError(f"Country {country_code} is not found in the Mozilla data.")


def process_mozilla_df(mozilla_df):
    country_agg_df = mozilla_df.groupby(["datetime", "country"]).agg({
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "adjusted_city": lambda city: list(set(city)),
    }).reset_index()

    # for counting number of cities and showing list of cities only,
    # List of cities will be dropped in eventual time series.
    city_col_debugging = ['adjusted_city']
    print(country_agg_df)
    # country_agg_df = (transform_list_data_and_add_city_count(city_col_debugging, country_agg_df)
    #                   .set_index('datetime').drop(['country', 'adjusted_city'], axis=1))
    country_agg_df = (transform_list_data_and_add_city_count(city_col_debugging, country_agg_df)
                      .set_index(['datetime', 'country']).drop(['adjusted_city'], axis=1))

    country_batches = {timestamp: data.droplevel('datetime') for timestamp, data in country_agg_df.groupby('datetime')}

    country_agg_dict = {timestamp: timestamp_agg_df.to_dict(orient="index")
                       for timestamp, timestamp_agg_df in country_batches.items()}
    # country_agg_dict = country_agg_df.to_dict(orient="index")

    # region-aggregated data is trickier, we need to map and aggregate the data according to region code
    # convert ioda_ids to ints. if not available, convert to NaN
    NE_MAPPING.ioda_id = pd.to_numeric(NE_MAPPING.ioda_id, errors='coerce').astype('Int64')
    mozilla_with_ioda_id_df = mozilla_df.merge(NE_MAPPING,
                                               on=['country', 'geo_subdivision1', 'geo_subdivision2'])

    region_agg_df = mozilla_with_ioda_id_df.groupby(["datetime", "ioda_id"]).agg({
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "adjusted_city": lambda city: list(set(city))
    }).reset_index()

    region_agg_df = (transform_list_data_and_add_city_count(city_col_debugging, region_agg_df)
                     .set_index(['datetime', 'ioda_id']).drop(['adjusted_city'], axis=1))

    # batch according to timestamp, and store region data as values
    region_batches = {timestamp: data.droplevel('datetime') for timestamp, data in region_agg_df.groupby('datetime')}

    region_agg_dict = {timestamp: timestamp_agg_df.to_dict(orient="index")
                       for timestamp, timestamp_agg_df in region_batches.items()}
    return (country_agg_dict, region_agg_dict)


def transform_list_data_and_add_city_count(cols, df):
    df['city_count'] = df['adjusted_city'].apply(lambda city_list: len(city_list))
    for col in cols:
        df[col] = df[col].apply(lambda col_data: ", ".join(map(str, col_data)))
    return df


def main(args):
    datadict = {}

    # Boiler-plate libtimeseries setup for a kafka output
    pyts = _pytimeseries.Timeseries()
    be = pyts.get_backend_by_name('kafka')
    if not be:
        logging.error('Unable to find pytimeseries kafka backend')
        return -1
    if not pyts.enable_backend(be, "-b %s -c %s -f ascii -p %s" % ( \
            args.broker, args.channel, args.topicprefix)):
        logging.error('Unable to initialise pytimeseries kafka backend')
        return -1

    kp = pyts.new_keypackage(reset=False, disable=True)
    # Boiler-plate ends

    # Determine the start and end time periods for our upcoming query
    if args.endtime:
        endtime = datetime.datetime.fromtimestamp(args.endtime)
    else:
        endtime = datetime.datetime.now()

    if args.starttime:
        starttime = datetime.datetime.fromtimestamp(args.starttime)
    else:
        starttime = endtime - datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)

    # Due to a bug in the netanalysis API, we must fetch at least one
    # days worth of data -- otherwise we will generate a 400 Bad Request.
    if (starttime > endtime or \
            endtime - starttime < datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)):
        starttime = endtime - datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)

    response = requests.get(IODA_API_COUNTRY_ENTITY_QUERY)

    if response.status_code == 200:
        data = response.json()['data']
        country_codes = [dictionary['code'] for dictionary in data]
        for country in country_codes:
            ret = fetchData(args.projectid, starttime, endtime, country, datadict)
    else:
        print(f"IODA API Query Request to obtain all countries failed with status code {response.status_code}")

    for ts, dat in sorted(datadict.items()):
        # If our fetched time range was expanded out to a full day, now
        # is a good time for us to ignore any time periods that the user
        # didn't explicitly ask for
        if args.starttime and ts < args.starttime:
            continue

        # pytimeseries code to save each key and value for this timestamp
        for val in dat:
            idx = kp.get_key(val[0])
            if idx is None:
                idx = kp.add_key(val[0])
            else:
                kp.enable_key(idx)
            kp.set(idx, val[1])

        # Write to the kafka queue
        kp.flush(ts)
    return


def test_all_countries(args):
    start_all = time.time()
    datadict = {}

    if args.endtime:
        endtime = datetime.datetime.fromtimestamp(args.endtime)
    else:
        endtime = datetime.datetime.now()

    if args.starttime:
        starttime = datetime.datetime.fromtimestamp(args.starttime)
    else:
        starttime = endtime - datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)

    # Due to a bug in the netanalysis API, we must fetch at least one
    # days worth of data -- otherwise we will generate a 400 Bad Request.
    if (starttime > endtime or \
            endtime - starttime < datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)):
        starttime = endtime - datetime.timedelta(days=DEFAULT_LOOKBACK_PERIOD)

    fetchData(args.projectid, starttime, endtime, None, saved=datadict)
    # response = requests.get(IODA_API_COUNTRY_ENTITY_QUERY)

    # if response.status_code == 200:
    #     data = response.json()['data']
    #     country_codes = [dictionary['code'] for dictionary in data]
    #     for country in country_codes:
    #         ret = fetchData(args.projectid, starttime, endtime, country, datadict)
    # else:
    #     print(f"IODA API Query Request to obtain all countries failed with status code {response.status_code}")

    print(datadict)
    print(f'Time taken to fetch & save data for all countries: {time.time() - start_all}s')
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Continually fetches Mozilla telemetry data from Google Bigquery and writes it into kafka')
    #
    # parser.add_argument("--broker", type=str, required=True, help="The kafka broker to connect to")
    # parser.add_argument("--channel", type=str, required=True, help="Kafka channel to write the data into")
    # parser.add_argument("--topicprefix", type=str, required=True, help="Topic prefix to prepend to each Kafka message")
    parser.add_argument("--projectid", type=str, required=True, help="The Google Cloud project ID")
    parser.add_argument("--starttime", type=int, help="Fetch traffic data starting from the given Unix timestamp. \
                                                                    If not provided, defaults to 2 days before endtime.")
    parser.add_argument("--endtime", type=int, help="Fetch traffic data up until the given Unix timestamp. \
                                                                  If not provided, defaults to the current time.")
    args = parser.parse_args()

    test_all_countries(args)
    # main(args)

    pass
