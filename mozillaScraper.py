#!/usr/bin/env python3

import logging, datetime, time, argparse, requests
import os

import pandas as pd
import _pytimeseries
from google.cloud import bigquery

from constants import NE_MAP_PATH, DEFAULT_LOOKBACK_PERIOD, CONTINENT_COUNTRY_MAP, BASEKEY, MOZILLA_TABLE_NAME, \
    IODA_API_COUNTRY_ENTITY_QUERY

NE_MAPPING = pd.read_csv(NE_MAP_PATH)
# region-aggregated data is trickier, we need to map and aggregate the data according to region code
# convert ioda_ids to ints. if not available, convert to NaN
NE_MAPPING['ioda_id'] = pd.to_numeric(NE_MAPPING['ioda_id'], errors='coerce').astype('Int64')
NE_MAPPING['continent'] = NE_MAPPING['country'].map(CONTINENT_COUNTRY_MAP)
CONTINENT_REGION_MAP = NE_MAPPING.dropna(subset=['ioda_id']).set_index('ioda_id')['continent'].to_dict()


def fetchData(projectid, starttime, endtime, country_code, datadict, debug, savedata):
    """
     Parameters:
          projectid -- the project ID for the project in Google Cloud Platform.
          start_time -- the start of the time period to query for (as a
                        Datetime object)
          end_time -- the end of the time period to query for (as a
                        Datetime object)
          country_code -- the ISO 2-letter country code for the country to query for
          saved -- the dictionary to save the fetched data into
    """
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
        country_code = "all"

    try:
        job = client.query(query)
        result_df = job.to_dataframe()
        if savedata:
            if not os.path.exists('data'):
                os.makedirs('data')
            save_filename = f'data/mozilla_data_{country_code}_start_{starttime}_end_{endtime}.csv'
            result_df.to_csv(save_filename)
            print("mozilla data saved in: ", save_filename)
            return 0
    except bigquery.exceptions.BigQueryError as e:
        logging.error("BigQueryError: Failed to get telemetry data from %s to %s: %s", str(starttime), str(endtime),
                      str(e))
        return -1
    except Exception as e:
        logging.error("An unexpected error occurred from %s to %s: %s", str(starttime), str(endtime), str(e))
        return -1

    time.sleep(0.1)
    if result_df.empty:
        logging.error("No telemetry data for from %s to %s.", str(starttime), str(endtime))
        return 0

    fetched_country, fetched_region = process_mozilla_df(result_df)

    # pytimeseries works best if we write all datapoints for a given timestamp
    # in a single batch, so we will save our fetched data into a dictionary
    # keyed by timestamp. Once we've fetched everything, then we can walk
    # through that dictionary to emit the data in timestamp order.
    # We iterate through two different sets of fetched data, one for country-aggregated data
    # and the other for region-aggregated data.
    for k, all_metrics in fetched_country.items():
        for country_code, metric_data in all_metrics.items():
            # IODA uses a "continent.country" format to hierarchically structure
            # geographic time series so we need to add the appropriate continent
            # for our requested country to the time series label.
            if country_code not in CONTINENT_COUNTRY_MAP:
                logging.error("No continent mapping for %s." % (country_code))
                contcode = "??"
            else:
                contcode = CONTINENT_COUNTRY_MAP[country_code]
            ts = int(k.timestamp())

            if ts not in datadict:
                datadict[ts] = []

            for metric, metric_value in metric_data.items():
                # This is the key that we're going to write into kafka for this
                # country + product. They key must be encoded because pytimeseries
                # expects a bytes object for the key, not a string.
                key = "%s.%s.%s.%s.%s" % (BASEKEY, contcode, 'country', country_code, metric)
                key = key.encode()

                # The traffic data is stored as a normalised float (with 10 d.p. of
                # precision -- we'd rather deal with integers so scale it up.
                # The only exception is the 'city_count' metric,
                # where we use the original value from the data.
                if metric != 'city_count':
                    datadict[ts].append((key, int(10000000000 * metric_value)))
                else:
                    datadict[ts].append((key, int(metric_value)))

    for timestamp, region_data in fetched_region.items():
        for ioda_id, all_metrics in region_data.items():
            ts = int(timestamp.timestamp())

            if ioda_id not in NE_MAPPING['ioda_id'].values:
                logging.error("No continent mapping for region %s." % (ioda_id))
                contcode = "??"
            else:
                contcode = CONTINENT_REGION_MAP[ioda_id]

            if ts not in datadict:
                datadict[ts] = []

            for metric, metric_value in all_metrics.items():
                key = "%s.%s.%s.%s.%s" % (BASEKEY, contcode, 'region', ioda_id, metric)
                key = key.encode()

                if metric != 'city_count':
                    datadict[ts].append((key, int(10000000000 * metric_value)))
                else:
                    datadict[ts].append((key, int(metric_value)))
    if debug:
        print("data saved in datadict: ", datadict)
    return 1


def get_query_string(start_time, end_time, country_code=None):
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
    """

    if country_code:
        return base_query + f"\nAND country = '{country_code}'"

    # query for all countries obtained from the IODA API call
    response = requests.get(IODA_API_COUNTRY_ENTITY_QUERY)
    if response.status_code == 200:
        data = response.json()['data']
        country_codes = [dictionary['code'] for dictionary in data]
        ioda_countries = ", ".join(f'"{country}"' for country in country_codes)
    else:
        logging.error(f"IODA API Query Request to obtain all countries failed with status code {response.status_code}.")
        return ""
    return base_query + f"\nAND country in ({ioda_countries})"


def check_country_exists_mozilla(country_code):
    if not (NE_MAPPING['country'].isin([country_code]).any()):
        raise ValueError(f"Country {country_code} is not found in the Mozilla data.")


def process_mozilla_df(mozilla_df):
    country_agg_df = mozilla_df.groupby(["datetime", "country"]).agg({
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "adjusted_city": lambda city: list(set(city)),
    }).reset_index()

    # For counting number of cities and showing list of cities only, which can be used for debugging.
    # The list of cities will be dropped in the returned data.
    city_col_debugging = ['adjusted_city']
    country_agg_df = (transform_list_data_and_add_city_count(city_col_debugging, country_agg_df)
                      .set_index(['datetime', 'country']).drop(['adjusted_city'], axis=1))

    country_batches = {timestamp: data.droplevel('datetime') for timestamp, data in country_agg_df.groupby('datetime')}

    country_agg_dict = {timestamp: timestamp_agg_df.to_dict(orient="index")
                       for timestamp, timestamp_agg_df in country_batches.items()}

    # region-aggregated data is trickier, we need to map and aggregate the data according to region code
    # convert ioda_ids to ints. if not available, convert to NaN
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
    return country_agg_dict, region_agg_dict


def transform_list_data_and_add_city_count(cols, df):
    df['city_count'] = df['adjusted_city'].apply(lambda city_list: len(city_list))
    for col in cols:
        df[col] = df[col].apply(lambda col_data: ", ".join(map(str, col_data)))
    return df


def main(args):
    datadict = {}

    if not args.debug:
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

    ret = fetchData(args.projectid, starttime, endtime, None, datadict, args.debug, args.savedata)

    if not args.debug:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Continually fetches Mozilla telemetry data from Google Bigquery and writes it into kafka')

    parser.add_argument("--broker", type=str, required=True, help="The kafka broker to connect to")
    parser.add_argument("--channel", type=str, required=True, help="Kafka channel to write the data into")
    parser.add_argument("--topicprefix", type=str, required=True, help="Topic prefix to prepend to each Kafka message")
    parser.add_argument("--projectid", type=str, required=True, help="The Google Cloud project ID")
    parser.add_argument("--starttime", type=int, help="Fetch traffic data starting from the given Unix timestamp. \
                                                                    If not provided, defaults to 2 days before endtime.")
    parser.add_argument("--endtime", type=int, help="Fetch traffic data up until the given Unix timestamp. \
                                                                  If not provided, defaults to the current time.")
    parser.add_argument("--debug", type=str, help="Enables debug mode for printing data.")
    parser.add_argument("--savedata", type=str, help="Enables save mode for saving fetched Mozilla data with all metrics.")

    args = parser.parse_args()

    main(args)