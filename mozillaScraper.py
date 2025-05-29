import logging, datetime, time, argparse
# import pytimeseries

from google.cloud import bigquery

# from constants import GCP_PROJECT_ID, NE_MAP_PATH

MOZILLA_TABLE_NAME = "moz-fx-data-shared-prod.internet_outages.global_outages_v2"
DEFAULT_LOOKBACK_PERIOD = 2  # in days
CONTINENT_MAP = {
    "AD": "EU", "AE": "AS", "AF": "AS", "AG": "NA", "AI": "NA",
    "AL": "EU", "AM": "AS", "AO": "AF", "AQ": "AN", "AR": "SA",
    "AT": "EU", "AU": "OC", "AW": "NA", "AX": "EU", "AZ": "AS",
    "BA": "EU", "BB": "NA", "BD": "AS", "BE": "EU", "BF": "AF",
    "BG": "EU", "BH": "AS", "BI": "AF", "BJ": "AF", "BM": "NA",
    "BN": "AS", "BO": "SA", "BR": "SA", "BS": "NA", "BT": "AS",
    "BW": "AF", "BY": "EU", "BZ": "NA", "CA": "NA", "CD": "AF",
    "CF": "AF", "CG": "AF", "CH": "EU", "CI": "AF", "CK": "OC",
    "CL": "SA", "CM": "AF", "CN": "AS", "CO": "SA", "CR": "NA",
    "CU": "NA", "CV": "AF", "CY": "EU", "CZ": "EU", "DE": "EU",
    "DJ": "AF", "DK": "EU", "DM": "NA", "DO": "NA", "DZ": "AF",
    "EC": "SA", "EE": "EU", "EG": "AF", "EH": "AF", "ER": "AF",
    "ES": "EU", "ET": "AF", "FI": "EU", "FJ": "OC", "FM": "OC",
    "FO": "EU", "FR": "EU", "GA": "AF", "GB": "EU", "GD": "NA",
    "GE": "EU", "GF": "SA", "GG": "EU", "GH": "AF", "GI": "EU",
    "GL": "NA", "GM": "AF", "GN": "AF", "GP": "NA", "GQ": "AF",
    "GR": "EU", "GT": "NA", "GU": "OC", "GW": "AF", "GY": "SA",
    "HK": "AS", "HN": "NA", "HR": "EU", "HT": "NA", "HU": "EU",
    "ID": "AS", "IE": "EU", "IL": "AS", "IM": "EU", "IN": "AS",
    "IQ": "AS", "IR": "AS", "IS": "EU", "IT": "EU", "JE": "EU",
    "JM": "NA", "JO": "AS", "JP": "AS", "KE": "AF", "KG": "AS",
    "KH": "AS", "KI": "OC", "KM": "AF", "KN": "NA", "KW": "AS",
    "KY": "NA", "KZ": "AS", "LA": "AS", "LB": "AS", "LC": "NA",
    "LI": "EU", "LK": "AS", "LR": "AF", "LS": "AF", "LT": "EU",
    "LU": "EU", "LV": "EU", "LY": "AF", "MA": "AF", "MC": "EU",
    "MD": "EU", "ME": "EU", "MG": "AF", "MH": "OC", "MK": "EU",
    "ML": "AF", "MM": "AS", "MN": "AS", "MO": "AS", "MP": "OC",
    "MQ": "NA", "MR": "AF", "MS": "NA", "MT": "EU", "MU": "AF",
    "MV": "AS", "MW": "AF", "MX": "NA", "MY": "AS", "MZ": "AF",
    "NA": "AF", "NC": "OC", "NE": "AF", "NF": "OC", "NG": "AF",
    "NI": "NA", "NL": "EU", "NO": "EU", "NP": "AS", "NR": "OC",
    "NU": "OC", "NZ": "OC", "OM": "AS", "PA": "NA", "PE": "SA",
    "PF": "OC", "PG": "OC", "PH": "AS", "PK": "AS", "PL": "EU",
    "PM": "NA", "PN": "OC", "PR": "NA", "PS": "AS", "PT": "EU",
    "PW": "OC", "PY": "SA", "QA": "AS", "RE": "AF", "KR": "AS",
    "KP": "AS", "VG": "NA", "SH": "AF", "RO": "EU", "RS": "EU",
    "RU": "EU", "RW": "AF", "SA": "AS", "SB": "OC", "SC": "AF",
    "SD": "AF", "SE": "EU", "SG": "AS", "SI": "EU", "SK": "EU",
    "SL": "AF", "SM": "EU", "SN": "AF", "SO": "AF", "SR": "SA",
    "SS": "AF", "ST": "AF", "SV": "NA", "SY": "AS", "SZ": "AF",
    "TC": "NA", "TD": "AF", "TG": "AF", "TH": "AS", "TJ": "AS",
    "TK": "OC", "TL": "AS", "TM": "AS", "TN": "AF", "TO": "OC",
    "TR": "EU", "TT": "NA", "TV": "OC", "TW": "AS", "TZ": "AF",
    "UA": "EU", "UG": "AF", "US": "NA", "VI": "NA", "UY": "SA",
    "UZ": "AS", "VA": "EU", "VC": "NA", "VE": "SA", "VN": "AS",
    "VU": "OC", "WS": "OC", "YE": "AS", "YT": "AF", "ZA": "AF",
    "ZM": "AF", "ZW": "AF",
}
BASEKEY = "mozilla_tlm"


def fetchData(mozilla_table_name, projectid, starttime, endtime, region, saved):
    """
     Parameters:
          mozilla_table_name -- the table name to be queried that contains the Mozilla telemetry data
          start_time -- the start of the time period to query for (as a
                        Datetime object)
          end_time -- the end of the time period to query for (as a
                        Datetime object)
          region -- the ISO 2-letter country code for the region to query
                    for (?)
          saved -- the dictionary to save the fetched data into
    """
    # IODA uses a "continent.country" format to hierarchically structure
    # geographic time series so we need to add the appropriate continent
    # for our requested region to the time series label.
    if region not in CONTINENT_MAP:
        logging.error("No continent mapping for %s?" % (region))
        contcode = "??"
    else:
        contcode = CONTINENT_MAP[region]

    # This is the key that we're going to write into kafka for this
    # region + product. They key must be encoded because pytimeseries
    # expects a bytes object for the key, not a string.
    key = "%s.%s.%s.traffic" % (BASEKEY, contcode, region)
    key = key.encode()

    client = bigquery.Client(project=projectid)
    query = ""

    if region:
        query = get_query_string(starttime, endtime, region)

    try:
        job = client.query(query)
        result_df = job.to_dataframe()
    except bigquery.exceptions.GoogleCloudError as e:
        logging.error("Failed to get telemetry data from %s to %s: %s", str(starttime), str(endtime), str(e))
        return -1
    except Exception as e:
        logging.error("An unexpected error occurred from %s to %s: %s", str(starttime), str(endtime), str(e))
        return -1

    time.sleep(0.5)
    if result_df.empty:
        print("The telemetry data from %s to %s is None", str(starttime), str(endtime))
        return 0

    fetched = process_mozilla_df(result_df)

    # pytimeseries works best if we write all datapoints for a given timestamp
    # in a single batch, so we will save our fetched data into a dictionary
    # keyed by timestamp. Once we've fetched everything, then we can walk
    # through that dictionary to emit the data in timestamp order.
    for k, v in fetched.items():
        # note that v values are in the format:
        # {'proportion_timeout': float, 'proportion_unreachable': float, 'city_count': int}

        ts = int(k.timestamp())
        print(ts)
        print(v)

        if ts not in saved:
            saved[ts] = []

        # The traffic data is stored as a normalised float (with 10 d.p. of
        # precision -- we'd rather deal with integers so scale it up
        saved[ts].append((key, int(1000 * v['proportion_timeout']),
                          int(10 * v['proportion_unreachable']), int(v['city_count'])))
        print(saved)
    return 1


def get_query_string(start_time, end_time, region):
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

    if region:
        return base_query + f"\nAND country = '{region}'"
    return base_query


def process_mozilla_df(mozilla_df):
    region_agg_df = mozilla_df.groupby(["datetime", "country"]).agg({
        "proportion_timeout": "mean",
        "proportion_unreachable": "mean",
        "adjusted_city": lambda city: list(set(city)),
    }).reset_index()

    # for debugging only
    city_col_debugging = ['adjusted_city']
    region_agg_df = (transform_list_data_and_add_city_count(city_col_debugging, region_agg_df)
                     .set_index('datetime').drop(['country', 'adjusted_city'], axis=1))

    region_agg_df = region_agg_df.to_dict(orient="index")
    # print(region_agg_df)
    return region_agg_df


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

    # format timestamps into appropriate strings for querying
    endtime = endtime.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    starttime = starttime.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # for p in products:
    #     print(p, starttime, file=sys.stderr)
    #     for r in regions:
    #         ret = fetchData(trafrepo, starttime, endtime, p, r, datadict)
    ret = fetchData(MOZILLA_TABLE_NAME, args.projectid, starttime, endtime, args.region, datadict)

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
        description='Continually fetches Mozilla telemetry data from the Google Bigquery and writes it into kafka')

    parser.add_argument("--broker", type=str, required=True, help="The kafka broker to connect to")
    parser.add_argument("--channel", type=str, required=True, help="Kafka channel to write the data into")
    parser.add_argument("--topicprefix", type=str, required=True, help="Topic prefix to prepend to each Kafka message")
    parser.add_argument("--projectid", type=str, required=True, help="The Google Cloud project ID")
    parser.add_argument("--starttime", type=int, help="Fetch traffic data starting from the given Unix timestamp. \
                                                                    If not provided, defaults to 2 days before endtime.")
    parser.add_argument("--endtime", type=int, help="Fetch traffic data up until the given Unix timestamp. \
                                                                  If not provided, defaults to the current time.")
    args = parser.parse_args()

    main(args)
    # args for fetchData: mozilla_table_name, projectid, starttime, endtime, region, saved):
    # fetchData(MOZILLA_TABLE_NAME, region='NL', saved={})
    pass
