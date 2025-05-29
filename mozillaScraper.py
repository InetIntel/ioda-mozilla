import logging
import time
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

from constants import GCP_PROJECT_ID, NE_MAP_PATH

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
BASEKEY = "mozilla"


def fetchData(mozilla_table_name, region, saved=None,
              start_time=None, end_time=datetime.now()):
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

    if start_time:
        start_time = datetime.fromtimestamp(start_time)
    else:
        start_time = end_time - timedelta(days=DEFAULT_LOOKBACK_PERIOD)

    end_time = end_time.astimezone(timezone.utc)
    start_time = start_time.astimezone(timezone.utc)

    end_time_fmt = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_time_fmt = start_time.strftime("%Y-%m-%d %H:%M:%S")

    query = ""

    if region:
        query = get_query_string(start_time, end_time, region)

    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        job = client.query(query)
        mozilla_df = job.to_dataframe()
    except Exception as error:
        logging.warning("Failed to get data for %s.%s: %s", \
                        region, str(error))
        return -1

    time.sleep(0.5)
    if mozilla_df.empty:
        return 0

    fetched = process_mozilla_df(mozilla_df)

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
        saved[ts].append((key, int(1000* v['proportion_timeout']),
                          int(10*v['proportion_unreachable']), int(v['city_count'])))
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
        # todo: remove later, keep inside here for debugging first
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
    # if start_time:
    #     start_time = datetime.fromtimestamp(start_time)
    # else:
    #     start_time = end_time - timedelta(days=DEFAULT_LOOKBACK_PERIOD)
    #
    # end_time = end_time.astimezone(timezone.utc)
    # start_time = start_time.astimezone(timezone.utc)
    #
    # end_time_fmt = end_time.strftime("%Y-%m-%d %H:%M:%S")
    # start_time_fmt = start_time.strftime("%Y-%m-%d %H:%M:%S")
    return


if __name__ == "__main__":
    fetchData(MOZILLA_TABLE_NAME, region='NL', saved={})
    pass
