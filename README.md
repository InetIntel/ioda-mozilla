# IODA Mozilla
- [Container setup](#container-setup)
- [Supporting docs](#supporting-docs)
- [Create IODA timeseries](#create-ioda-timeseries)



## Container setup
1. Create a Google Service account.
    - https://cloud.google.com/iam/docs/service-accounts-create
2. Install the gcloud CLI on the machine it will run on.
    - https://cloud.google.com/sdk/docs/install
3. Follow the prompts to after installation to select the service account that will run the queries. This creates the Authorization Default Credentials (ADC).
4. Run the container with one of the following commmands to insert the required credentials.

### Run on Windows

```
docker run --rm ^
  -v "C:\Users\d\AppData\Roaming\gcloud\application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json" ^
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json ^
  -e HOME=/root ^
  mozilla-ioda
```

### Run on Linux

```
docker run --rm \
  -v "$HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
  -e HOME=/root \
  mozilla-ioda
```

### Credential Expiration

See the documentation on ADCs linked below, specifically the section titled "A credential file created by using the gcloud auth application-default login command".
This workflow was tested by running the `gcloud auth application-default login` command, which produces two outputs:
  1. An access token with a life of 1 hour.
  2. A refresh token that allows automatic renewal.

The refresh tokens do not expire on a regular schedule, but they can expire in some specific scenarios, including:
  - Inactivity of >6 months.
  - Google password change.
  - Manually revoking the credentials.
  - Too many tokens are issued in a given time period.


Expiration info link:
https://cloud.google.com/docs/authentication/application-default-credentials

## Supporting Docs

### Running locally
https://cloud.google.com/docs/authentication/set-up-adc-local-dev-environment

### Running in a containerized environment
https://cloud.google.com/docs/authentication/set-up-adc-containerized-environment

## Create IODA timeseries
Before running the script, please replace the placeholder Project ID with your own GCP Project ID in `constants.py`:
```
GCP_PROJECT_ID = "<YOUR_PROJECT_ID_HERE>"
```

Using the script, you can obtain region- and country-aggregated Mozilla data. Currently, the data are processed in the form of DataFrames and saved as
CSV files in `/data`. 

### Parameters
There are four parameters that can be specified:
- `country` (2-letter country code, default None)
- `region` (4 digit IODA region code, default None)
- `end_time` (UNIX timestamp, default current time)
- `start_time` (UNIX timstamp, default None)

### Script overview
The script works in the following ways:
1. If no `end_time` is specified, the current time is taken.
2. If no `start_time` is specified, the start time is set to 2 days before the `end_time`.

>Example: 
> - For a query conducted on May 3, if both the start and end time are not specified, the period queried will be 1 May to 3 May.
> - For a query with a specified end date of May 4 with no start date, the period queried will be 2 May to 4 May.

The lookback period can be updated using the `DEFAULT_LOOKBACK_PERIOD` variable in `create_ioda_timeseries.py`.

3. If `region` is specified, data from the specific region and corresponding country will be obtained.
4. If `country` is specified but not `region`, country-aggregated and region-aggregated data will be obtained. 
5. If both `region` and `country` are specified, region takes precedence and the query country is ignored. Data from the specific region and corresponding country will be obtained.

> Example:
> * ```
>    get_mozilla_data(country='NL', region=4416)
>   ```
>   will yield regional data for 4416 (California), and country data for the US. No data for NL will be obtained.
> * ```
>   get_mozilla_data(region=4416)
>   ``` 
>   also yields the same data.
> * ```
>   get_mozilla_data(region='NL')
>   ``` 
>   will yield NL country data, and region-aggregated data for all regions in NL.

6. If neither `region` nor `country` are specified, all data from the specified timeframe is captured, and we obtain country-aggregated and region-aggregated.

### Run on Windows/Linux
```
python create_ioda_timeseries.py
``` 

### Troubleshooting
If you encounter a length mismatch error: 
```
 Length mismatch: Original Mozilla DataFrame has 13381 rows, DataFrame of Mozilla data merged with IODA ids has 13373 rows. Uncomment lines 103-110 to understand which rows were not mapped properly. Otherwise, comment out lines 114-119 to continue without the unmapped rows. 
```

- It is likely that some datapoints in the Mozilla data were not successfully mapped to the specified country/region due to missing IODA IDs in the NE mapping.
- Uncomment the mentioned lines to obtain a CSV file of which datapoints were not mapped. This file can be found in: `data/unmmatched.csv`
- Otherwise, comment out the specified lines (code for length mismatch test) to continue without the unmapped rows.
