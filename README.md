# IODA Mozilla
- [Prerequisites](#prerequisites)
- [Scraper script arguments](#scraper-script-arguments)
- [Script overview](#script-overview)

## Prerequisites
To obtain Mozilla data from BigQuery using the scraper script, you will first need to generate the Application Default Credentials (ADC) for authentication. 
For more information about ADCs: https://cloud.google.com/docs/authentication/application-default-credentials
1. Install the Google Cloud CLI on the machine it will run on.
    - https://cloud.google.com/sdk/docs/install
2. Create local authentication credentials for the principal user account by entering the following in the terminal: 
   ```
   gcloud auth application-default login
   ```
3. A Google sign-in screen should open in your web browser. Select the relevant user account. After you land on the page indicating successful authentication, the Authorization Default Credentials (ADC) for the account is created. 
You can find the `generated application_default_credentials.json` file in the following paths:
- Linux/MacOS:
  ```
  $HOME/.config/gcloud/application_default_credentials.json    
  ```
- Windows:
  ```
  %APPDATA%\gcloud\application_default_credentials.json
  ```

4. In the shell script to set up the Docker container, ensure that the following line is included to map the local ADC to the corresponding Docker directory:
- Linux/MacOS: 
  ```
  -v "$HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json`
  ```
- Windows:
  ```
  -v "C:\Users\d\AppData\Roaming\gcloud\application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json" 
  ```

### Running locally
https://cloud.google.com/docs/authentication/set-up-adc-local-dev-environment

### Running in a containerized environment
https://cloud.google.com/docs/authentication/set-up-adc-containerized-environment

## Scraper script arguments
In the Docker container script, some key arguments can be specified:  
- `--projectid`, which is your own GCP Project ID. Please note that this is **required.**
- `--endtime`, a Unix timestamp corresponding to the end time of the data to be fetched. If not provided, defaults to the current time.
- `--starttime`, a Unix timestamp corresponding to the starting time of the data to be fetched. If not provided, defaults to the end time minus the lookback period (in days).
> Note:
> - The default lookback period is **2 days**.
> - If you would like to change this, please update `DEFAULT_LOOKBACK_PERIOD` in `constants.py`.

Other utility arguments you could also specify are:
- `--debug`. Specify `y` to enable debug mode that prints the **fetched and processed data** to the command line. If debug mode is activated, Data is also not pushed to Kafka.
- `--savedata`. Specify `y` to enable save mode for saving **fetched (and unprocessed)** Mozilla data with all metrics as a `.csv` file in the `/data` directory.


## Script overview
The script does the following:
1. Fetches data for all countries from the specified (or default) timestamp.
>Note: 
>- The default configuration fetches data from all countries. 
>- If you would like to fetch data from a specific country, please change the `country_code` parameter for the `fetchData` function in `main` (line 283). 

2. Aggregates data into two separate DataFrames, one country-aggregated and one region-aggregated.
3. Calculates relevant metrics for each country and region.
4. The processed country & region-aggregated data are then combined into a single dictionary, where country-aggregated data comes first, followed by region-aggregated data.
5. The combined data is sent to Kafka.

