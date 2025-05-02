# IODA Mozilla

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