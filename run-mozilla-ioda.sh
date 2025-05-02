#!/bin/bash

# Exit immediately on error
set -e

# Build the Docker image
docker build -t mozilla-ioda .

# Run the container with ADC credentials mounted
docker run --rm \
  -v "$HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
  -e HOME=/root \
  mozilla-ioda
