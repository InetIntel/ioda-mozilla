#!/bin/bash

HOSTNAME=`hostname`
DOMAIN=`hostname -d`

echo "Starting Kafka and Zookeeper..."
docker-compose -f kafka-setup.yml up -d

echo "Waiting for Kafka to be ready..."
until docker run --rm --network $(docker network ls | grep local_kafka_default | awk '{print $1}') bitnami/kafka:3.1 \
    kafka-topics.sh --list --bootstrap-server kafka:9092 >/dev/null 2>&1; do
    echo "Kafka not ready yet, sleeping 5s..."
    sleep 5
done

echo "Kafka setup is complete"

TOPIC_PREFIX="mytopicprefix"
CHANNEL="mychannel"

# 26 nov: rollback & check permissions
echo "Creating topic ${TOPIC_PREFIX}.${CHANNEL}..."
docker run --rm --network local_kafka_default bitnami/kafka:3.1 \
  kafka-topics.sh --create --topic ${TOPIC_PREFIX}.${CHANNEL} \
  --partitions 1 --replication-factor 1 \
  --if-not-exists --bootstrap-server kafka:9092

echo "Building Docker image for ioda-moz-staging..."
docker build --no-cache -t ioda-moz-staging .

echo "Running ioda-moz-staging container..."
docker run -d --rm --network local_kafka_default --name ioda-moz-staging  \
	-v "$HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json" \
	-e HOME=/root \
	ioda-moz-staging --broker kafka:9092 --channel ${CHANNEL} \
	--topicprefix ${TOPIC_PREFIX} --projectid MYPROJECTID

echo "Mozilla data pulled and pushed to Kafka!"

# Use the following command to access data from Kafka
# docker run --rm --network local_kafka_default bitnami/kafka:3.1 kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic mytopicprefix.mychannel --from-beginning=false