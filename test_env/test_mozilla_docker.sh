#!/bin/bash

HOSTNAME=`hostname`
DOMAIN=`hostname -d`

echo "Running Kafka & Zookeeper startup script..."
docker compose -f kafka_setup.yml up -d 2>compose_error.log

until docker compose -f kafka_setup.yml up -d 2>compose_error.log; do
    echo "Kafka setup failed. Checking for port conflicts..."

    PORTS=(2181 9092)
    CONFLICT_FOUND=false

    for PORT in "${PORTS[@]}"; do
    PIDS=$(lsof -t -i :"$PORT")
    if [ -n "$PIDS" ]; then
        echo "Port $PORT conflict detected. Killing processes..."
        while read -r PID; do
            [ -z "$PID" ] && continue
            echo "Killing PID $PID on port $PORT..."
            kill -9 "$PID"
        done <<< "$PIDS"
        CONFLICT_FOUND=true
    fi
done

    # If no conflicts were found for either port, assume Compose failed for another reason
    if [ "$CONFLICT_FOUND" = false ]; then
        echo "Docker Compose failed for another reason. Check compose_error.log"
        exit 1
    fi
done

echo "Waiting for Kafka & Zookeeper to be ready..."

until docker exec kafka_test kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    ZOOKEEPER_STATUS=$(docker ps --filter "name=zookeeper" --format "{{.Status}}")
    KAFKA_STATUS=$(docker ps --filter "name=kafka" --format "{{.Status}}")

    if [[ -n "$ZOOKEEPER_STATUS" && "$ZOOKEEPER_STATUS" == *"Up"* ]]; then
        echo "Zookeeper is up: $ZOOKEEPER_STATUS"
    else
        echo "Zookeeper failed to start. Showing last logs:"
        docker compose -f kafka_setup.yml logs zookeeper --tail=20
    fi

    if [[ -n "$KAFKA_STATUS" && "$KAFKA_STATUS" == *"Up"* ]]; then
        echo "Kafka is up: $KAFKA_STATUS"
    else
        echo "Kafka failed to start. Showing last logs:"
        docker compose -f kafka_setup.yml logs kafka --tail=20
    fi

    docker ps --filter "name=kafka" --filter "name=zookeeper" --format "table {{.Names}}\t{{.Status}}"
    echo "Kafka & Zookeeper not ready yet, retrying after 5s..."
    sleep 5
done
echo "Kafka & Zookeper are ready!"

TOPIC_PREFIX="mytopicprefix"
CHANNEL="mychannel"

TOPIC_NAME="${TOPIC_PREFIX}.${CHANNEL}"
echo "Creating topic $TOPIC_NAME..."
docker exec kafka_test kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic "$TOPIC_NAME"

echo "Topic $TOPIC_NAME created!"

echo "Building Docker image for ioda-moz-staging..."
docker build -f Dockerfile.test --platform=linux/amd64 --no-cache -t ioda-moz-staging . && \
echo "Running ioda-moz-staging container..." && \
docker run -d --platform=linux/amd64 --rm --network local_kafka_default --name ioda-moz-staging  \
	-v "$HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json" \
	-e HOME=/root \
	ioda-moz-staging --broker kafka:9092 --channel ${CHANNEL} \
	--topicprefix ${TOPIC_PREFIX} --projectid MYPROJECTID && \
echo "Mozilla data pulled and pushed to Kafka!"

#echo "Earliest message pushed: "
#docker exec -it kafka_test kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mytopicprefix.mychannel --from-beginning -max-messages=1