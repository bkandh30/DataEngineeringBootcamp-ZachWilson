# IP Aggregation Job

This document describes how to run the IP aggregation job that processes web traffic data from Kafka and stores results in PostgreSQL.

## Prerequisites

- Python 3.7+
- Access to a Kafka cluster
- Access to a PostgreSQL database
- Required SQL file: `homework.sql`

## Environment Setup

Set the following environment variables before running the job:

```bash
export POSTGRES_URL="your_postgres_url"
export POSTGRES_USER="your_postgres_username"
export POSTGRES_PASSWORD="your_postgres_password"
export KAFKA_URL="your_kafka_broker_url"
export KAFKA_TOPIC="your_kafka_topic"
export KAFKA_GROUP="your_consumer_group_id"
export KAFKA_WEB_TRAFFIC_KEY="your_kafka_auth_key"
export KAFKA_WEB_TRAFFIC_SECRET="your_kafka_auth_secret"
```

## Running the Job

1. First, ensure the PostgreSQL schema is set up:

```bash
psql -U $POSTGRES_USER -h $POSTGRES_URL -f homework.sql
```

2. Run the aggregation job using Docker Compose:

```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregation_ip_job.py --pyFiles /opt/src -d
```

This command:

- Uses Docker Compose to execute the job in the Flink cluster
- Runs the job in detached mode (`-d`)
- Includes the source directory as additional Python files (`--pyFiles /opt/src`)

You can also run the job locally for development:

```bash
python aggregation_ip_job.py
```

## Monitoring

The job will continuously process events from Kafka and update aggregations in PostgreSQL. Check the console output for processing status and any potential errors.

## Troubleshooting

If you encounter connection issues:

1. Verify all environment variables are set correctly
2. Ensure network connectivity to both Kafka and PostgreSQL
3. Verify the SQL schema has been properly initialized
