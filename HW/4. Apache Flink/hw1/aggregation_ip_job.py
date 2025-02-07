import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

# Create an .env file in the project root directory and add the following environment variables:
# In order to run this program, you need to set up a PostgreSQL database and a Kafka broker.

# The following environment variables are required to run the script:

# POSTGRES_URL: URL of the PostgreSQL database
# POSTGRES_USER: Username for the PostgreSQL database
# POSTGRES_PASSWORD: Password for the PostgreSQL database
# KAFKA_URL: URL of the Kafka broker
# KAFKA_TOPIC: Kafka topic name for the event stream
# KAFKA_GROUP: Kafka consumer group ID
# KAFKA_WEB_TRAFFIC_KEY: Kafka authentication key
# KAFKA_WEB_TRAFFIC_SECRET: Kafka authentication secret


# Function to create a PostgreSQL sink table for aggregated events
# This function sets up a JDBC connection to PostgreSQL and creates a table
# The table stores processed events with timestamps, host, IP, and the count of hits

def create_aggregated_events_ip_sink_postgres(t_env):
    table_name = 'processed_events_ip_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_window_timestamp TIMESTAMP(3),  -- Timestamp for event window
            host VARCHAR,  -- Host from which event originated
            ip VARCHAR,  -- IP address of the source
            num_hits BIGINT  -- Number of hits in the event window
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',  -- Database URL from environment variables
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',  -- Default user 'postgres'
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',  -- Default password 'postgres'
            'driver' = 'org.postgresql.Driver'  -- PostgreSQL driver
        );
    """
    # Execute the SQL statement to create the sink table in PostgreSQL
    t_env.execute_sql(sink_ddl)
    return table_name

# Function to create a Kafka source table for processing events
# This function configures a Kafka consumer to read event data from a specified topic

def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")  # Retrieve Kafka authentication key
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")  # Retrieve Kafka authentication secret
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"  # Timestamp format pattern
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,  -- IP address of the user
            event_time VARCHAR,  -- Raw event timestamp as a string
            referrer VARCHAR,  -- Referrer URL
            host VARCHAR,  -- Host from which event originated
            url VARCHAR,  -- URL accessed
            geodata VARCHAR,  -- Geolocation data of the user
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),  -- Convert event_time to timestamp
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND  -- Define watermark for event processing
        ) WITH (
            'connector' = 'kafka',  -- Kafka connector for streaming
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',  -- Kafka broker URL
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',  -- Kafka topic name
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',  -- Consumer group ID
            'properties.security.protocol' = 'SASL_SSL',  -- Secure protocol for Kafka connection
            'properties.sasl.mechanism' = 'PLAIN',  -- Authentication mechanism
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'scan.startup.mode' = 'latest-offset',  -- Start reading from the latest offset
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'  -- JSON format for Kafka messages
        );
    """
    # Execute the SQL statement to create the Kafka source table
    t_env.execute_sql(sink_ddl)
    return table_name

# Function to perform log aggregation by processing Kafka stream and storing results in PostgreSQL
# Uses session windowing to group events by IP and host over a period of time

def log_aggregation():
    # Set up the execution environment for stream processing
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)  # Enable checkpointing with an interval of 10ms for fault tolerance
    env.set_parallelism(3)  # Set parallelism to 3 for concurrent execution

    # Set up the table environment in streaming mode
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create the Kafka source table for reading incoming events
        source_table = create_processed_events_source_kafka(t_env)

        # Create the PostgreSQL sink table to store aggregated results
        aggregated_ip_sink_table = create_aggregated_events_ip_sink_postgres(t_env)

        # Perform session-based aggregation on the streaming data
        t_env.from_path(source_table).window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")  # Create session window with 5-minute gap
        ).group_by(
            col("w"),  # Group by window
            col("host"),  # Group by host
            col("ip")  # Group by IP address
        ) \
            .select(
            col("w").start.alias("event_window_timestamp"),  # Select window start as event timestamp
            col("host"),  # Select host
            col("ip"),  # Select IP address
            col("host").count.alias("num_hits")  # Count number of hits per host and IP
        ) \
            .execute_insert(aggregated_ip_sink_table) \
            .wait()  # Wait for execution to complete

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))  # Handle errors and print error messages

# Main function to start the log aggregation process
# This function will be executed when the script is run
if __name__ == '__main__':
    log_aggregation()
