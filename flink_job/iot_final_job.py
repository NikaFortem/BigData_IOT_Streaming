from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    table_env.execute_sql("""
        CREATE TEMPORARY SYSTEM FUNCTION MEDIAN_HUMIDITY
        AS 'com.hse.bigdata.MedianHumidityAgg'
        LANGUAGE JAVA
    """)

    table_env.execute_sql("""
        CREATE TABLE iot_events (
            device_type_id INT,
            event_time STRING,
            temperature DOUBLE,
            humidity DOUBLE,
            event_ts AS TO_TIMESTAMP_LTZ(
                UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSSXXX') * 1000,
                3
            ),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot_events',
            'properties.bootstrap.servers' = 'iot_kafka:29092',
            'properties.group.id' = 'flink_final_java_median_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE device_types (
            id INT,
            type_name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://iot_postgres:5432/iot_db',
            'table-name' = 'device_types',
            'username' = 'iot_user',
            'password' = 'iot_password'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE iot_agg_results (
            window_time STRING,
            device_type STRING,
            avg_temperature DOUBLE,
            median_humidity DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot_agg_results',
            'properties.bootstrap.servers' = 'iot_kafka:29092',
            'format' = 'json'
        )
    """)

    table_env.execute_sql("""
        INSERT INTO iot_agg_results
        SELECT
            DATE_FORMAT(TUMBLE_START(e.event_ts, INTERVAL '1' MINUTE), 'HH:mm') AS window_time,
            d.type_name AS device_type,
            AVG(e.temperature) AS avg_temperature,
            MEDIAN_HUMIDITY(e.humidity) AS median_humidity
        FROM iot_events AS e
        JOIN device_types AS d
        ON e.device_type_id = d.id
        GROUP BY
            TUMBLE(e.event_ts, INTERVAL '1' MINUTE),
            d.type_name
    """).wait()


if __name__ == "__main__":
    main()
