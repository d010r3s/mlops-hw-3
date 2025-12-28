CREATE DATABASE IF NOT EXISTS demo;

DROP VIEW IF EXISTS demo.mv_kafka_to_transactions;
DROP TABLE IF EXISTS demo.kafka_transactions;
DROP TABLE IF EXISTS demo.transactions;

CREATE TABLE demo.kafka_transactions
(
    transaction_time String,
    us_state String,
    cat_id String,
    amount Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'ch_group_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_handle_error_mode = 'stream';

CREATE TABLE demo.transactions
(
    transaction_time DateTime,
    us_state String,
    cat_id String,
    amount Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, transaction_time, amount)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW demo.mv_kafka_to_transactions
TO demo.transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    us_state,
    cat_id,
    amount
FROM demo.kafka_transactions;
