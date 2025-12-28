# Домашка №3 MLOps
##### Торговкина Мария

#### Архитектура решения
```
train.csv
   ↓ producer
Kafka topic: transactions
   ↓ kafka
ClickHouse.kafka_transactions
   ↓
ClickHouse.transactions (MergeTree)
   ↓ sql
output/result.csv
```

#### Запуск:
!! нужно вручную положить `train.csv` в папку `data/`, он слишком тяжелый, чтобы залить на гитхаб
```bash
docker compose up -d --build
docker compose run --rm producer
# docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM demo.transactions" можно проверить, что все ок
docker compose run --rm export_result
```

#### Оптимизация хранения данных
Выполнена в 01_ddl.sql
* ENGINE = MergeTree
* PARTITION BY toYYYYMM(transaction_time)
* ORDER BY (us_state, transaction_time, amount)
