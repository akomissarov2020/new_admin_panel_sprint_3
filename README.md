# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.

# Architecture

## Docker containers implemented with compose

- Nginx server
- Django application (admin and api parts)
- Postgres DB
- Redis database for state keeping
- ETL instance

## ETL architecure

- the singleton main process (entrypoint)
- backoffed consumer
- backoffed producer
- backoffed main
- state/storage with Redis
- one huge SQL with modified date for updated fields
