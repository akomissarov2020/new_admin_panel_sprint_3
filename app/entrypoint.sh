#!/bin/sh

echo "Waiting for postgres..."

while ! nc -z $SQL_HOST $SQL_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

python manage.py collectstatic
python manage.py migrate
cd data
python load_data.py
cd ..

exec "$@"
