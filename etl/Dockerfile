# pull official base image
FROM python:3.9.6-alpine as builder

# install psycopg2 dependencies
RUN apk update \
    && apk add postgresql-dev gcc python3-dev musl-dev

COPY . /app
WORKDIR /app/
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

# run entrypoint.sh
RUN chmod +x  /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]

CMD ["python", "main.py"] 