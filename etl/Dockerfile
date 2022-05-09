# pull official base image
FROM python:3.9.6-alpine as builder
COPY . /app
WORKDIR /app/
RUN apt-get update
RUN pip install -r requirements.txt
CMD ["python", "main.py"] 