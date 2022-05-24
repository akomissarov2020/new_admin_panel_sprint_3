CREATE SCHEMA IF NOT EXISTS content;
GRANT ALL PRIVILEGES ON DATABASE content TO docker;

DROP TABLE IF EXISTS content.person_film_work CASCADE;
DROP TABLE IF EXISTS content.genre_film_work CASCADE;
DROP TABLE IF EXISTS content.film_work;
DROP TABLE IF EXISTS content.person;
DROP TABLE IF EXISTS content.genre;
