# -*- coding: utf-8 -*-
#
# @created: 10.05.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com

query_films = """
        SELECT content.film_work.id,
        content.film_work.rating AS imdb_rating,
        ARRAY_AGG(DISTINCT content.genre.name) AS genres,
        content.film_work.title,
        content.film_work.description,
        ARRAY_AGG(DISTINCT content.person.full_name)
        FILTER(WHERE content.person_film_work.role = 'director') AS directors_names,
        ARRAY_AGG(DISTINCT content.person.full_name)
        FILTER(WHERE content.person_film_work.role = 'actor') AS actors_names,
        ARRAY_AGG(DISTINCT content.person.full_name)
        FILTER(WHERE content.person_film_work.role = 'writer') AS writers_names,
        JSON_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name))
        FILTER(WHERE content.person_film_work.role = 'actor') AS actors,
        JSON_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name))
        FILTER(WHERE content.person_film_work.role = 'writer') AS writers,
        JSON_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name))
        FILTER(WHERE content.person_film_work.role = 'director') AS directors
        FROM content.film_work
        LEFT OUTER JOIN content.genre_film_work ON (content.film_work.id = content.genre_film_work.film_work_id)
        LEFT OUTER JOIN content.genre ON (content.genre_film_work.genre_id = content.genre.id)
        LEFT OUTER JOIN content.person_film_work ON (content.film_work.id = content.person_film_work.film_work_id)
        LEFT OUTER JOIN content.person ON (content.person_film_work.person_id = content.person.id)
        WHERE greatest(content.film_work.modified, content.person.modified, content.genre.modified) > '%s'
        GROUP BY content.film_work.id, content.film_work.title, content.film_work.description, content.film_work.rating
        """

query_genres = """
        SELECT content.genre.id, content.genre.name, content.genre.description
        FROM content.genre
        WHERE content.genre.modified > '%s'
        """

query_persons = """
        SELECT content.person.id, content.person.full_name
        FROM content.person
        WHERE content.person.modified > '%s'
        """
