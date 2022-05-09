# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Models for movies app."""

from datetime import datetime

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

from .mixins import TimeStampedMixin, UUIDMixin


class Genre(UUIDMixin, TimeStampedMixin):
    """Genre model."""

    # Первым аргументом обычно идёт человекочитаемое название поля
    name = models.CharField(_("name"), max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_("description"), blank=True, null=True)

    class Meta(object):
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = 'content"."genre'
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = _("Жанр")
        verbose_name_plural = _("Жанры")

        constraints = [models.UniqueConstraint(fields=["name"], name="name_uniq")]

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    """Person model."""

    # Первым аргументом обычно идёт человекочитаемое название поля
    full_name = models.CharField(_("name"), max_length=255)

    class Meta(object):
        """Meta for Person model."""

        db_table = 'content"."person'
        verbose_name = _("Персона")
        verbose_name_plural = _("Персоны")

    def __str__(self):
        return self.full_name


class FilmType(models.TextChoices):
    """FilmType choices container."""

    MOVIE = "movie", _("movie_type")
    SHORT = "short", _("short")
    TV = "tvSeries", _("tvSeries")


class Filmwork(UUIDMixin, TimeStampedMixin):
    """Filmwork model."""

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), blank=True, null=True)
    creation_date = models.DateField(_("creation_date"), blank=True, null=True)
    file_path = models.FileField(_("file"), blank=True, null=True, upload_to="movies/")
    rating = models.FloatField(
        _("rating"),
        blank=True,
        null=True,
        validators=[MinValueValidator(0), MaxValueValidator(10)],
    )
    type = models.CharField(
        _("type"),
        max_length=255,
        choices=FilmType.choices,
    )

    genres = models.ManyToManyField(
        Genre, through="GenreFilmwork", verbose_name=_("genres")
    )
    persons = models.ManyToManyField(
        Person, through="PersonFilmwork", verbose_name=_("persons")
    )

    class Meta(object):
        """Meta for Filmwork model."""

        db_table = 'content"."film_work'
        verbose_name = _("Фильм")
        verbose_name_plural = _("Фильмы")

        indexes = [
            models.Index(fields=["creation_date"]),
        ]

    def __str__(self):
        return self.title

    def __getattribute__(self, name):
        attr = models.Model.__getattribute__(self, name)
        if name == "creation_date" and not attr:
            return datetime.today().strftime("%Y-%m-%d")
        return attr


class GenreFilmwork(UUIDMixin):
    """GenreFilmwork model."""

    film_work = models.ForeignKey(
        "Filmwork", on_delete=models.CASCADE, verbose_name=_("film_work_id")
    )
    genre = models.ForeignKey(
        "Genre", on_delete=models.CASCADE, verbose_name=_("genre_id")
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta(object):
        """Meta for PersonFilmwork model."""

        db_table = 'content"."genre_film_work'


class Role(models.TextChoices):
    """Role choices container."""

    ACTOR = "actor", _("actor")
    WRITER = "writer", _("writer")
    DIRECTOR = "director", _("director")


class PersonFilmwork(UUIDMixin):
    """PersonFilmwork model."""

    film_work = models.ForeignKey(
        Filmwork, on_delete=models.CASCADE, verbose_name=_("film_work")
    )
    person = models.ForeignKey(
        Person, on_delete=models.CASCADE, verbose_name=_("person")
    )
    role = models.CharField(_("role"), max_length=30, choices=Role.choices)
    created = models.DateTimeField(auto_now_add=True)

    class Meta(object):
        """Meta for PersonFilmwork model."""

        db_table = 'content"."person_film_work'

        constraints = [
            models.UniqueConstraint(
                fields=["film_work", "person", "role"],
                name="film_work_person_role_uniq",
            )
        ]
