# -*- coding: utf-8 -*-
#
# @created: 10.04.2022
# @author: Aleksey Komissarov
# @contact: ad3002@gmail.com
"""Views for API calls."""

from abc import ABC, abstractmethod

from django.http import JsonResponse
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q

from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView


from movies.models.models import Filmwork
from movies.models.models import Role


class ApiViewMixin(ABC):

    model = Filmwork

    http_method_names = ["get"]

    @abstractmethod
    def get_queryset(self):
        pass

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context, safe=False)


def get_aggegated_by_role(role):
    return ArrayAgg(
        "persons__full_name", filter=Q(personfilmwork__role=Role[role]), distinct=True
    )


class MoviesListApi(ApiViewMixin, BaseListView):

    paginate_by = 50

    def get_queryset(self):
        return (
            self.model.objects.values(
                "id",
                "title",
                "description",
                "creation_date",
                "rating",
                "type",
            )
            .annotate(
                genres=ArrayAgg("genres__name", distinct=True),
                actors=get_aggegated_by_role("actor"),
                directors=get_aggegated_by_role("director"),
                writers=get_aggegated_by_role("writer"),
            )
            .all()
        )

    def get_context_data(self, *, object_list=None, **kwargs):
        full_queryset = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            full_queryset, self.get_paginate_by(full_queryset)
        )
        context = {
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "prev": page.previous_page_number() if page.has_previous() else None,
            "next": page.next_page_number() if page.has_next() else None,
            "results": list(page.object_list),
        }
        return context


class MoviesDetailApi(ApiViewMixin, BaseDetailView):
    def get_queryset(self):
        return (
            self.model.objects.values(
                "id",
                "title",
                "description",
                "creation_date",
                "rating",
                "type",
            )
            .annotate(
                genres=ArrayAgg("genres__name", distinct=True),
                actors=get_aggegated_by_role(Role.ACTOR),
                directors=get_aggegated_by_role(Role.DIRECTOR),
                writers=get_aggegated_by_role(Role.WRITER),
            )
            .filter(pk=self.kwargs["pk"])
        )

    def get_context_data(self, *, object_list=None, **kwargs):
        return list(self.get_queryset())[0]
