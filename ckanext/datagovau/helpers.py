from __future__ import annotations

import time
from datetime import datetime as dt
from typing import Any, Optional

import ckan.model as model
import ckan.plugins.toolkit as tk
import feedparser

import ckanext.agls.utils as agls_utils
import ckanext.datastore.backend as datastore_backend
from ckanext.toolbelt.decorators import Cache, Collector

from . import types

helper, get_helpers = Collector("dga").split()
cache = Cache(duration=600)


@helper
def get_ddg_site_statistics() -> types.DdgStatistics:
    package_search = tk.get_action("package_search")
    total = package_search({}, {"include_private": True, "rows": 0})["count"]
    unpublished = package_search(
        {}, {"fq": "unpublished:true", "include_private": True, "rows": 0}
    )["count"]
    open_count = package_search(
        {}, {"fq": "isopen:true", "include_private": True, "rows": 0}
    )["count"]
    api_count = _api_count()

    return types.DdgStatistics(
        dataset_count=total,
        unpub_data_count=unpublished,
        open_count=open_count,
        api_count=api_count,
    )


@cache
def _api_count():
    return tk.get_action("resource_search")(
        {}, {"query": ["format:wms"], "limit": 0}
    )["count"] + len(datastore_backend.get_all_resources_ids_in_datastore())


@helper
def blogfeed():
    d = feedparser.parse("https://blog.data.gov.au/blogs/rss.xml")
    for entry in d.entries:
        entry.date = time.strftime("%a, %d %b %Y", entry.published_parsed)
    return d


@helper
def geospatial_topics(_field: dict[str, Any]) -> types.SchemingChoices:
    return [{"value": t, "label": t} for t in agls_utils.geospatial_topics()]


@helper
def fields_of_research(_field: dict[str, Any]) -> types.SchemingChoices:
    return [{"value": t, "label": t} for t in agls_utils.fields_of_research()]


@helper
def agift_themes(_field: dict[str, Any]) -> types.SchemingChoices:
    groups = tk.get_action("group_list")({}, {"all_fields": True})
    empty = {"value": "", "label": "Please Select"}
    return [empty] + [
        {"value": g["id"], "label": g["display_name"]} for g in groups
    ]


_stat_labels = {
    "api": "API enabled resources",
    "open": "Openly licenced datasets",
    "unpublished": "Unpublished datasets",
}


@helper
def stat_group_to_facet_label(group: str) -> Optional[str]:
    return _stat_labels.get(group)


@helper
def get_package_stats(package_id: str):
    context = {
        "model": model,
        "session": model.Session,
        "user": tk.g.user,
    }

    try:
        stats = tk.get_action("dga_get_package_stats")(
            context, {"id": package_id}
        )
    except (tk.ObjectNotFound, tk.ValidationError, tk.NotAuthorized):
        return {}

    return [
        {
            "labels": [
                dt.strptime(date, "%Y-%m").strftime("%Y %b")
                for date in stats.keys()
            ],
            "datasets": [
                {
                    "label": tk._(category.title()),
                    "data": [month[category] for month in stats.values()],
                    "backgroundColor": "blue"
                    if category == "views"
                    else "green",
                }
            ],
            "total": sum(month[category] for month in stats.values()),
            "category": tk._(category.title()),
        }
        for category in ("downloads", "views")
    ]
