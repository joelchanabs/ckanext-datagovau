from typing import Any

from ckanext.toolbelt.decorators import Collector

import ckan.plugins.toolkit as tk
from ckan.logic import validate

from ckanext.datagovau.logic import schema


action, get_get_actions = Collector("dga").split()


@action
@validate(schema.get_package_stats_schema)
@tk.side_effect_free
def get_package_stats(context, data_dict):
    tk.check_access("dga_get_package_stats", context, data_dict)

    user: dict[str, Any] = tk.get_action("get_site_user")(
        {"ignore_auth": True}, {}
    )
    context["user"] = user["name"]

    try:
        stats = tk.get_action("flakes_flake_lookup")(
            context,
            {"name": "dga_ga_stats"},
        )["data"]
    except tk.ObjectNotFound:
        raise tk.ObjectNotFound(tk._("No dataset statistics"))

    if "id" not in data_dict:
        return stats

    pkg_id_or_name: str = data_dict["id"]
    pkg = context["model"].Package.get(pkg_id_or_name)
    pkg_stats: dict[str, dict[str, int]] = stats.get(pkg.id)

    if not pkg_stats:
        raise tk.ObjectNotFound(
            tk._(f"No statistics for the specific dataset: {pkg_id_or_name}")
        )

    return pkg_stats
