import logging
import json
import geomet

import ckan.plugins.toolkit as tk

from ckanext.toolbelt.decorators import Collector
from ckanext.agls.utils import details_for_gaz_id

log = logging.getLogger(__name__)
validator, get_validators = Collector("dga").split()


@validator
def spatial_from_coverage(key, data, errors, context):
    details = []
    coverage = data[("spatial_coverage",)]
    if not coverage:
        return
    id_ = coverage.split(":")[0]
    try:
        details = details_for_gaz_id(id_)
    except KeyError as e:
        log.warning("Cannot get details for GazId %s: %s", id_, e)

    valid_geojson = True
    try:
        coverage_json = json.loads(coverage)
        geomet.wkt.dumps(coverage_json)
    except (ValueError, geomet.InvalidGeoJSONException) as e:
        valid_geojson = False
        log.warning("Entered coverageerage is not a valid geojson")

    if details:
        data[key] = details["geojson"]
    elif valid_geojson:
        data[key] = coverage
    elif data.get(("id",)):
        data_dict = tk.get_action("package_show")({}, {"id": data[("id",)]})
        data[("spatial_coverage",)] = data_dict.get("spatial_coverage")
        data[key] = data_dict.get("spatial")
    else:
        errors[("spatial_coverage",)].append(tk._("Entered value cannot be converted into a spatial object"))
