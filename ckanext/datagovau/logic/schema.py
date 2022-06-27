from ckan.logic.schema import validator_args


@validator_args
def get_package_stats_schema(ignore_missing, package_id_or_name_exists):
    return {"id": [ignore_missing, package_id_or_name_exists]}


@validator_args
def set_package_stats_schema(
    not_missing, ignore_missing, package_id_or_name_exists, int_validator
):
    return {
        "id": [not_missing, package_id_or_name_exists],
        "views": [ignore_missing, int_validator],
        "downloads": [ignore_missing, int_validator],
    }
