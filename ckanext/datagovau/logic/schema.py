import tempfile

from ckan.logic.schema import validator_args


@validator_args
def get_package_stats(ignore_missing, package_id_or_name_exists):
    return {"id": [ignore_missing, package_id_or_name_exists]}


@validator_args
def set_package_stats(
    not_missing, ignore_missing, package_id_or_name_exists, int_validator
):
    return {
        "id": [not_missing, package_id_or_name_exists],
        "views": [ignore_missing, int_validator],
        "downloads": [ignore_missing, int_validator],
    }


@validator_args
def extract_resource(not_missing, default, unicode_safe):
    return {
        "id": [not_missing, unicode_safe],
        "tmp_dir": [default(tempfile.gettempdir()), unicode_safe],
    }
