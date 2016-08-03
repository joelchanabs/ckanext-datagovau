import ckan.plugins.toolkit as toolkit

get_validator = toolkit.get_validator

not_missing = get_validator('not_missing')
not_empty = get_validator('not_empty')
resource_id_exists = get_validator('resource_id_exists')
package_id_exists = get_validator('package_id_exists')
ignore_missing = get_validator('ignore_missing')
empty = get_validator('empty')
boolean_validator = get_validator('boolean_validator')
int_validator = get_validator('int_validator')
OneOf = get_validator('OneOf')
url_validator = get_validator('url_validator')


def zipextractor_submit_schema():
    schema = {
        'id': [not_missing, not_empty, unicode],
        'package_id': [not_missing, not_empty, package_id_exists],
        'name': [not_missing, not_empty],
        'url': [not_missing, not_empty, url_validator],
    }
    return schema


def zipextractor_cleanup_schema():
    schema = {
        'name': [ignore_missing],
        'id': [not_missing, not_empty, unicode],
        'package_id': [not_missing, not_empty],
    }
    return schema


def spatialingestor_submit_schema():
    schema = {
        'id': [not_missing, not_empty, unicode],
        'package_id': [not_missing, not_empty, package_id_exists],
        'name': [not_missing, not_empty],
        'url': [not_missing, not_empty, url_validator],
    }
    return schema


def spatialingestor_cleanup_schema():
    schema = {
        'name': [ignore_missing],
        'id': [not_missing, not_empty, unicode],
        'package_id': [not_missing, not_empty],
    }
    return schema
