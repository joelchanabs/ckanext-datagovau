from ckan.authz import is_authorized

from ckanext.toolbelt.decorators import Collector

auth, get_auth_functions = Collector("dga").split()


@auth
def get_package_stats(context, data_dict):
    return is_authorized("sysadmin", context, data_dict)


@auth
def extract_resource(context, data_dict):
    return is_authorized("resource_update", context, data_dict)
