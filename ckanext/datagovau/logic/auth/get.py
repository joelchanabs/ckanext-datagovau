from ckanext.toolbelt.decorators import Collector

from ckan.authz import is_authorized


auth, get_get_auth_functions = Collector("dga").split()


@auth
def get_package_stats(context, data_dict):
    return is_authorized("sysadmin", context, data_dict)
