import ckan.logic.auth.create as auth_create
import ckan.logic.auth.delete as auth_delete
import ckan.logic.auth.get as auth_get


def zipextractor_submit(context, data_dict):
    return auth_create.resource_create(context, data_dict)


def zipextractor_cleanup(context, data_dict):
    return auth_delete.resource_delete(context, data_dict)


def zipextractor_status(context, data_dict):
    return auth_get.resource_show(context, data_dict)


def spatialingestor_submit(context, data_dict):
    return auth_create.resource_create(context, data_dict)


def spatialingestor_cleanup(context, data_dict):
    return auth_delete.resource_delete(context, data_dict)


def spatialingestor_status(context, data_dict):
    return auth_get.resource_show(context, data_dict)
