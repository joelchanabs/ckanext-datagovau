from __future__ import annotations

import inspect

from typing import Any
import ckan.plugins as p
import ckan.plugins.toolkit as tk
import ckan.model as model

import ckanext.datagovau.helpers as helpers
from ckanext.xloader.plugin import xloaderPlugin
import ckan.authz as authz
import ckan.lib.jobs as jobs
import ckan.lib.helpers as h

from . import validators, cli
from ckanext.datagovau.geoserver_utils import run_ingestor, delete_ingested


ingest_rest_list = [
    'kml',
    'kmz',
    'shp',
    'shapefile'
]

geo_pub_url = tk.config.get(
            "ckanext.datagovau.spatialingestor.geoserver.public_url")

ignore_ingestor_workflow = tk.config.get(
            "ckanext.datagovau.spatialingestor.ignore_workflow", False)


_original_xnotify = xloaderPlugin.notify
def _dga_xnotify(self, resource):
    try:
        return _original_xnotify(self, resource)
    except tk.ObjectNotFound:
        # resource has `deleted` state
        pass
xloaderPlugin.notify = _dga_xnotify


_original_permission_check = authz.has_user_permission_for_group_or_org

def _dga_permission_check(group_id, user_name, permission):
    stack = inspect.stack()
    # Bypass authorization to enable datasets to be removed from/added to AGIFT
    # classification
    if stack[1].function == "package_membership_list_save":
        return True
    return _original_permission_check(group_id, user_name, permission)


authz.has_user_permission_for_group_or_org = _dga_permission_check


class DataGovAuPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=False)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IValidators)
    p.implements(p.IClick)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.IDomainObjectModification)

    # IClick

    def get_commands(self):
        return cli.get_commands()

    # IConfigurer

    def update_config(self, config):
        tk.add_template_directory(config, "templates")
        tk.add_resource("assets", "datagovau")
        tk.add_public_directory(config, "assets")

    # ITemplateHelpers

    def get_helpers(self) -> dict[str, Any]:
        return helpers.get_helpers()

    # IValidators

    def get_validators(self):
        return validators.get_validators()

    # IPackageController

    def before_index(self, pkg_dict):
        pkg_dict["unpublished"] = tk.asbool(pkg_dict.get("unpublished"))
        return pkg_dict

    def before_search(self, search_params):
        stat_facet = search_params["extras"].get("ext_dga_stat_group")
        if stat_facet:
            search_params.setdefault("fq_list", []).append(
                _dga_stat_group_to_fq(stat_facet)
            )
        return search_params

    def after_delete(self, context, pkg_dict):
        if pkg_dict.get('id'):
            if not tk.asbool(ignore_ingestor_workflow):
                try:
                    jobs.enqueue(delete_ingested,
                            kwargs={'pkg_id': pkg_dict['id']},
                            rq_kwargs={'timeout': 1000})
                except Exception as e:
                    h.flash_error(f"{e}")

    #IDomainObjectModification
    
    def notify(self, entity, operation):
        if not tk.asbool(ignore_ingestor_workflow):
            if operation == 'changed' and isinstance(entity, model.Package):
                if entity.state == 'active':
                    ingest_resources = [
                        res for res in entity.resources
                        if res.format.lower() in ingest_rest_list
                    ]
                    geoserver_resources = [
                        res for res in entity.resources
                        if geo_pub_url in res.url
                    ]

                    if ingest_resources:
                        ingest_res = ingest_resources[0]
                        send = False
                        
                        if not geoserver_resources:
                            send = True
                        else:
                            if [r for r in geoserver_resources if r.last_modified == ingest_res.last_modified]:
                                send = False
                            else:
                                geo_res = geoserver_resources[0] 
                                if ingest_res.last_modified > geo_res.last_modified:
                                    send = True

                        if send:
                            try:
                                jobs.enqueue(run_ingestor,
                                        kwargs={'pkg_id': entity.id},
                                        rq_kwargs={'timeout': 1000})
                                h.flash_success(
                                    f"Send {entity.id} for ingesting.")
                            except Exception as e:
                                h.flash_error(f"{e}")


_stat_fq = {
    "api": "res_extras_datastore_active:true OR res_format:WMS",
    "open": "isopen:true",
    "unpublished": "unpublished:true",
}


def _dga_stat_group_to_fq(group: str) -> str:
    return _stat_fq.get(group, "*:*")
