import logging
import sys

from ckan.lib.cli import CkanCommand

import helpers as helpers

log = logging.getLogger('ckanext_datagovau')


# No other CKAN imports allowed until _load_config is run,
# or logging is disabled

def exec_processing(args, process, task_type):
    import pylons.config as config
    import ckan.model as model

    import ckan.plugins.toolkit as toolkit
    from lib.zip_core import purge_zip, rebuild_zip
    from lib.spatial_core import purge_spatial, rebuild_spatial

    if args:
        try:
            pkg = model.Package.get(args[0])
        except:
            pkg = None

        if pkg is None:
            print ("You have not specified a valid package name or id, aborting.")
            return

        pkg_id = pkg.id
    else:
        pkg_id = None

    if task_type == 'zipextractor':
        context = helpers.get_zip_context(config)
        msg2 = 'zip data'
    else:
        context = helpers.get_spatial_context(config)
        msg2 = 'spatial data'

    if process == 'purge':
        msg1 = 'Purging'
        msg3 = 'from'
        if task_type == 'zipextractor':
            process_func = purge_zip
        else:
            process_func = purge_spatial
    else:
        msg1 = 'Rebuilding'
        msg3 = 'for'
        if task_type == 'zipextractor':
            process_func = rebuild_zip
        else:
            process_func = rebuild_spatial

    if pkg_id:
        pkg_dict = toolkit.get_action('package_show')(context, {'id': pkg_id})
        log.info("{0} {1} {2} package {3}...".format(msg1, msg2, msg3, pkg_dict['name']))
        process_func(context, pkg_dict)
    else:
        context['model'] = model
        context['session'] = model.Session
        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).filter(model.Package.state != 'deleted').all()]
        log.info("{0} {1} {2} all packages...".format(msg1, msg2, msg3))

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write("\r{0} {1} {2} dataset {3}/{4}".format(msg1, msg2, msg3, counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                process_func(context, pkg_dict)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        sys.stdout.write("\n>>> Process complete\n")


class PurgeZip(CkanCommand):
    """ Purges ZIP child resources from a package or all packages.

    Usage (single package): paster purgezip <package_id>
    Usage (all packages): paster purgezip

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        exec_processing(self.args, 'purge', 'zipextractor')


class PurgeSpatial(CkanCommand):
    """ Purges Spatial child resources from a package or all packages.

    Usage (single package): paster purgespatial <package_id>
    Usage (all packages): paster purgespatial

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        exec_processing(self.args, 'purge', 'spatialingestor')


class RebuildZip(CkanCommand):
    """ Rebuild ZIP child resources from a package or all packages.

    Usage (single package): paster rebuildzip <package_id>
    Usage (all packages): paster rebuildzip

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        exec_processing(self.args, 'rebuild', 'zipextractor')


class RebuildSpatial(CkanCommand):
    """ Rebuilds Spatial child resources from a package or all packages.

    Usage (single package): paster rebuildspatial <package_id>
    Usage (all packages): paster rebuildspatial

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        exec_processing(self.args, 'rebuild', 'spatialingestor')


class CleanDatastore(CkanCommand):
    """ Calls datastore delete on all deleted resources that are of
    url_type 'datastore' or 'datapusher'.

    Usage (all packages): paster cleandatastore

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def command(self):
        self._load_config()

        import pylons.config as config
        import ckan.model as model

        from ckanext.datastore.logic.action import datastore_delete

        context = helpers.get_zip_context(config)

        context['model'] = model
        context['session'] = model.Session
        res_ids = [r[0] for r in
                   model.Session.query(model.Resource.id).filter(model.Resource.state == 'deleted').all()]
        log.info("Cleaning datastore tables for all deleted resources...")

        total_resources = len(res_ids)
        for counter, res_id in enumerate(res_ids):
            sys.stdout.write("\rCleaning tables for deleted resource {0}/{1}".format(counter + 1, total_resources))
            sys.stdout.flush()
            res_dict = model.Resource.get(res_id).as_dict()
            try:
                datastore_delete(context, res_dict)
            except Exception, e:
                pass

        sys.stdout.write("\n>>> Process complete\n")


class PurgeLegacySpatial(CkanCommand):
    """ Cleans out old what old spatial ingestor did

    Usage (all packages): paster purgelegacyspatial

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def command(self):
        self._load_config()

        import pylons.config as config
        from ckanext.datagovau.lib.spatial_core import _get_db_cursor
        from ckanext.datastore.db import _get_engine
        import requests
        import sqlalchemy
        import ckan.model as model
        import ckan.plugins.toolkit as toolkit

        context = helpers.get_spatial_context(config)
        context['model'] = model
        context['session'] = model.Session

        def process_pkg(pkg_dict):

            pkg_sql = sqlalchemy.text(u'''SELECT 1 FROM "_table_metadata"
                                                        WHERE name = :id AND alias_of IS NULL''')
            results = _get_engine({
                'connection_url': config['ckan.datastore.write_url']
            }).execute(pkg_sql, id=pkg_dict['id'].replace('-', '_'))
            res_exists = results.rowcount > 0

            if res_exists:
                log.debug("{0} appears to contain a legacy spatial ingestion.".format(pkg_dict['name']))
                # We have a table that exists in the PostGIS DB
                pkg_raw = model.Package.get(pkg_id)

                if pkg_raw.state != 'deleted':
                    for res_raw in pkg_raw.resources:
                        res_dict = res_raw.as_dict()
                        if "http://data.gov.au/geoserver/" in res_dict.get('url', ''):
                            toolkit.get_action('resource_delete')(context, res_dict)
                        elif helpers.is_spatial_resource(res_dict):
                            res_dict['spatial_parent_legacy'] = 'True'
                            toolkit.get_action('resource_update')(context, res_dict)

                credentials = (context['geoserver']['db_user'], context['geoserver']['db_pass'])
                context['geoserver_internal_url'] = 'http://' + context['geoserver']['db_host']
                wsurl = context['geoserver_internal_url'] + 'rest/workspaces'
                workspace = pkg_dict['name']

                res = requests.delete(wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', auth=credentials)

                log.error("Geoserver recursive workspace deletion returned {0}".format(res))

                table_name = pkg_dict['id'].replace("-", "_")

                res = _get_db_cursor(context, pkg_dict)

                if res is None:
                    log.error("Failed to open SQL connection for {0}".format(table_name))
                    return res

                cursor, connection = res

                cursor.execute("DROP TABLE IF EXISTS {tab_name}".format(tab_name=table_name))
                cursor.close()
                connection.close()

                log.error("Dropped SQL table {0}".format(table_name))

        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).all()]
        log.info("Migrating legacy spatial ingestion on all packages...")

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write("\rProcessing dataset {0}/{1}".format(counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                process_pkg(pkg_dict)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        sys.stdout.write("\n>>> Process complete\n")
