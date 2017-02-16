import logging
import re
import sys

import psycopg2
from ckan.lib.cli import CkanCommand

log = logging.getLogger('ckanext_datagovau')


# No other CKAN imports allowed until _load_config is run,
# or logging is disabled

def get_db_cursor(data):
    db_port = None
    if data.get('db_port', '') != '':
        db_port = data['db_port']

    try:
        connection = psycopg2.connect(dbname=data['db_name'],
                                      user=data['db_user'],
                                      password=data['db_pass'],
                                      host=data['db_host'],
                                      port=db_port)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return connection.cursor(), connection
    except Exception, e:
        log.error("Unable to get DB connection")


class ReconcileGeoserverAndDatastore(CkanCommand):
    """ Cleans out old what old spatial ingestor did

    Usage (all packages): paster purgelegacyspatial

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        from datetime import datetime
        from pylons import config
        import requests

        from ckan import model
        from ckan.lib import cli, search
        from ckan.plugins import toolkit

        geoserver_info = cli.parse_db_config('ckanext.datagovau.geoserver.url')
        postgis_info = cli.parse_db_config('ckanext.datagovau.postgis.url')
        datastore_info = cli.parse_db_config('ckanext.datagovau.datastore.url')

        active_datastore_tablenames = set()
        active_geoserver_workspaces = set()

        dry_run = (len(self.args) == 1 and self.args[0].lower() == "dry-run")
        clean_all = (len(self.args) == 1 and self.args[0].lower() == "clean-all")
        clean_dbs = clean_all or (len(self.args) == 1 and self.args[0].lower() == "clean-dbs-only")
        clean_geoserver = clean_all or (len(self.args) == 1 and self.args[0].lower() == "clean-geoserver-only")
        clean_ckan_resources = clean_all or (len(self.args) == 1 and self.args[0].lower() == "clean-ckan-only")

        sys.stdout.write("\n----------")
        if all([not x for x in [dry_run, clean_all, clean_dbs, clean_geoserver, clean_ckan_resources]]):
            sys.stdout.write("\nUsage:")
            sys.stdout.write("\n         paster --plugin=ckanext-datagovau cleanupdatastoregeoserver <command> --config=/path/to/config.ini")
            sys.stdout.write("\nCommands:")
            sys.stdout.write("\n         help: This message")
            sys.stdout.write("\n         dry-run: Run script to generate a command line report with no actions taken")
            sys.stdout.write("\n         clean-all: Run script to clean out unused Geoserver workspaces, DB tables and CKAN Resources")
            sys.stdout.write("\n         clean-dbs-only: Run script to clean out only unused DB tables")
            sys.stdout.write("\n         clean-geoserver-only: Run script to clean out only unused Geoserver workspaces")
            sys.stdout.write("\n         clean-ckan-only: Run script to clean out only unused CKAN resources")
            sys.stdout.write("\nNote: Supply no command or anything other than the above will have the system default to 'help'\n")
            sys.exit(0)

        if clean_all:
            sys.stdout.write("\nRunning in 'clean-all' Mode - Will Delete DB Tables, Geoserver Workspaces and CKAN Resources")
        elif clean_dbs:
            sys.stdout.write("\nRunning in 'clean-dbs-only' Mode - Will Delete DB Tables Only")
        elif clean_geoserver:
            sys.stdout.write("\nRunning in 'clean-geoserver-only' Mode - Will Delete Geoserver Workspaces Only")
        elif clean_ckan_resources:
            sys.stdout.write("\nRunning in 'clean-ckan-only' Mode - Will Delete CKAN Resources Only")
        else:
            sys.stdout.write("\nRunning in 'dry-run' Mode - Will Generate A Report Only; No Assets Will Be Deleted")


        # Get datastore tables
        sys.stdout.write("\n----------")
        sys.stdout.write("\nFetching All Datastore Table Names...")
        cursor, connection = get_db_cursor(datastore_info)
        cursor.execute("""select
                            c.relname
                          from
                            pg_class c join pg_roles r on r.oid = c.relowner
                          where
                            r.rolname = '{0}' and c.relkind = 'r'
                          order by
                            c.relname""".format(datastore_info['db_user']))
        datastore_tables = [r[0] for r in cursor.fetchall()]
        connection.commit()
        connection.close()
        for sp_tab in ['spatial_ref_sys']:
            if sp_tab in datastore_tables:
                datastore_tables.remove(sp_tab)
        sys.stdout.write(" Found {0} Tables".format(len(datastore_tables)))

        # Get postGIS tables
        if config.get('ckan.datastore_url') == config.get('ckan.postgis_url'):
            postgis_tables = datastore_tables
        else:
            sys.stdout.write("\n----------")
            sys.stdout.write("\nFetching All Geoserver PostGIS Table Names...")
            cursor, connection = get_db_cursor(postgis_info)
            cursor.execute("""select
                                c.relname
                              from
                                pg_class c join pg_roles r on r.oid = c.relowner
                              where
                                r.rolname = '{0}' and c.relkind = 'r'
                              order by
                                c.relname""".format(datastore_info['db_user']))
            postgis_tables = [r[0] for r in cursor.fetchall()]
            connection.commit()
            connection.close()
            for sp_tab in ['spatial_ref_sys']:
                if sp_tab in postgis_tables:
                    postgis_tables.remove(sp_tab)
            sys.stdout.write(" Found {0} Tables".format(len(postgis_tables)))

        # Get geoserver workspaces
        geoserver_url = 'http://' + geoserver_info['db_host']
        if geoserver_info.get('db_port', '') != '':
            geoserver_url += ':' + geoserver_info['db_port']
        geoserver_url += '/' + geoserver_info['db_name'] + '/'
        sys.stdout.write("\n----------")
        sys.stdout.write("\nFetching All Geoserver Workspace Names From {0}...".format(geoserver_url))
        geoserver_credentials = (geoserver_info['db_user'], geoserver_info['db_pass'])
        geoserver_headers = {'Accept': 'application/json'}
        res = requests.get(geoserver_url + 'rest/workspaces', headers=geoserver_headers, auth=geoserver_credentials)
        geoserver_workspaces = [r['name'] for r in res.json()['workspaces']['workspace']]
        sys.stdout.write(" Found {0} Workspaces".format(len(geoserver_workspaces)))

        # Test workspace structures in geoserver
        active_geoserver_featuretypes = set()
        total = len(geoserver_workspaces)
        sys.stdout.write("\n----------")
        sys.stdout.write("\nTesting Geoserver Workspace Structures 0/0")
        filestore_workspaces = set()
        fts_without_table = set()
        ws_without_table = set()
        for counter, ws in enumerate(geoserver_workspaces):
            try:
                sys.stdout.write("\rTesting Geoserver Workspace Structures {0}/{1}".format(counter + 1, total))
                sys.stdout.flush()
                res = requests.get(geoserver_url + 'rest/workspaces/' + ws + '/datastores', headers=geoserver_headers,
                                   auth=geoserver_credentials)
                datastores = [r['name'] for r in res.json()['dataStores']['dataStore']]
                have_table = True
                for ds in datastores:
                    res = requests.get(geoserver_url + 'rest/workspaces/' + ws + '/datastores/' + ds + '/featuretypes',
                                       headers=geoserver_headers, auth=geoserver_credentials)

                    for ft in [r['name'] for r in res.json()['featureTypes']['featureType']]:
                        if re.search('ckan\_(.*)', ft):
                            ft = re.search('ckan\_(.*)', ft).group(1)
                        if ft in postgis_tables:
                            active_geoserver_featuretypes.add(ft)
                        else:
                            have_table = False
                            fts_without_table.add(ft)
                if not have_table:
                    ws_without_table.add(ws)
            except:
                filestore_workspaces.add(ws)

        ft_without_table_counter = len(fts_without_table)
        ws_without_table_counter = len(ws_without_table)
        filestore_counter = len(filestore_workspaces)

        sys.stdout.write("\nGeoserver Raster Workspaces: {0}".format(filestore_counter))
        sys.stdout.write("\nGeoserver Vector Workspaces: {0}".format(total - filestore_counter))
        sys.stdout.write("\nGeoserver Vector Workspaces Without PostGIS Tables: {0}".format(ws_without_table_counter))
        sys.stdout.write("\nGeoserver Feature Types Without PostGIS Tables: {0}".format(ft_without_table_counter))

        sys.stdout.write("\n----------")
        sys.stdout.write("\nFetching All Active Package IDs...")
        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).filter_by(state='active').all()]

        total = len(pkg_ids)
        sys.stdout.write(" Found {0} Package IDs".format(total))
        sys.stdout.write("\nProcessing Package ID 0/0")
        resources_to_delete = set()
        active_filestore_workspaces = set()
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write("\rProcessing Package ID {0}/{1}".format(counter + 1, total))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            for res_dict in pkg_dict.get('resources', []):
                res_delete = False
                ws_name = None
                if 'data.gov.au/geoserver' in res_dict.get('url', ''):
                    re_res = re.search('.*data\.gov\.au\/geoserver\/(.*)\/.*', res_dict['url'])
                    if re_res:
                        ws_name = re_res.group(1)
                        if (ws_name in ws_without_table) or not (ws_name in geoserver_workspaces):
                            resources_to_delete.add(res_dict['id'])
                            res_delete = True
                        elif ws_name in filestore_workspaces:
                            active_filestore_workspaces.add(ws_name)
                        else:
                            active_geoserver_workspaces.add(ws_name)

                if not res_delete:
                    new_label = ''
                    if toolkit.asbool(res_dict.get('datastore_active', 'False')):
                        if res_dict['id'] in datastore_tables:
                            active_datastore_tablenames.add(res_dict['id'])
                        else:
                            new_label = 'False'
                    elif res_dict['id'] in datastore_tables:
                        new_label = 'True'

                    if new_label != '':
                        new_label_bool = toolkit.asbool(new_label)
                        sys.stdout.write("\nCorrecting 'datastore_active' Value For {0}".format(res_dict['id']))
                        try:
                            res_dict['datastore_active'] = new_label_bool
                            if clean_ckan_resources:
                                toolkit.get_action('resource_update')({'ignore_auth': True}, res_dict)
                            active_datastore_tablenames.add(res_dict['id'])
                        except:
                            if new_label_bool:
                                sys.stdout.write(
                                    "\nResource {0} Failed Validation; Will Drop Associated Datastore Table".format(
                                        res_dict['id']))
                            else:
                                sys.stdout.write(
                                    "\nResource {0} Failed Validation; Marking Resource For Deletion".format(
                                        res_dict['id']))
                                resources_to_delete.add(res_dict['id'])
                                if ws_name:
                                    if ws_name in active_filestore_workspaces:
                                        active_filestore_workspaces.remove(ws_name)
                                    elif ws_name in active_geoserver_workspaces:
                                        active_geoserver_workspaces.remove(ws_name)
                        sys.stdout.write("\nProcessing Package ID {0}/{1}".format(counter + 1, total))

        ds_counter = len(active_datastore_tablenames)
        delete_counter = len(resources_to_delete)
        active_filestore_counter = len(active_filestore_workspaces)
        geo_counter = len(active_geoserver_workspaces)

        sys.stdout.write("\nActive Datastore Resources: {0}".format(ds_counter))
        sys.stdout.write("\nActive Geoserver Resources: {0}.".format(geo_counter + active_filestore_counter))
        sys.stdout.write("\nActive Geoserver Rasterized Resources: {0}".format(active_filestore_counter))
        sys.stdout.write("\nActive Geoserver Vector Resources: {0}".format(geo_counter))
        sys.stdout.write("\nActive Geoserver Resources With Broken Workspaces: {0}".format(delete_counter))

        # Extract feature types for active geoserver workspaces
        active_geoserver_featuretypes = set()
        total = len(active_geoserver_workspaces)
        sys.stdout.write("\n----------")
        sys.stdout.write("\nExtracting PostGIS Table Name From Geoserver Workspace 0/0")
        for counter, ws in enumerate(active_geoserver_workspaces):
            sys.stdout.write(
                "\rExtracting PostGIS Table Name From Geoserver Workspace {0}/{1}".format(counter + 1, total))
            sys.stdout.flush()
            res = requests.get(geoserver_url + 'rest/workspaces/' + ws + '/datastores', headers=geoserver_headers,
                               auth=geoserver_credentials)
            datastores = [r['name'] for r in res.json()['dataStores']['dataStore']]
            for ds in datastores:
                res = requests.get(geoserver_url + 'rest/workspaces/' + ws + '/datastores/' + ds + '/featuretypes',
                                   headers=geoserver_headers, auth=geoserver_credentials)
                for ft in [r['name'] for r in res.json()['featureTypes']['featureType']]:
                    if re.search('ckan\_(.*)', ft):
                        ft = re.search('ckan\_(.*)', ft).group(1)
                    active_geoserver_featuretypes.add(ft)

        postgis_tables_to_drop = []
        sys.stdout.write("\n----------")
        if config.get('ckan.datastore_url') == config.get('ckan.postgis_url'):
            sys.stdout.write("\nFinding Active Geoserver Feature Types Without A PostGIS Table...")
            geoserver_featuretypes_without_table = active_geoserver_featuretypes - set(datastore_tables)
            sys.stdout.write(" Found {0} Feature Types".format(len(geoserver_featuretypes_without_table)))

            sys.stdout.write("\nDetermining Which Datastore Tables To Drop...")
            valid_table_names = set(active_datastore_tablenames) | active_geoserver_featuretypes
            datastore_tables_to_drop = list(set(datastore_tables) - valid_table_names)
            sys.stdout.write(
                " Will Drop {0} Out Of {1} Tables".format(len(datastore_tables_to_drop), len(datastore_tables)))
        else:
            sys.stdout.write("\nFinding Active Geoserver Feature Types Without A PostGIS Table...")
            geoserver_featuretypes_without_table = active_geoserver_featuretypes - set(postgis_tables)
            sys.stdout.write(" Found {0} Feature Types".format(len(geoserver_featuretypes_without_table)))

            sys.stdout.write("\nDetermining Which PostGIS Tables To Drop...")
            valid_table_names = active_geoserver_featuretypes
            postgis_tables_to_drop = list(set(postgis_tables) - valid_table_names)
            sys.stdout.write(
                " Will Drop {0} Out Of {1} Tables".format(len(postgis_tables_to_drop), len(postgis_tables)))

            sys.stdout.write("\nDetermining Which Datastore Tables To Drop...")
            valid_table_names = set(active_datastore_tablenames)
            datastore_tables_to_drop = list(set(datastore_tables) - valid_table_names)
            sys.stdout.write(
                " Will Drop {0} Out Of {1} Tables".format(len(datastore_tables_to_drop), len(datastore_tables)))

        sys.stdout.write("\nDetermining Which Geoserver Workspaces To Delete...")
        valid_workspaces = active_geoserver_workspaces | active_filestore_workspaces
        workspaces_to_delete = list(set(geoserver_workspaces) - valid_workspaces)
        sys.stdout.write(
            " Will Delete {0} Out Of {1} Workspaces".format(len(workspaces_to_delete), len(geoserver_workspaces)))

        sys.stdout.write("\n----------")
        sys.stdout.write(
            "\nTables To Be Dropped From Datastore DB ({0} Out Of {1}):".format(len(datastore_tables_to_drop),
                                                                                len(datastore_tables)))
        cursor, connection = get_db_cursor(datastore_info)
        for table_name in datastore_tables_to_drop:
            sys.stdout.write("\nDropping Table {0}".format(table_name))
            if clean_dbs:
                cursor.execute("""DROP TABLE "{0}" CASCADE""".format(table_name))
        connection.commit()
        connection.close()

        if config.get('ckan.datastore_url') != config.get('ckan.postgis_url'):
            sys.stdout.write("\n----------")
            sys.stdout.write(
                "\nTables To Be Dropped From PostGIS DB ({0} Out Of {1}):".format(len(postgis_tables_to_drop),
                                                                                  len(postgis_tables)))
            cursor, connection = get_db_cursor(postgis_info)
            for table_name in postgis_tables_to_drop:
                sys.stdout.write("\nDropping Table {0}".format(table_name))
                if clean_dbs:
                    cursor.execute("""DROP TABLE "{0}" CASCADE""".format(table_name))
            connection.commit()
            connection.close()

        sys.stdout.write("\n----------")
        sys.stdout.write("\nGeoserver Workspaces To Be Deleted ({0} Out Of {1}):".format(len(workspaces_to_delete),
                                                                                         len(geoserver_workspaces)))
        for ws_name in workspaces_to_delete:
            sys.stdout.write("\nRecursively Deleting Workspace {0}".format(ws_name))
            if clean_geoserver:
                requests.delete(geoserver_url + '/rest/workspaces/' + ws_name + '?recurse=true&quietOnNotFound',
                                auth=geoserver_credentials)

        sys.stdout.write("\n----------")
        sys.stdout.write("\nResources To Be Deleted From CKAN ({0}):".format(len(resources_to_delete)))
        for res_id in resources_to_delete:
            sys.stdout.write("\nDeleting Resource {0}".format(res_id))
            if clean_ckan_resources:
                try:
                    toolkit.get_action('resource_delete')({'ignore_auth': True}, {'id': res_id})
                except:
                    # Resource or package is failing a validation check, force the delete
                    del_dict = dict(state='deleted', last_modified=datetime.now())
                    res_dict = model.Resource.get(res_id).as_dict()
                    model.Session.query(model.Resource).filter_by(id=res_id).update(del_dict)
                    if 'package_id' in res_dict:
                        search.rebuild(res_dict['package_id'])

        sys.stdout.write("\n----------")
        sys.stdout.write("\nProcessing Complete!\n")
