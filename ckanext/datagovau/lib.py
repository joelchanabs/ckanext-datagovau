import os
from osgeo import osr
import sys
import uuid
import json
import shutil
import zipfile
import requests
import urllib
import psycopg2
import logging
from datetime import datetime
import ckan.plugins.toolkit as tk
import lxml.etree as et

log = logging.getLogger('ckanext_datagovau')

from subprocess import call

MSG_SPATIAL_PREFIX = 'Spatial Ingestor:'
MSG_SPATIAL_SKIP_SUFFIX = 'skipping spatial ingestion.'
MSG_ZIP_PREFIX = 'Zip Extractor:'
MSG_ZIP_SKIP_SUFFIX = 'skipping Zip extraction.'


def load_config(ckan_ini_filepath):
    import paste.deploy
    config_abs_path = os.path.abspath(ckan_ini_filepath)
    conf = paste.deploy.appconfig('config:' + config_abs_path)
    import ckan
    ckan.config.environment.load_environment(conf.global_conf,
                                             conf.local_conf)


def register_translator():
    # Register a translator in this thread so that
    # the _() functions in logic layer can work
    from paste.registry import Registry
    from pylons import translator
    from ckan.lib.cli import MockTranslator
    global registry
    registry = Registry()
    registry.prepare()
    global translator_obj
    translator_obj = MockTranslator()
    registry.register(translator, translator_obj)


def _init(raw_context):
    load_config(raw_context['config_file_path'])
    register_translator()

    from ckan import model

    # Complete the context
    raw_context['model'] = model
    raw_context['session'] = model.Session
    raw_context['ignore_auth'] = True

    if not os.path.isdir(raw_context['temporary_directory']):
        try:
            os.makedirs(raw_context['temporary_directory'])
        except:
            log.error(
                "Failed to create temporary directory {0}, skipping...".format(raw_context['temporary_directory']))
            return None

    return raw_context


def _get_db_cursor(db_settings):
    db_port = None
    if db_settings.get('db_port', '') != '':
        db_port = db_settings['db_port']

    try:
        connection = psycopg2.connect(dbname=db_settings['db_name'],
                                      user=db_settings['db_user'],
                                      password=db_settings['db_pass'],
                                      host=db_settings['db_host'],
                                      port=db_port)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return connection.cursor(), connection
    except Exception, e:
        log.error("{0} failed to connect with PostGIS with error {1}, {2}".format(MSG_SPATIAL_PREFIX, str(e),
                                                                                  MSG_SPATIAL_SKIP_SUFFIX))
        return None


def _setup_spatial_table(context, resource):
    res = _get_db_cursor(context['postgis'])

    if res is None:
        return res

    cursor, connection = res

    table_name = "sp_" + resource['id'].replace("-", "_")

    cursor.execute("DROP TABLE IF EXISTS {tab_name}".format(tab_name=table_name))
    cursor.close()
    connection.close()

    return table_name


def _check_blacklists(context, package):
    message = ''
    if package['organization']['name'] in context['org_blacklist']:
        message = "{0} {1} in organization blacklist, {2}".format(
            MSG_SPATIAL_PREFIX, package['organization']['name'], MSG_SPATIAL_SKIP_SUFFIX)
    elif package['name'] in context['pkg_blacklist']:
        message = "{0} {1} in package blacklist, {2}".format(
            MSG_SPATIAL_PREFIX, package['name'], MSG_SPATIAL_SKIP_SUFFIX)
    else:
        activity_list = tk.get_action('package_activity_list')(context, {
            'id': package['id'],
        })
        if activity_list[0]['user_id'] in context['user_blacklist']:
            message = "{0} {1} was last edited by blacklisted user, {2}".format(
                MSG_SPATIAL_PREFIX, activity_list[0]['user_id'], MSG_SPATIAL_SKIP_SUFFIX)

    return message


def _get_input_format(resource):
    check_string = (resource['format'] + resource['url']).upper()
    if any(x in check_string for x in ["SHP", "SHAPEFILE"]):
        return 'SHP'
    elif "KML" in check_string:
        return 'KML'
    elif "KMZ" in check_string:
        return 'KMZ'
    elif "GRID" in check_string:
        return 'GRID'
    else:
        return None


def _get_upload_formats(context, package, parent_resource, input_format):
    result = []
    for res_format in context['target_spatial_formats']:
        # Obviously do not expand into our own format
        if res_format == input_format:
            continue

        target_id = None
        for resource in package['resources']:
            test_string = (resource['format'] + resource['url']).upper()
            if res_format in test_string or all(["JSON" in x for x in [res_format, test_string]]):
                # Format is previously existing, see if the spatial ingestor was the last editor
                if resource['spatial_child_of'] == parent_resource['id']:
                    # Found resource previously created by ingestor, we add its list to the ids to be modified
                    target_id = resource['id']
                    break

        result += [(res_format, target_id)]

    return result


def _db_upload(context, url, table_name, input_format):
    def download_file(resource_url, file_format):
        if 'SHP' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'shp.zip')
        elif 'KML' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'kml')
        elif 'KMZ' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'kml.zip')
        elif 'GRID' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'zip')

        response = requests.get(resource_url, stream=True)

        if response.status_code != 200:
            log.error("{0} {1} could not be downloaded, {2}".format(MSG_SPATIAL_PREFIX, resource_url,
                                                                    MSG_SPATIAL_SKIP_SUFFIX))
            return None

        with open(os.path.join(context['temporary_directory'], tmpname), 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)

        return tmpname

    def unzip_file(filepath):
        try:
            z = zipfile.ZipFile(os.path.join(context['temporary_directory'], filepath))
        except zipfile.BadZipfile as e:
            log.error(
                "{0} {1} is not a valid zip file, {2}".format(MSG_SPATIAL_PREFIX,
                                                              os.path.join(context['temporary_directory'], filepath),
                                                              MSG_SPATIAL_SKIP_SUFFIX))
            return None

        # Take only the filename before the extension
        dirname = os.path.join(context['temporary_directory'], filepath.split('.', 1)[0])

        os.makedirs(dirname)
        for name in z.namelist():
            z.extract(name, dirname)

        return dirname

    def db_ingest(full_file_path, crs='EPSG:4326'):
        # Use ogr2ogr to process the KML into the postgis DB
        port_string = ''
        if 'db_port' in context['postgis']:
            port_string = '\' port=\'' + context['postgis']['db_port']

        args = ['ogr2ogr', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
                'PG:dbname=\'' + context['postgis']['db_name'] + '\' host=\'' + context['postgis'][
                    'db_host'] + port_string + '\' user=\'' + context['postgis'][
                    'db_user'] + '\' password=\'' + context['postgis']['db_pass'] + '\'', full_file_path, '-lco',
                'GEOMETRY_NAME=geom', "-lco", "PRECISION=NO", '-nln', table_name, '-a_srs', crs,
                '-nlt', 'PROMOTE_TO_MULTI', '-overwrite']

        return call(args)

    native_crs = "EPSG:4326"
    unzip_dir = None
    base_file = download_file(url, input_format)

    # Did we not manage to download anything?
    if base_file is None:
        return None

    # Do we need to unzip?
    if input_format in ["KMZ", "SHP", "GRID"]:
        unzip_dir = unzip_file(base_file)

        # File is unzipped, no need to keep the compressed version
        os.remove(os.path.join(context['temporary_directory'], base_file))

        if unzip_dir is None:
            return None

    if input_format in ["KMZ", "KML", "GRID"]:
        if unzip_dir is not None:
            kml_file = None
            for f in os.listdir(unzip_dir):
                if f.lower().endswith(".kml"):
                    kml_file = f

            if kml_file is None:
                log.error("{0} No KML file found in archive: {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                              MSG_SPATIAL_SKIP_SUFFIX))
                shutil.rmtree(unzip_dir)
                return None
        else:
            kml_file = base_file

        # Update folder name in KML file with table_name
        tree = et.parse(kml_file)
        for ns in ['http://www.opengis.net/kml/2.2', 'http://earth.google.com/kml/2.1']:
            find = et.ETXPath('//{' + ns + '}Folder/{' + ns + '}name')
            element = find(tree)
            for x in element:
                x.text = table_name

        # Clean up temporary files
        if unzip_dir is not None:
            shutil.rmtree(unzip_dir)
        else:
            os.remove(os.path.join(context['temporary_directory'], base_file))

        # Write new KML file
        kml_file_new = os.path.join(context['temporary_directory'], table_name + ".kml")
        with open(kml_file_new, 'w') as out_file:
            out_file.write(et.tostring(tree))

        # Use ogr2ogr to process the KML into the postgis DB
        return_code = db_ingest(kml_file_new, native_crs)

        # Remove edited KML file
        os.remove(kml_file_new)

        if return_code == 1:
            log.error(
                "{0} {1} could not be converted, {2}".format(MSG_SPATIAL_PREFIX, kml_file_new, MSG_SPATIAL_SKIP_SUFFIX))
            return None

    elif input_format == "SHP":

        shp_file = None
        prj_file = None
        for f in os.listdir(unzip_dir):
            if f.lower().endswith(".shp"):
                shp_file = f

            if f.lower().endswith(".prj"):
                prj_file = f

        if shp_file is None:
            log.error("{0} No shapefile found in archive: {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                           MSG_SPATIAL_SKIP_SUFFIX))
            shutil.rmtree(unzip_dir)
            return None

        file_path = os.path.join(unzip_dir, shp_file)

        # Determine projection information
        if prj_file:
            prj_txt = open(os.path.join(unzip_dir, prj_file), 'r').read()
            sr = osr.SpatialReference()
            sr.ImportFromESRI([prj_txt])
            res = sr.AutoIdentifyEPSG()
            if res == 0:  # Successful auto-identify
                native_crs = sr.GetAuthorityName(None) + ":" + sr.GetAuthorityCode(None)
                log.info(
                    "{0} successfully identified projection of {1} as {2}".format(MSG_SPATIAL_PREFIX, shp_file,
                                                                                  native_crs))
            elif any(x in prj_txt for x in ["GDA_1994_MGA_Zone_56", "GDA94_MGA_zone_56"]):
                native_crs = "EPSG:28356"
            elif any(x in prj_txt for x in ["GDA_1994_MGA_Zone_55", "GDA94_MGA_zone_55"]):
                native_crs = "EPSG:28355"
            elif any(x in prj_txt for x in ["GDA_1994_MGA_Zone_54", "GDA94_MGA_zone_54"]):
                native_crs = "EPSG:28354"
            elif "GCS_GDA_1994" in prj_txt:
                native_crs = "EPSG:4283"
            elif 'GEOGCS["GDA94",DATUM["D_GDA_1994",SPHEROID["GRS_1980"' in prj_txt:
                native_crs = "EPSG:4283"
            elif "MapInfo Generic Lat/Long" in prj_txt:
                native_crs = "EPSG:4326"
            elif "Asia_South_Equidistant_Conic" in prj_txt:
                native_crs = "ESRI:102029"
            elif "Australian_Albers_Equal_Area_Conic_WGS_1984" in prj_txt:
                native_crs = "EPSG:3577"
            elif "WGS_1984_Web_Mercator_Auxiliary_Sphere" in prj_txt:
                native_crs = "EPSG:3857"
            elif 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984"' in prj_txt:
                native_crs = "EPSG:4326"
            else:
                log.error("{0} {1} has unknown projection: {2}, {3}".format(MSG_SPATIAL_PREFIX, file_path, prj_txt,
                                                                            MSG_SPATIAL_SKIP_SUFFIX))
                shutil.rmtree(unzip_dir)
                return None

        # Use ogr2ogr to process the shapefile into the postgis DB
        return_code = db_ingest(file_path, native_crs)

        shutil.rmtree(unzip_dir)

        if return_code == 1:
            log.error(
                "{0} {1} could not be converted, {2}".format(MSG_SPATIAL_PREFIX, file_path, MSG_SPATIAL_SKIP_SUFFIX))
            return None
    else:
        # Should never get here
        log.error(
            "{0} {1} unrecognized for DB upload, {2}".format(MSG_SPATIAL_PREFIX, input_format, MSG_SPATIAL_SKIP_SUFFIX))
        return None

    return native_crs


def _geoserver_transfer(context, input_format, table_name, native_crs, dataset, resource):
    # Call to postgis to determine bounding box
    conn_params = _get_db_cursor(context['postgis'])

    if conn_params is None:
        return None

    (db_cursor, db_connection) = conn_params
    if input_format in ['KML', 'KMZ', 'GRID']:
        try:
            db_cursor.execute("ALTER TABLE {tab_name} "
                              "DROP \"description\" RESTRICT, "
                              "DROP \"timestamp\" RESTRICT, "
                              "DROP \"begin\" RESTRICT, "
                              "DROP \"end\" RESTRICT, "
                              "DROP altitudemode RESTRICT, "
                              "DROP tessellate RESTRICT, "
                              "DROP extrude RESTRICT, "
                              "DROP visibility RESTRICT, "
                              "DROP draworder RESTRICT, "
                              "DROP icon RESTRICT;".format(tab_name=table_name))
        except Exception, e:
            log.error(
                "{0} failed to alter KML PostGIS table with exception: {1}, continuing...".format(MSG_SPATIAL_PREFIX,
                                                                                                  str(e)))

    # Pull out data from PostGIS as GeoJSON, along with bounding box. Note we extract the data in a
    # "EPSG:4326" native SRS
    try:
        db_cursor.execute("SELECT ST_Extent(geom) AS box,"
                          "ST_Extent(ST_Transform(geom,4326)) AS latlngbox, "
                          "ST_AsGeoJSON(ST_Extent(ST_Transform(geom,4326))) AS geojson "
                          "FROM {tab_name}".format(tab_name=table_name))
        (bbox, latlngbbox, bgjson) = db_cursor.fetchone()
        db_cursor.close()
        db_connection.close()
    except Exception, e:
        log.error("{0} failed to extract data from PostGIS with exception: {1}, {2}".format(MSG_SPATIAL_PREFIX, str(e),
                                                                                            MSG_SPATIAL_SKIP_SUFFIX))
        return None

    # Construct geoserver url & name core
    context['geoserver_internal_url'] = 'http://' + context['geoserver']['db_host']
    if context['geoserver'].get('db_port', '') != '':
        context['geoserver_internal_url'] += ':' + context['geoserver']['db_port']
        context['geoserver_internal_url'] += '/' + context['geoserver']['db_name'] + '/'

    core_name = dataset['name'] + "_" + resource['id']
    headers = {'Content-type': 'application/json'}
    credentials = (context['geoserver']['db_user'], context['geoserver']['db_pass'])

    # Create a workspace metadata
    workspace = core_name + "_ws"
    wsurl = context['geoserver_internal_url'] + 'rest/workspaces'
    wsdata = json.dumps({'workspace': {'name': workspace}})


    # Create datastore metadata
    datastore = core_name + '_ds'
    dsurl = context['geoserver_internal_url'] + 'rest/workspaces/' + workspace + '/datastores'
    dsdata = json.dumps({'dataStore': {'name': datastore,
                                       'connectionParameters': {
                                           "dbtype": "postgisng",
                                           "encode functions": "false",
                                           "jndiReferenceName": "java:comp/env/jdbc/postgres",
                                           "Support on the fly geometry simplification": "true",
                                           "Expose primary keys": "false",
                                           "Estimated extends": "false"
                                       }}})

    # Create layer metadata
    layer = core_name + "_ft"
    fturl = context[
                'geoserver_internal_url'] + 'rest/workspaces/' + workspace + '/datastores/' + datastore + "/featuretypes"
    ftdata = {'featureType': {'name': layer, 'nativeName': table_name, 'title': dataset['title']}}

    bbox_obj = None
    if bbox:
        (minx, miny, maxx, maxy) = bbox.replace("BOX", "").replace("(", "").replace(")", "").replace(",",
                                                                                                     " ").split(" ")
        bbox_obj = {'minx': minx, 'maxx': maxx, 'miny': miny, 'maxy': maxy}
        (llminx, llminy, llmaxx, llmaxy) = latlngbbox.replace("BOX", "").replace("(", "").replace(")", "").replace(
            ",", " ").split(" ")
        llbbox_obj = {'minx': llminx, 'maxx': llmaxx, 'miny': llminy, 'maxy': llmaxy}

        ftdata['featureType']['nativeBoundingBox'] = bbox_obj
        ftdata['featureType']['latLonBoundingBox'] = llbbox_obj

        if float(llminx) < -180 or float(llmaxx) > 180:
            log.error("{0} {1} has invalid automatic projection {3}, {4}".format(MSG_SPATIAL_PREFIX, dataset['title'],
                                                                                 native_crs, MSG_SPATIAL_SKIP_SUFFIX))
            return None
        else:
            ftdata['featureType']['srs'] = native_crs
            if 'spatial' not in dataset or dataset['spatial'] != bgjson:
                dataset['spatial'] = bgjson
                tk.get_action('package_update')(context, dataset)

    ftdata = json.dumps(ftdata)

    # Remove any pre-existing geoserver assets
    log.info("{0} removing any pre-existing geoserver assets...".format(MSG_SPATIAL_PREFIX))

    # Manually add geoserver parameters here as requests does not hangle parameters without values
    # https://github.com/kennethreitz/requests/issues/2651
    res = requests.delete(wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', auth=credentials)

    log.info("{0} geoserver {1} recursive deletion returned {2}".format(MSG_SPATIAL_PREFIX, wsurl + '/' + workspace, res))

    # Upload new geoserver assets
    log.info("{0} uploading new assets to geoserver...".format(MSG_SPATIAL_PREFIX))

    res = requests.post(wsurl, data=wsdata, headers=headers, auth=credentials)

    log.info("{0} geoserver workspace creation returned {1}".format(MSG_SPATIAL_PREFIX, res))

    res = requests.post(dsurl, data=dsdata, headers=headers, auth=credentials)

    log.info("{0} geoserver datastore creation returned {1}".format(MSG_SPATIAL_PREFIX, res))

    res = requests.post(fturl, data=ftdata, headers=headers, auth=credentials)

    log.info("{0} feature type creation returned {1}".format(MSG_SPATIAL_PREFIX, res))

    return workspace, layer, bbox_obj


def _create_or_update_resources(context, expansion_formats, workspace, layer, bbox_obj, dataset, parent_resource):
    ws_addr = context['geoserver_public_url'] + "/" + workspace + "/"
    current_time = datetime.now().isoformat()

    number_updated = 0

    for new_format, old_id in expansion_formats:
        number_updated += 1

        resource_command = 'resource_create'
        new_res = {'package_id': dataset['id'],
                   'format': new_format.lower(),
                   'last_modified': current_time,
                   'spatial_child_of': parent_resource['id'],
                   'parent_resource_url': parent_resource['url']}

        if new_res['format'] == 'json':
            new_res['format'] = 'geojson'

        if old_id is not None:
            resource_command = 'resource_update'
            new_res['id'] = old_id

        if new_format in ["IMAGE/PNG", 'KML']:
            new_res['url'] = ws_addr + "wms?request=GetMap&layers=" + layer + "&bbox=" + bbox_obj['minx'] + "," + \
                             bbox_obj['miny'] + "," + bbox_obj['maxx'] + "," + bbox_obj[
                                 'maxy'] + "&width=512&height=512&format=" + urllib.quote(new_format.lower())
            if new_format == "IMAGE/PNG":
                new_res['name'] = dataset['title'] + " Preview Image"
                new_res['description'] = "View overview image of this dataset"
            elif new_format == "KML":
                new_res['name'] = dataset['title'] + " KML"
                new_res[
                    'description'] = "View a map of this dataset in web and desktop spatial data tools including Google Earth"
        elif new_format == "WMS":
            new_res['url'] = ws_addr + "wms?request=GetCapabilities"
            new_res['name'] = dataset['title'] + " - Preview this Dataset (WMS)"
            new_res['description'] = "View the data in this dataset online via an online map"
            new_res['wms_layer'] = layer
        elif new_format == "WFS":
            new_res['url'] = ws_addr + "wfs"
            new_res['name'] = dataset['title'] + " Web Feature Service API Link"
            new_res['description'] = "WFS API Link for use in Desktop GIS tools"
            new_res['wfs_layer'] = layer
        elif new_format in ['CSV', 'JSON', 'GEOJSON']:
            if new_format == 'CSV':
                serialization = 'csv'
                new_res['name'] = dataset['title'] + " CSV"
                new_res['description'] = "For summary of the objects/data in this collection"
            else:
                serialization = 'json'
                new_res['name'] = dataset['title'] + " GeoJSON"
                new_res['description'] = "For use in web-based data visualisation of this collection"

            new_res[
                'url'] = ws_addr + "wfs?request=GetFeature&typeName=" + layer + "&outputFormat=" + urllib.quote(
                serialization)

        try:
            tk.get_action(resource_command)(context, new_res)
        except Exception, e:
            number_updated -= 1
            log.error("{0} {1} failed with exception {2} for format {3}, continuing...".format(MSG_SPATIAL_PREFIX,
                                                                                               resource_command, str(e),
                                                                                               new_format))

    return number_updated


# Expands and breaks up a zip file pointed to by the url.
# - Nested Zip files are not immediately expanded. They are saved as zipped resources, with the zip_extract
#   flag set, causing a recursion on the CKAN application level.
# - Sub directories are re-zipped _if_ they contain one or more interesting files/sub-directories
#   and are in a directory with at least one other interesting file/sub-directory
# - Individual, interesting files are moved to the target directory, as needed for upload.
def _zip_expand(context, url):
    def interesting_or_zip(file_name):
        return any([file_name.lower().endswith("." + x.lower()) for x in context['target_zip_formats']] + [
            file_name.lower().endswith(".zip")])

    def zip_dir(path, zip_handle):
        for root, dirs, files in os.walk(path):
            for f_name in files:
                zip_handle.write(os.path.join(root, f_name))

    def num_interesting_in_dir(dir):
        res = 0
        for root, dirs, files in os.walk(dir):
            for f_name in files:
                if interesting_or_zip(f_name):
                    res += 1

    def process_contents(dir, target_dir):
        for root, dirs, files in os.walk(dir):
            num_interest_in_level = 0
            for f_name in files:
                if interesting_or_zip(f_name):
                    num_interest_in_level += 1
                    if dir != target_dir:
                        shutil.move(os.path.join(dir, f_name), os.path.join(target_dir, f_name))
                os.remove(os.path.join(root, f_name))
            for d_name in dirs:
                if num_interesting_in_dir(d_name) > 0:
                    num_interest_in_level += 1
            for sub_dir in dirs:
                if num_interesting_in_dir(sub_dir) > 0:
                    if num_interest_in_level > 1:
                        zip_file = zipfile.ZipFile(os.path.join(target_dir, sub_dir + '.zip'), 'w',
                                                   zipfile.ZIP_DEFLATED)
                        zip_dir(sub_dir, zip_file)
                        zip_file.close()
                    else:
                        # The directory only contains one sub_directory with interesting files. There is no
                        # point compressing this sub directory, so we recurse down into it
                        process_contents(sub_dir, target_dir)
                shutil.rmtree(os.path.join(root, sub_dir))
            # Break after one iteration, as any sub-directories will be either Zipped (and recursed into on
            # the application level) or directly recursed into
            break

    response = requests.get(url, stream=True)

    if response.status_code != 200:
        log.error("{0} {1} could not be downloaded, {2}".format(MSG_SPATIAL_PREFIX, url, MSG_SPATIAL_SKIP_SUFFIX))
        return None

    random_id = uuid.uuid1()
    tmp_name = '{0}.{1}'.format(random_id, '.zip')
    tmp_dir = os.path.join(context['temporary_directory'], str(random_id))
    tmp_filepath = os.path.join(tmp_dir, tmp_name)

    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)

    with open(tmp_filepath, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)

    try:
        z = zipfile.ZipFile(tmp_filepath)
    except zipfile.BadZipfile as e:
        log.error(
            "{0} {1} is not a valid zip file, {2}".format(MSG_ZIP_PREFIX, tmp_filepath, MSG_ZIP_SKIP_SUFFIX))
        return None

    try:
        z.extractall(path=tmp_dir)
        os.remove(tmp_filepath)
        process_contents(tmp_dir, tmp_dir)
    except Exception, e:
        log.error("{0} extraction of {1} failed, {2}".format(MSG_ZIP_PREFIX, tmp_filepath, MSG_ZIP_SKIP_SUFFIX))
        shutil.rmtree(tmp_dir)
        return None

    return tmp_dir


def _delete_children(context, resource, child_key, msg_prefix, msg_suffix):
    try:
        log.info("{0} retrieving package data containing resource {1}.".format(msg_prefix, resource['name']))
        dataset = tk.get_action('package_show')(context, {
            'id': resource['package_id'],
        })
    except:
        log.error("{0} failed to retrieve package ID: {1}, {2}".format(msg_prefix, resource['package_id'], msg_suffix))
        return

    res_to_delete = [x['id'] for x in dataset['resources'] if x.get(child_key, '') == resource['id']]
    for res_id in res_to_delete:
        try:
            log.info("{0} deleting child resource {1}".format(msg_prefix, resource['name']))
            tk.get_action('resource_delete')(context, {'id': res_id})
        except Exception, e:
            log.error("{0} failed to delete child resource {1} with exception {2}, continuing...".format(msg_prefix,
                                                                                                         resource[
                                                                                                             'name'],
                                                                                                         str(e)))


def _clean_backend_servers(context, resource):
    try:
        log.info("{0} retrieving package data containing resource {1}.".format(MSG_SPATIAL_PREFIX, resource['name']))
        dataset = tk.get_action('package_show')(context, {
            'id': resource['package_id'],
        })
    except:
        log.error("{0} failed to retrieve package ID: {1}, {2}".format(MSG_SPATIAL_PREFIX, resource['package_id'],
                                                                       MSG_SPATIAL_SKIP_SUFFIX))
        return

    # Clean out PostGIS and Geoserver
    # Drop the PostGIS table, if it exists
    log.info("{0} dropping PostGIS table...".format(MSG_SPATIAL_PREFIX))

    deleted_table_name = _setup_spatial_table(context, resource)

    log.info("{0} PostGIS table dropped!".format(MSG_SPATIAL_PREFIX))

    # Construct geoserver url & name core
    context['geoserver_internal_url'] = 'http://' + context['geoserver']['db_host']
    if context['geoserver'].get('db_port', '') != '':
        context['geoserver_internal_url'] += ':' + context['geoserver']['db_port']
        context['geoserver_internal_url'] += '/' + context['geoserver']['db_name'] + '/'

    core_name = dataset['name'] + "_" + resource['id']
    credentials = (context['geoserver']['db_user'], context['geoserver']['db_pass'])

    # Create a workspace metadata
    workspace = core_name + "_ws"
    wsurl = context['geoserver_internal_url'] + 'rest/workspaces/' + workspace

    # Remove any pre-existing geoserver assets
    log.info("{0} removing any existing geoserver assets...".format(MSG_SPATIAL_PREFIX))

    res = requests.delete(wsurl, params={'recurse': 'true'}, auth=credentials)

    log.info("{0} geoserver {1} recursive deletion returned {2}".format(MSG_SPATIAL_PREFIX, wsurl, res))


def _ingest_dir(context, tmp_dir, parent_resource, dataset):
    new_res = {'package_id': dataset['id'],
               'url': 'will_be_overwritten_but_needed',
               'last_modified': datetime.now().isoformat(),
               'zip_child_of': parent_resource['id'],
               'parent_resource_url': parent_resource['url']}

    for file_name in os.listdir(tmp_dir):
        file_path = os.path.join(tmp_dir, file_name)
        new_res['name'] = file_name.split('.', 1)[0]
        new_res['upload'] = open(file_path)

        if file_name.lower().endswith(".zip"):
            new_res['zip_extract'] = 'True'
        elif 'zip_extract' in new_res:
            new_res.pop('zip_extract', None)
        try:
            tk.get_action('resource_create')(context, new_res)
        except Exception, e:
            log.error(
                "{0} failed to create child Zip resource {1} with exception {2}, continuing...".format(MSG_ZIP_PREFIX,
                                                                                                       new_res['name'],
                                                                                                       str(e)))

    # Remove temp directory, now that resources have been created
    shutil.rmtree(tmp_dir)


def process_spatial(context, resource):
    context = _init(context)
    if context is None:
        return

    # Do not proceed with any expansion, if the resource is the result of another
    # dataset being expanded
    if resource.get('spatial_child_of', None) is not None:
        log.info("{0} {1} is a child resource and not eligible for expansion, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                          resource['name'],
                                                                                          MSG_SPATIAL_SKIP_SUFFIX))
        return

    # We only run the ingest on shapefiles, KML or ArcGrid files
    input_format = _get_input_format(resource)
    if input_format is None:
        log.info("{0} {1} is not an ingestible spatial format, {2}.".format(MSG_SPATIAL_PREFIX, resource['name'],
                                                                            MSG_SPATIAL_SKIP_SUFFIX))
        return

    try:
        log.info("{0} retrieving package data containing resource {1}.".format(MSG_SPATIAL_PREFIX, resource['name']))
        dataset = tk.get_action('package_show')(context, {
            'id': resource['package_id'],
        })
    except Exception, e:
        log.error("{0} failed to retrieve package ID: {1} with error {2}, {3}".format(MSG_SPATIAL_PREFIX,
                                                                                      resource['package_id'],
                                                                                      str(e), MSG_SPATIAL_SKIP_SUFFIX))
        return

    log.info("{0} loaded dataset {1}.".format(MSG_SPATIAL_PREFIX, dataset['name']))

    # Check org, package and last editor blacklists
    blacklist_msg = _check_blacklists(context, dataset)
    if blacklist_msg != '':
        log.info(blacklist_msg)
        return

    # Figure out if any target formats are available to be expanded into.
    # I.e. if a resource of a target format already exists and is _not_
    # last modified by the spatial ingestor user, we do not added/update the
    # resource for that format.
    expansion_formats = _get_upload_formats(context, dataset, resource, input_format)
    if not expansion_formats:
        log.info("{0} dataset {1} has no available formats to expand into, {2}".format(
            (MSG_SPATIAL_PREFIX, dataset['name'], MSG_SPATIAL_SKIP_SUFFIX)))
        return

    # Finally, we mark this as a spatial parent and update the resource. Upon doing the update, we
    # exit, as this method will be called again but with the updated resource.
    if resource.get('spatial_parent', '') != 'True':
        resource['spatial_parent'] = 'True'
        try:
            log.info("{0} marking resource {1} as spatial parent...".format(MSG_SPATIAL_PREFIX, resource['name']))
            tk.get_action('resource_update')(context, resource)
        except Exception, e:
            log.error("{0} failed to update resource {1} as parent with exception {2}, {3}".format(MSG_SPATIAL_PREFIX,
                                                                                                   resource['name'],
                                                                                                   str(e),
                                                                                                   MSG_SPATIAL_SKIP_SUFFIX))
        return

    # We have an ingestible resource that has been updated, passing all blacklist checks
    # and we have potential resources for creation.
    table_name = _setup_spatial_table(context, resource)

    # Ingest into DB and exit if this fails for whatever reason
    native_crs = _db_upload(context, resource['url'], table_name, input_format)

    # Terminate if something went wrong
    if native_crs is None:
        return

    geo_result = _geoserver_transfer(context, input_format, table_name, native_crs, dataset, resource)

    # Terminate if something went wrong
    if geo_result is None:
        return

    workspace, layer, bbox_obj = geo_result

    num_update = _create_or_update_resources(context, expansion_formats, workspace, layer, bbox_obj, dataset, resource)

    log.info("{0} {1} resources created/updated.".format(MSG_SPATIAL_PREFIX, num_update))


def process_zip(context, resource):
    context = _init(context)
    if context is None:
        return

    if "zip" in resource['format'].lower():
        if resource.get('zip_extract', '') != 'True':
            log.info("{0} {1} is not opted in for Zip extraction, {2}".format(MSG_ZIP_PREFIX, resource['name'],
                                                                              MSG_ZIP_SKIP_SUFFIX))
            return
    else:
        log.info("{0} {1} is not a Zip archive, {2}".format(MSG_ZIP_PREFIX, resource['name'], MSG_ZIP_SKIP_SUFFIX))
        return

    res_dir = _zip_expand(context, resource['url'])

    # Make sure the download and extraction were successful
    if res_dir is None:
        return

    # Delete any Zip resources which are the children of this one
    _delete_children(context, resource, 'zip_child_of', MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX)

    _ingest_dir(res_dir)


# Only deletes direct children of resource, as these deletions will cause events to be
# passed to the celery queue that will subsequently recall this with children IDs
def delete_all_children(context, resource):
    context = _init(context)
    if context is None:
        return

    if "zip" in resource['format'].lower() and resource.get('zip_extract', '') == 'True':
        _delete_children(context, resource, 'zip_child_of', MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX)

    if resource.get('spatial_parent', '') == 'True':
        _delete_children(context, resource, 'spatial_child_of', MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX)
        _clean_backend_servers(context, resource)


def purge_spatial(context, package):
    for res in package['resources']:
        if res.get('spatial_parent', '') == 'True':
            _delete_children(context, res, 'spatial_child_of', MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX)
            _clean_backend_servers(context, res)


def rebuild_spatial(context, package):
    for res in package['resources']:
        process_spatial(context, res['id'])


def purge_zip(context, package):
    for res in package['resources']:
        if "zip" in res['format'].lower() and res.get('zip_extract', '') == 'True':
            _delete_children(context, res, 'zip_child_of', MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX)


def rebuild_zip(context, package):
    for res in package['resources']:
        process_zip(context, res['id'])


def process_all(context, package_id=None, process='purge', data_type='zip'):
    context = _init(context)
    if context is None:
        return

    if process == 'purge':
        msg1 = 'Purging'
        msg3 = 'from'
        if data_type == 'zip':
            msg2 = 'zip data'
            process_func = purge_zip
        else:
            msg2 = 'spatial data'
            process_func = purge_spatial
    else:
        msg1 = 'Rebuilding'
        msg3 = 'for'
        if data_type == 'zip':
            msg2 = 'zip data'
            process_func = rebuild_zip
        else:
            msg2 = 'spatial data'
            process_func = rebuild_spatial

    if package_id:
        pkg_dict = tk.get_action('package_show')(context,
                                                 {'id': package_id})
        log.info("{0} {1} {2} package {3}...".format(msg1, msg2, msg3, pkg_dict['name']))
        process_func(context, pkg_dict)
    else:
        model = context['model']
        session = context['session']
        pkg_dicts = [r for r in session.query(model.Package.id).filter(model.Package.state != 'deleted').all()]
        log.info("{0} {1} {2} all packages...".format(msg1, msg2, msg3))

        total_packages = len(pkg_dicts)
        for counter, pkg_dict in enumerate(pkg_dicts):
            sys.stdout.write("\r{0} {1} {2} dataset {3}/{4}".format(msg1, msg2, msg3, counter + 1, total_packages))
            sys.stdout.flush()
            try:
                process_func(context, pkg_dict)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))
