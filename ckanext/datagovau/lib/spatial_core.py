import json
import os
import shutil
import urllib
import uuid
import zipfile
from subprocess import call

import ckanapi
import lxml.etree as et
import psycopg2
import requests
from ckan.common import _
from ckan.lib import uploader
from osgeo import osr

from ckanext.datagovau.helpers import MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX, check_blacklists, \
    get_spatial_input_format, is_spatial_resource
from ckanext.datagovau.lib.common import delete_all_children, delete_children, init, update_process_status, \
    update_cleanup_status


def _get_db_cursor(context, resource):
    db_port = None
    if context['postgis'].get('db_port', '') != '':
        db_port = context['postgis']['db_port']

    try:
        connection = psycopg2.connect(dbname=context['postgis']['db_name'],
                                      user=context['postgis']['db_user'],
                                      password=context['postgis']['db_pass'],
                                      host=context['postgis']['db_host'],
                                      port=db_port)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return connection.cursor(), connection
    except Exception, e:
        update_process_status(context,
                              'error',
                              "{0} failed to connect with PostGIS, {1}".format(MSG_SPATIAL_PREFIX,
                                                                               MSG_SPATIAL_SKIP_SUFFIX),
                              resource,
                              'spatialingestor',
                              "{0} failed to connect with PostGIS with error {1}, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                              str(e),
                                                                                              MSG_SPATIAL_SKIP_SUFFIX)
                              )
        return None


def _setup_spatial_table(context, resource):
    res = _get_db_cursor(context, resource)

    if res is None:
        return res

    cursor, connection = res

    table_name = "sp_" + resource['id'].replace("-", "_")

    cursor.execute("DROP TABLE IF EXISTS {tab_name}".format(tab_name=table_name))
    cursor.close()
    connection.close()

    return table_name


def _db_upload(context, parent_resource, input_format, table_name):
    def download_file(resource_url, file_format):
        tmpname = None
        if 'SHP' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'shp.zip')
        elif 'KML' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'kml')
        elif 'KMZ' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'kml.zip')
        elif 'GRID' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'zip')

        if tmpname is None:
            update_process_status(context,
                                  'error',
                                  "{0} {1} file format not recognized: {2}, {3}".format(MSG_SPATIAL_PREFIX,
                                                                                        resource_url, file_format,
                                                                                        MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  None)

        response = requests.get(resource_url.replace('https', 'http'),
                                stream=True,
                                headers={'X-CKAN-API-Key': context['model'].User.get(context['user']).apikey})

        if response.status_code != 200:
            update_process_status(context,
                                  'error',
                                  "{0} {1} could not be downloaded, {2}".format(MSG_SPATIAL_PREFIX, resource_url,
                                                                                MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  None)
            return None

        try:
            with open(os.path.join(context['temporary_directory'], tmpname), 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
        except:
            update_process_status(context,
                                  'error',
                                  "{0} failed to copy file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                               MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} failed to copy file {1}, {2}".format(MSG_SPATIAL_PREFIX, out_file,
                                                                                   MSG_SPATIAL_SKIP_SUFFIX))
            return None

        return os.path.join(context['temporary_directory'], tmpname)

    def unzip_file(zpf, filepath):
        # Take only the filename before the extension
        f_path, f_name = os.path.split(filepath)
        dirname = os.path.join(context['temporary_directory'], f_name.split('.', 1)[0])

        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        else:
            # Probably created by another process
            update_process_status(context,
                                  'error',
                                  "{0} previous temp directory found, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                  MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} previous temp directory {1} found , {2}".format(MSG_SPATIAL_PREFIX, dirname,
                                                                                       MSG_SPATIAL_SKIP_SUFFIX))
            return None

        for name in zpf.namelist():
            zpf.extract(name, dirname)

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

    base_file_istemp = True
    base_filepath = None
    zpf = None
    if parent_resource.get('__extras', {}).get('url_type', parent_resource.get('url_type', '')) == 'upload':
        try:
            upload = uploader.ResourceUpload(parent_resource)
            base_filepath = upload.get_path(parent_resource['id'])
            base_file_istemp = False
            zpf = zipfile.ZipFile(base_filepath)
        except Exception, e:
            update_process_status(context,
                                  'error',
                                  "{0} failed to retrieve local copy of file, retrying via URL.".format(
                                      MSG_SPATIAL_PREFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} failed to retrieve {1} with error {2}, retrying via URL.".format(
                                      MSG_SPATIAL_PREFIX, base_filepath, str(e)))

    if zpf is None:
        try:
            base_filepath = download_file(parent_resource['url'], input_format)
            zpf = zipfile.ZipFile(base_filepath)
        except zipfile.BadZipfile:
            update_process_status(context,
                                  'error',
                                  "{0} {1} did not produce a valid zip file, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                         parent_resource['url'],
                                                                                         MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} {1} is not a valid zip file, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                base_filepath,
                                                                                MSG_SPATIAL_SKIP_SUFFIX),
                                  )
            return None

    # Did we not manage to download anything?
    if base_filepath is None:
        return None

    # Do we need to unzip?
    if input_format in ["KMZ", "SHP", "GRID"]:
        unzip_dir = unzip_file(zpf, base_filepath)

        # File is unzipped, no need to keep the compressed version
        if base_file_istemp:
            try:
                os.remove(os.path.join(base_filepath))
            except:
                # Probably removed by another process
                update_process_status(context,
                                      'error',
                                      "{0} could not remove source zip file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                      MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} could not remove {1} found , {2}".format(MSG_SPATIAL_PREFIX, base_filepath,
                                                                                           MSG_SPATIAL_SKIP_SUFFIX))
                return None

        if unzip_dir is None:
            return None

    if input_format in ["KMZ", "KML", "GRID"]:
        if unzip_dir is not None:
            kml_file = None
            for f in os.listdir(unzip_dir):
                if f.lower().endswith(".kml"):
                    kml_file = f

            if kml_file is None:
                update_process_status(context,
                                      'error',
                                      "{0} No KML file found in archive, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                     MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} No KML file found in archive {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                                         MSG_SPATIAL_SKIP_SUFFIX))
                try:
                    shutil.rmtree(unzip_dir)
                except:
                    # Probably done by another process
                    update_process_status(context,
                                          'error',
                                          "{0} failed to remove directory, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                       MSG_SPATIAL_SKIP_SUFFIX),
                                          parent_resource,
                                          'spatialingestor',
                                          "{0} failed to remove directory {1}, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                           unzip_dir,
                                                                                           MSG_SPATIAL_SKIP_SUFFIX))

                return None
        else:
            kml_file = base_filepath

        # Update folder name in KML file with table_name
        try:
            tree = et.parse(kml_file)
        except IOError:
            update_process_status(context,
                                  'error',
                                  "{0} unable to read extracted KML file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                      MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} unable to read KML file {1}, {2}".format(MSG_SPATIAL_PREFIX, kml_file,
                                                                                MSG_SPATIAL_SKIP_SUFFIX))

            try:
                shutil.rmtree(unzip_dir)
            except:
                update_process_status(context,
                                      'error',
                                      "{0} failed to remove directory, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                   MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} failed to remove directory {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                                       MSG_SPATIAL_SKIP_SUFFIX))
            return None

        for ns in ['http://www.opengis.net/kml/2.2', 'http://earth.google.com/kml/2.1']:
            find = et.ETXPath('//{' + ns + '}Folder/{' + ns + '}name')
            element = find(tree)
            for x in element:
                x.text = table_name

        # Clean up temporary files
        if unzip_dir is not None:
            try:
                shutil.rmtree(unzip_dir)
            except:
                # Probably done by another process
                update_process_status(context,
                                      'error',
                                      "{0} failed to remove directory, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                          MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} failed to remove directory {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                                    MSG_SPATIAL_SKIP_SUFFIX))
                return None
        else:
            if base_file_istemp:
                try:
                    os.remove(base_filepath)
                except:
                    # Probably done by another process
                    update_process_status(context,
                                          'error',
                                          "{0} could not remove source zip file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                             MSG_SPATIAL_SKIP_SUFFIX),
                                          parent_resource,
                                          'spatialingestor',
                                          "{0} could not remove {1} found , {2}".format(MSG_SPATIAL_PREFIX,
                                                                                        base_filepath,
                                                                                        MSG_SPATIAL_SKIP_SUFFIX))
                    return None

        # Write new KML file
        kml_file_new = os.path.join(context['temporary_directory'], table_name + ".kml")
        try:
            with open(kml_file_new, 'w') as out_file:
                out_file.write(et.tostring(tree))
        except:
            # Another job probably blocked this
            update_process_status(context,
                                  'error',
                                  "{0} could not write KML file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                     MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} could not write KML file {1} found , {2}".format(MSG_SPATIAL_PREFIX,
                                                                                kml_file_new,
                                                                                MSG_SPATIAL_SKIP_SUFFIX))
            return None

        # Use ogr2ogr to process the KML into the postgis DB
        return_code = db_ingest(kml_file_new, native_crs)

        # Remove edited KML file
        try:
            os.remove(kml_file_new)
        except:
            # Probably done by another process
            update_process_status(context,
                                  'error',
                                  "{0} could not delete KML file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                  MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} could not remove KML file {1} found , {2}".format(MSG_SPATIAL_PREFIX,
                                                                                        kml_file_new,
                                                                                        MSG_SPATIAL_SKIP_SUFFIX))
            return None

        if return_code == 1:
            update_process_status(context,
                                  'error',
                                  "{0} file could not be converted by ogr2ogr, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                           MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} {1} could not be converted by ogr2ogr, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                          kml_file_new,
                                                                                          MSG_SPATIAL_SKIP_SUFFIX))
            return None
        else:
            update_process_status(context,
                                  'pending',
                                  "{0} ogr2ogr successfully ingested resource file into PostGIS DB, {1}".format(
                                      MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} ogr2ogr successfully ingested {1} into PostGIS DB, {2}".format(
                                      MSG_SPATIAL_PREFIX, kml_file_new, MSG_SPATIAL_SKIP_SUFFIX))

    elif input_format == "SHP":

        shp_file = None
        prj_file = None
        for f in os.listdir(unzip_dir):
            if f.lower().endswith(".shp"):
                shp_file = f

            if f.lower().endswith(".prj"):
                prj_file = f

        if shp_file is None:
            update_process_status(context,
                                  'error',
                                  "{0} No shapefile found in archive, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                  MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} No shapefile found in archive {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                                      MSG_SPATIAL_SKIP_SUFFIX))
            try:
                shutil.rmtree(unzip_dir)
            except:
                # Probably done by another process
                update_process_status(context,
                                      'error',
                                      "{0} failed to remove directory, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                          MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} failed to remove directory {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                                    MSG_SPATIAL_SKIP_SUFFIX))
                return None

        file_path = os.path.join(unzip_dir, shp_file)

        # Determine projection information
        if prj_file:
            try:
                prj_txt = open(os.path.join(unzip_dir, prj_file), 'r').read()
            except:
                # Possibly already ingested by another job
                update_process_status(context,
                                      'error',
                                      "{0} could not open projection file, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                   MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} could not open projection file {1}, {2}".format(MSG_SPATIAL_PREFIX, prj_file,
                                                                                       MSG_SPATIAL_SKIP_SUFFIX))
                return None
            sr = osr.SpatialReference()
            sr.ImportFromESRI([prj_txt])
            res = sr.AutoIdentifyEPSG()
            if res == 0:  # Successful auto-identify
                native_crs = sr.GetAuthorityName(None) + ":" + sr.GetAuthorityCode(None)
                update_process_status(context,
                                      'pending',
                                      "{0} successfully identified projection of {1} as {2}".format(MSG_SPATIAL_PREFIX,
                                                                                                    parent_resource[
                                                                                                        'url'],
                                                                                                    native_crs),
                                      parent_resource,
                                      'spatialingestor',
                                      None)
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
                update_process_status(context,
                                      'error',
                                      "{0} {1} has unknown projection, {2}".format(MSG_SPATIAL_PREFIX, file_path,
                                                                                   MSG_SPATIAL_SKIP_SUFFIX),
                                      parent_resource,
                                      'spatialingestor',
                                      "{0} {1} has unknown projection: {2}, {3}".format(MSG_SPATIAL_PREFIX, file_path,
                                                                                        prj_txt,
                                                                                        MSG_SPATIAL_SKIP_SUFFIX))
                try:
                    shutil.rmtree(unzip_dir)
                except:
                    # Probably done by another process
                    update_process_status(context,
                                          'error',
                                          "{0} failed to remove directory, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                       MSG_SPATIAL_SKIP_SUFFIX),
                                          parent_resource,
                                          'spatialingestor',
                                          "{0} failed to remove directory {1}, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                           unzip_dir,
                                                                                           MSG_SPATIAL_SKIP_SUFFIX))
                    return None

        # Use ogr2ogr to process the shapefile into the postgis DB
        return_code = db_ingest(file_path, native_crs)

        try:
            shutil.rmtree(unzip_dir)
        except:
            # Probably done by another process
            update_process_status(context,
                                  'error',
                                  "{0} failed to remove directory, {1}".format(MSG_SPATIAL_PREFIX,
                                                                               MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} failed to remove directory {1}, {2}".format(MSG_SPATIAL_PREFIX, unzip_dir,
                                                                                   MSG_SPATIAL_SKIP_SUFFIX))
            return None

        if return_code == 1:
            update_process_status(context,
                                  'error',
                                  "{0} file could not be converted by ogr2ogr, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                           MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} {1} could not be converted by ogr2ogr, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                          file_path,
                                                                                          MSG_SPATIAL_SKIP_SUFFIX))
            return None
        else:
            update_process_status(context,
                                  'pending',
                                  "{0} ogr2ogr successfully ingested resource file into PostGIS DB, {1}".format(
                                      MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  "{0} ogr2ogr successfully ingested {1} into PostGIS DB, {2}".format(
                                      MSG_SPATIAL_PREFIX, file_path,
                                      MSG_SPATIAL_SKIP_SUFFIX))

    else:
        # Should never get here
        update_process_status(context,
                              'error',
                              "{0} {1} unrecognized for DB upload, {2}".format(MSG_SPATIAL_PREFIX, input_format,
                                                                               MSG_SPATIAL_SKIP_SUFFIX),
                              parent_resource,
                              'spatialingestor',
                              None)
        return None

    return native_crs


def _geoserver_transfer(context, ckan, package, parent_resource, input_format, native_crs, table_name):
    # Call to postgis to determine bounding box
    conn_params = _get_db_cursor(context, parent_resource)

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
            update_process_status(context,
                                  'error',
                                  "{0} failed to alter KML PostGIS table with exception: {1}, continuing...".format(
                                      MSG_SPATIAL_PREFIX,
                                      str(e)),
                                  parent_resource,
                                  'spatialingestor')

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
        update_process_status(context,
                              'error',
                              "{0} failed to extract data from PostGIS, {1}".format(
                                  MSG_SPATIAL_PREFIX,
                                  MSG_SPATIAL_SKIP_SUFFIX),
                              parent_resource,
                              'spatialingestor',
                              "{0} failed to extract data from PostGIS with exception: {1}, {2}".format(
                                  MSG_SPATIAL_PREFIX,
                                  str(e),
                                  MSG_SPATIAL_SKIP_SUFFIX))
        return None

    # Construct geoserver url & name core
    context['geoserver_internal_url'] = 'http://' + context['geoserver']['db_host']
    if context['geoserver'].get('db_port', '') != '':
        context['geoserver_internal_url'] += ':' + context['geoserver']['db_port']
    context['geoserver_internal_url'] += '/' + context['geoserver']['db_name'] + '/'

    core_name = package['name'] + "_" + parent_resource['id']
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
    ftdata = {'featureType': {'name': layer, 'nativeName': table_name, 'title': package['title']}}

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
            update_process_status(context,
                                  'error',
                                  "{0} {1} has invalid automatic projection {3}, {4}".format(MSG_SPATIAL_PREFIX,
                                                                                             package['title'],
                                                                                             native_crs,
                                                                                             MSG_SPATIAL_SKIP_SUFFIX),
                                  parent_resource,
                                  'spatialingestor',
                                  None)
            return None
        else:
            ftdata['featureType']['srs'] = native_crs
            if 'spatial' not in package or package['spatial'] != bgjson:
                package['spatial'] = bgjson
                ckan.call_action('package_update', data_dict=package)

    ftdata = json.dumps(ftdata)

    # Remove any pre-existing geoserver assets
    update_process_status(context,
                          'pending',
                          "{0} removing any pre-existing geoserver assets...".format(MSG_SPATIAL_PREFIX),
                          parent_resource,
                          'spatialingestor',
                          None)

    # Manually add geoserver parameters here as requests does not hangle parameters without values
    # https://github.com/kennethreitz/requests/issues/2651
    res = requests.delete(wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', auth=credentials)

    if res.status_code != 200:
        update_process_status(context,
                              'error',
                              "{0} geoserver {1} recursive workspace deletion failed with response {2}, continuing...".format(
                                  MSG_SPATIAL_PREFIX, wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', res),
                              parent_resource,
                              'spatialingestor',
                              None)
    else:
        update_process_status(context,
                              'pending',
                              "{0} geoserver recursive workspace deletion succeeded.".format(MSG_SPATIAL_PREFIX),
                              parent_resource,
                              'spatialingestor',
                              None)

    # Upload new geoserver assets
    update_process_status(context,
                          'pending',
                          "{0} uploading new assets to geoserver...".format(MSG_SPATIAL_PREFIX),
                          parent_resource,
                          'spatialingestor',
                          None)

    res = requests.post(wsurl, data=wsdata, headers=headers, auth=credentials)

    if res.status_code != 201:
        update_process_status(context,
                              'error',
                              "{0} geoserver {1} workspace creation failed with response {2}, continuing...".format(
                                  MSG_SPATIAL_PREFIX, wsurl, res),
                              parent_resource,
                              'spatialingestor',
                              None)
    else:
        update_process_status(context,
                              'pending',
                              "{0} geoserver workspace creation succeeded.".format(MSG_SPATIAL_PREFIX),
                              parent_resource,
                              'spatialingestor',
                              None)

    res = requests.post(dsurl, data=dsdata, headers=headers, auth=credentials)

    if res.status_code != 201:
        update_process_status(context,
                              'error',
                              "{0} geoserver {1} datastore creation failed with response {2}, continuing...".format(
                                  MSG_SPATIAL_PREFIX, dsurl, res),
                              parent_resource,
                              'spatialingestor',
                              None)
    else:
        update_process_status(context,
                              'pending',
                              "{0} geoserver datastore creation succeeded.".format(MSG_SPATIAL_PREFIX),
                              parent_resource,
                              'spatialingestor',
                              None)

    res = requests.post(fturl, data=ftdata, headers=headers, auth=credentials)

    if res.status_code != 201:
        update_process_status(context,
                              'error',
                              "{0} geoserver {1} feature type creation failed with response {2}, continuing...".format(
                                  MSG_SPATIAL_PREFIX, fturl, res),
                              parent_resource,
                              'spatialingestor',
                              None)
    else:
        update_process_status(context,
                              'pending',
                              "{0} geoserver feature type creation succeeded.".format(MSG_SPATIAL_PREFIX),
                              parent_resource,
                              'spatialingestor',
                              None)

    return workspace, layer, bbox_obj


def _get_spatial_upload_formats(context, package, parent_resource, input_format):
    '''
    :param context:
    :param package:
    :param parent_resource:
    :param input_format:
    :return:
    '''
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


def _create_or_update_resources(context, ckan, package, parent_resource, bbox_obj, expansion_formats, layer, workspace):
    ws_addr = context['geoserver_public_url'] + "/" + workspace + "/"

    number_updated = 0

    for new_format, old_id in expansion_formats:
        number_updated += 1

        resource_command = 'resource_create'
        if old_id is not None:
            resource_command = 'resource_update'
            new_res = ckan.call_action('resource_show', data_dict={'id': old_id})
            new_res['format'] = new_format.lower()
            new_res['spatial_child_of'] = parent_resource['id']
            new_res['parent_resource_url'] = parent_resource['url']
        else:
            new_res = {'package_id': package['id'],
                       'format': new_format.lower(),
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
                new_res['name'] = package['title'] + " Preview Image"
                new_res['description'] = "View overview image of this dataset"
            elif new_format == "KML":
                new_res['name'] = package['title'] + " KML"
                new_res[
                    'description'] = "View a map of this dataset in web and desktop spatial data tools including Google Earth"
        elif new_format == "WMS":
            new_res['url'] = ws_addr + "wms?request=GetCapabilities"
            new_res['name'] = package['title'] + " - Preview this Dataset (WMS)"
            new_res['description'] = "View the data in this dataset online via an online map"
            new_res['wms_layer'] = layer
        elif new_format == "WFS":
            new_res['url'] = ws_addr + "wfs"
            new_res['name'] = package['title'] + " Web Feature Service API Link"
            new_res['description'] = "WFS API Link for use in Desktop GIS tools"
            new_res['wfs_layer'] = layer
        elif new_format in ['CSV', 'JSON', 'GEOJSON']:
            if new_format == 'CSV':
                serialization = 'csv'
                new_res['name'] = package['title'] + " CSV"
                new_res['description'] = "For summary of the objects/data in this collection"
            else:
                serialization = 'json'
                new_res['name'] = package['title'] + " GeoJSON"
                new_res['description'] = "For use in web-based data visualisation of this collection"

            new_res[
                'url'] = ws_addr + "wfs?request=GetFeature&typeName=" + layer + "&outputFormat=" + urllib.quote(
                serialization)
        else:
            continue

        try:
            api_res = ckan.call_action(resource_command, data_dict=new_res)
            update_process_status(context,
                                  'pending',
                                  "{0} resource {1} ingested into CKAN".format(
                                      MSG_SPATIAL_PREFIX, api_res['name']),
                                  parent_resource,
                                  'spatialingestor',
                                  None)
        except Exception:
            number_updated -= 1
            update_process_status(context,
                                  'error',
                                  "{0} {1} could not be uploaded via CKAN API, continuing...".format(MSG_SPATIAL_PREFIX,
                                                                                                     new_res['name']),
                                  parent_resource,
                                  'spatialingestor',
                                  None)

    return number_updated


def _clean_backend_servers(context, ckan, resource):
    try:
        update_cleanup_status(context,
                              'pending',
                              "{0} retrieving package data containing resource {1}.".format(MSG_SPATIAL_PREFIX,
                                                                                            resource['name']),
                              resource,
                              'spatialingestor',
                              None)
        dataset = ckan.call_action('package_show', data_dict={'id': resource['package_id']})
    except:
        update_cleanup_status(context,
                              'error',
                              "{0} failed to retrieve package ID from resource, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                            MSG_SPATIAL_SKIP_SUFFIX),
                              resource['id'],
                              'spatialingestor',
                              "{0} failed to retrieve package ID: {1}, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                   resource['package_id'],
                                                                                   MSG_SPATIAL_SKIP_SUFFIX))
        return

    # Clean out PostGIS and Geoserver
    # Drop the PostGIS table, if it exists
    update_cleanup_status(context,
                          'pending',
                          "{0} dropping PostGIS table...".format(MSG_SPATIAL_PREFIX),
                          resource,
                          'spatialingestor',
                          None)

    update_cleanup_status(context,
                          'pending',
                          "{0} PostGIS table dropped!".format(MSG_SPATIAL_PREFIX),
                          resource,
                          'spatialingestor',
                          None)

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
    update_cleanup_status(context,
                          'pending',
                          "{0} removing any existing geoserver assets...".format(MSG_SPATIAL_PREFIX),
                          resource,
                          'spatialingestor',
                          None)

    res = requests.delete(wsurl, params={'recurse': 'true'}, auth=credentials)

    if res.status_code != 200:
        update_process_status(context,
                              'error',
                              "{0} geoserver {1} recursive workspace deletion failed with response {2}, continuing...".format(
                                  MSG_SPATIAL_PREFIX, wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', res),
                              resource,
                              'spatialingestor',
                              None)
    else:
        update_process_status(context,
                              'pending',
                              "{0} geoserver recursive workspace deletion succeeded.".format(MSG_SPATIAL_PREFIX),
                              resource,
                              'spatialingestor',
                              None)


def process_spatial(context, resource):
    context = init(context, resource, 'spatialingestor')
    if context is None:
        return

    ckan = ckanapi.RemoteCKAN(address=context['ckan_api_url'],
                              apikey=context['model'].User.get(context['user']).apikey)

    try:
        package = ckan.call_action('package_show', data_dict={'id': resource['package_id']})
    except Exception, e:
        update_process_status(context,
                              'error',
                              "{0} failed to retrieve package ID from resource, {1}".format(MSG_SPATIAL_PREFIX,
                                                                                            MSG_SPATIAL_SKIP_SUFFIX),
                              resource,
                              'spatialingestor',
                              "{0} failed to retrieve package ID: {1} with error {2}, {3}".format(MSG_SPATIAL_PREFIX,
                                                                                                  resource[
                                                                                                      'package_id'],
                                                                                                  str(e),
                                                                                                  MSG_SPATIAL_SKIP_SUFFIX))
        return

    # We have an ingestible resource that has been updated, passing all blacklist checks
    # and we have potential resources for creation.
    table_name = _setup_spatial_table(context, resource)

    input_format = get_spatial_input_format(resource)

    resource.get('format', resource.get('url', '')).upper()

    # Ingest into DB and exit if this fails for whatever reason
    native_crs = _db_upload(context, resource, input_format, table_name)

    # Terminate if something went wrong
    if native_crs is None:
        return

    geo_result = _geoserver_transfer(context, ckan, package, resource, input_format, native_crs, table_name)

    # Terminate if something went wrong
    if geo_result is None:
        return

    workspace, layer, bbox_obj = geo_result

    # Figure out if any target formats are available to be expanded into.
    # I.e. if a resource of a target format already exists and is _not_
    # last modified by the spatial ingestor user, we do not added/update the
    # resource for that format.
    expansion_formats = _get_spatial_upload_formats(context, package, resource, input_format)
    if not expansion_formats:
        update_process_status(context,
                              'error',
                              "{0} dataset {1} has no available formats to expand into, {2}".format(MSG_SPATIAL_PREFIX,
                                                                                                    package['name'],
                                                                                                    MSG_SPATIAL_SKIP_SUFFIX),
                              resource,
                              'spatialingestor',
                              None)
        return

    update_process_status(context,
                          'complete',
                          "{0} creating resources.".format(MSG_SPATIAL_PREFIX),
                          resource,
                          'spatialingestor',
                          None)

    num_update = _create_or_update_resources(context, ckan, package, resource, bbox_obj, expansion_formats, layer,
                                             workspace)

    update_process_status(context,
                          'complete',
                          "{0} {1} resources created/updated.".format(MSG_SPATIAL_PREFIX, num_update),
                          resource,
                          'spatialingestor',
                          None)


# Only deletes direct children of resource, as these deletions will cause events to be
# passed to the celery queue that will subsequently recall this with children IDs
def spatial_delete_all_children(context, resource):
    context = init(context, resource, 'spatialingestor')
    if context is None:
        return

    ckan = ckanapi.RemoteCKAN(address=context['ckan_api_url'],
                              apikey=context['model'].User.get(context['user']).apikey)

    delete_children(context, ckan, resource, 'spatial_child_of', MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX,
                    'spatialingestor')

    update_cleanup_status(context,
                          'pending',
                          "{0} all children of {1} deleted. Cleaning PostGIS and Geoserver assets.".format(
                              MSG_SPATIAL_PREFIX, resource.get('name', resource['id'])),
                          resource,
                          'zipextractor',
                          None)

    _clean_backend_servers(context, ckan, resource)

    update_cleanup_status(context,
                          'complete',
                          "{0} PostGIS and Geoserver assets of {1} deleted.".format(MSG_SPATIAL_PREFIX,
                                                                                    resource.get('name',
                                                                                                 resource['id'])),
                          resource,
                          'zipextractor',
                          None)


def purge_spatial(context, package):
    ckan = ckanapi.RemoteCKAN(address=context['ckan_api_url'],
                              apikey=context['model'].User.get(context['user']).apikey)

    delete_all_children(context, ckan, package, 'spatial_child_of', MSG_SPATIAL_PREFIX, 'spatialingestor')


def rebuild_spatial(context, package):
    default_user = context['user']
    for res in package['resources']:
        if is_spatial_resource(res):
            context['user'] = res.get('spatial_creator', default_user)

            if res.get('name', '') == '':
                res['name'] = _('Unamed resource')

            if res.get('spatial_parent', '') == 'True' or (
                            context.get('auto_process', '') == 'True' and check_blacklists(context, package) == ''):
                process_spatial(context, res)
