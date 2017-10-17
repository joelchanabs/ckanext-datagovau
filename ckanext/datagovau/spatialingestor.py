#!/usr/bin/python
# coding=utf-8
'''
spatial ingestor for data.gov.au
<greg.vonnessi@linkdigital.com.au>
1.0 28/11/2013 initial implementation
1.1 25/03/2014 new create_resource technique for CKAN editing
1.2 08/09/2014 projection guessing, JNDI database connection and better
               modification detection
1.3 24/09/2014 grid raster support
1.4 16/01/2015 unzip files into flat structure, record wms layer
               name for future expansion
'''
from __future__ import print_function

import calendar
import errno
import glob
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import urllib
from math import copysign
from pylons import config
from datetime import datetime
from zipfile import ZipFile

import lxml.etree as et
import psycopg2
import requests
from dateutil import parser
from osgeo import osr

from ckan.plugins.toolkit import get_action
import ckan.model as model
from ckanext.datagovau import ogr2ogr

# dga.spatialingestor.paster.dbname
# dga.spatialingestor.paster.dbuser
# dga.spatialingestor.paster.dbpassword
# dga.spatialingestor.paster.dbhost
# dga.spatialingestor.paster.username
# dga.spatialingestor.paster.tmp_dir
# dga.spatialingestor.paster.geo_addr
# dga.spatialingestor.paster.geo_user
# dga.spatialingestor.paster.geo_pass


logger = logging.getLogger('root')
log_handler = logging.StreamHandler()
log_handler.setFormatter(
    logging.Formatter("%(asctime)s - [%(levelname)8s] - %(message)s")
)
logger.addHandler(log_handler)
logger.setLevel(logging.DEBUG)

logger.info = logger.warn = logger.debug = logger.error = print

BOT_USER_ID = "68b91a41-7b08-47f1-8434-780eb9f4332d"
SITE_URL = "https://data.gov.au"

OMITTED_ORGS = [
    'australianantarcticdivision', 'australian-institute-of-marine-science',
    'bureauofmeteorology', 'city-of-hobart', 'cityoflaunceston',
    'departmentofenvironment', 'dpipwe', 'geoscienceaustralia',
    'logan-city-council', 'mineral-resources-tasmania', 'nsw-land-and-property'
]
OMITTED_PKGS = [
    'city-of-gold-coast-road-closures', 'central-geelong-3d-massing-model'
]


class IngestionFail(Exception):
    pass


class IngestionSkip(Exception):
    pass


def _get_tmp_path():
    return config.get('dga.spatialingestor.paster.tmp_dir')


def _get_geoserver_data():
    return (
        config.get('dga.spatialingestor.paster.geo_addr'),
        config.get('dga.spatialingestor.paster.geo_user'),
        config.get('dga.spatialingestor.paster.geo_pass')
    )


def _get_db_settings():
    return dict(
        dbname=config.get('dga.spatialingestor.paster.dbname'),
        user=config.get('dga.spatialingestor.paster.dbuser'),
        password=config.get('dga.spatialingestor.paster.dbpassword'),
        host=config.get('dga.spatialingestor.paster.dbhost')
    )


def _get_username():
    return config.get('dga.spatialingestor.paster.username')


def clean_temp(tempdir):
    try:
        shutil.rmtree(tempdir)
    except:
        pass


def success(msg):
    logger.info("Completed!")


def failure(msg):
    logger.error(msg)
    raise IngestionFail(msg)


def get_cursor(db_settings):
    # Connect to an existing database
    try:
        conn = psycopg2.connect(**db_settings)
    except:
        failure("I am unable to connect to the database.")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    conn.set_isolation_level(0)
    # Execute a command: this creates a new table
    # cur.execute("create extension postgis")
    return (cur, conn)


def parse_date(date_str):
    try:
        return calendar.timegm(parser.parse(date_str).utctimetuple())
    except Exception:
        return


def _check_if_may_skip(dataset, force=False):
    """Skip blacklisted orgs, datasets and datasets, updated by bot
    """
    org_name = dataset['organization']['name']
    if org_name in OMITTED_ORGS:
        raise IngestionSkip(org_name + " in omitted_orgs blacklist")

    if dataset['name'] in OMITTED_PKGS:
        raise IngestionSkip(dataset['name'] + " in omitted_pkgs blacklist")

    activity_list = get_action('package_activity_list')(
        {'user': _get_username(), 'model': model},
        {'id': dataset['id']})

    if force:
        return

    if activity_list and activity_list[0]['user_id'] == BOT_USER_ID:
        raise IngestionSkip('Not updated since last ingest')


def _group_resources(dataset):
    ows = []
    kml = []
    shp = []
    grid = []
    sld = []
    for resource in dataset['resources']:
        _format = resource['format'].lower()

        if "wms" in _format or "wfs" in _format:
            if 'geoserver' not in resource['url']:
                raise IngestionSkip(dataset['id'] + " already has geo api")
            else:
                ows.append(resource)
        if 'geoserver' in resource['url']:
            continue
        if "kml" in _format or "kmz" in _format:
            kml.append(resource)
        elif "shp" in _format or "shapefile" in _format:
            shp.append(resource)
        elif "grid" in _format:
            grid.append(resource)
        elif "sld" in _format:
            sld.append(resource)

    return ows, kml, shp, grid, sld


def _clear_old_table(dataset):
    cur, conn = get_cursor(_get_db_settings())
    table_name = dataset['id'].replace("-", "_")
    cur.execute('DROP TABLE IF EXISTS "' + table_name + '"')
    cur.close()
    conn.close()
    return table_name


def _load_esri_shapefiles(shp_resources,
                          table_name, dataset, tempdir):
    shp_res = shp_resources[0]
    shp_res['url'] = shp_res['url'].replace('https', 'http')
    logger.debug(
        "Using SHP file " + shp_res['url'])
    (filepath, headers) = urllib.urlretrieve(
        shp_res['url'], "input.zip")
    logger.debug('SHP downloaded')

    subprocess.call(['unzip', '-j', filepath])
    logger.debug('SHP unzipped')

    shpfiles = glob.glob("*.[sS][hH][pP]")
    prjfiles = glob.glob("*.[pP][rR][jJ]")
    if not shpfiles:
        failure("No shp files found in zip " + shp_res['url'])
    logger.debug("converting to pgsql " + table_name + " " + shpfiles[0])
    if len(prjfiles) > 0:
        prj_txt = open(prjfiles[0], 'r').read()
        sr = osr.SpatialReference()
        sr.ImportFromESRI([prj_txt])

        # FIXME: why this variable is redefined
        res = sr.AutoIdentifyEPSG()
        res = -1
        if res == 0:  # success
            nativeCRS = sr.GetAuthorityName(
                None) + ":" + sr.GetAuthorityCode(None)
        else:
            mapping = {
                "EPSG:28356": [
                    "GDA_1994_MGA_Zone_56", "GDA94_MGA_zone_56"],
                "EPSG:28355": [
                    "GDA_1994_MGA_Zone_55", "GDA94_MGA_zone_55"],
                "EPSG:28354": [
                    "GDA_1994_MGA_Zone_54", "GDA94_MGA_zone_54"],
                "EPSG:4283": [
                    "GCS_GDA_1994",
                    'GEOGCS["GDA94",DATUM["D_GDA_1994",SPHEROID["GRS_1980"'],
                "ESRI:102029": ["Asia_South_Equidistant_Conic"],
                "EPSG:3577": ["Australian_Albers_Equal_Area_Conic_WGS_1984"],
                "EPSG:3857": ["WGS_1984_Web_Mercator_Auxiliary_Sphere"],
                "EPSG:4326": [
                    "MapInfo Generic Lat/Long",
                    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984"']
            }
            for key, values in mapping.items():
                if any([v in prj_txt for v in values]):
                    nativeCRS = key
                    break
            else:
                failure(
                    dataset['title'] + " has unknown projection: " + prj_txt)
    else:
        # if wyndham then GDA_1994_MGA_Zone_55 EPSG:28355
        nativeCRS = "EPSG:4326"
    db_settings = _get_db_settings()
    pargs = [
        ' ', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
        'PG:dbname=\'' + db_settings['dbname'] + '\' host=\'' +
        db_settings['host'] + '\' user=\'' + db_settings['user'] +
        '\' password=\'' + db_settings['password'] + '\'', tempdir, '-lco',
        'GEOMETRY_NAME=geom', "-lco", "PRECISION=NO", '-nln', table_name,
        '-a_srs', nativeCRS, '-nlt', 'PROMOTE_TO_MULTI', '-overwrite'
    ]
    ogr2ogr.main(pargs)
    return nativeCRS


def _load_kml_resources(kml_resources, failure, table_name):
    kml_res = kml_resources[0]
    kml_res['url'] = kml_res['url'].replace('https', 'http')
    logger.debug(
        "Using KML file " + kml_res['url'])
    nativeCRS = 'EPSG:4326'
    # if kml ogr2ogr http://gis.stackexchange.com/questions/33102
    # /how-to-import-kml-file-with-custom-data-to-postgres-postgis-database
    if kml_res['format'] == "kmz" or 'kmz' in kml_res['url'].lower():
        (filepath, headers) = urllib.urlretrieve(
            kml_res['url'], "input.zip")
        subprocess.call(['unzip', '-j', filepath])
        logger.debug("KMZ unziped")
        kmlfiles = glob.glob("*.[kK][mM][lL]")
        if len(kmlfiles) == 0:
            failure("No kml files found in zip " + kml_res['url'])
        else:
            kml_file = kmlfiles[0]
    else:
        filepath, headers = urllib.urlretrieve(kml_res['url'], "input.kml")
        kml_file = "input.kml"

    logger.debug("Changing kml folder name in " + kml_file)
    tree = et.parse(kml_file)
    element = tree.xpath(
        '//kml:Folder/kml:name',
        namespaces={'kml': "http://www.opengis.net/kml/2.2"})
    if 0 in element:
        element[0].text = table_name
    else:
        logger.debug('No kml:Folder tag found')
    find = et.ETXPath(
        '//{http://www.opengis.net/kml/2.2}Folder'
        '/{http://www.opengis.net/kml/2.2}name'
    )
    element = find(tree)
    if len(element):
        for x in range(0, len(element)):
            logger.debug(element[x].text)
            element[x].text = table_name
    else:
        logger.debug('no Folder tag found')
    find = et.ETXPath(
        '//{http://earth.google.com/kml/2.1}Folder'
        '/{http://earth.google.com/kml/2.1}name'
    )
    element = find(tree)
    if len(element):
        for x in range(0, len(element)):
            print(element[x].text)
            element[x].text = table_name
    else:
        logger.debug('no Folder tag found')
    with open(table_name + ".kml", "w") as ofile:
        ofile.write(et.tostring(tree))
    logger.debug("converting to pgsql " + table_name + ".kml")
    db_settings = _get_db_settings()
    pargs = [
        '', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
        'PG:dbname=\'' + db_settings['dbname'] + '\' host=\'' +
        db_settings['host'] + '\' user=\'' + db_settings['user'] +
        '\' password=\'' + db_settings['password'] + '\'',
        table_name + ".kml", '-lco', 'GEOMETRY_NAME=geom', "-lco",
        "PRECISION=NO", '-nln', table_name, '-a_srs', nativeCRS, '-nlt',
        'PROMOTE_TO_MULTI', '-overwrite'
    ]
    ogr2ogr.main(pargs)
    return nativeCRS


def _check_ows_amount(ows_resources, dataset):
    # if geoserver api link does not exist or api
    # link is out of date with data, continue
    data_modified_date = dataset['metadata_modified']

    if len(ows_resources) > 0:
        # todo scan for last date of non-bot edit
        logger.info("Data modified: " + str(parser.parse(data_modified_date)))
    else:
        logger.info(
            "Data modified: " + str(parser.parse(data_modified_date)) +
            " New Dataset" + "\n" + SITE_URL + "/api/action/package_show?id=" +
            dataset['id'] + "\n" + SITE_URL + "/dataset/" + dataset['name'])


def _convert_resources(
        shp_resources, table_name, dataset, tempdir,
        kml_resources, grid_resources):
    using_kml = False
    nativeCRS = ''
    if len(shp_resources) > 0:
        nativeCRS = _load_esri_shapefiles(
            shp_resources, table_name, dataset, tempdir)
    elif len(kml_resources) > 0:
        using_kml = True
        nativeCRS = _load_kml_resources(
            kml_resources, failure, table_name)
    elif len(grid_resources) > 0:
        grid_url = grid_resources[0]['url'].replace('https', 'http')
        logger.debug("using grid file " + grid_url)
        filepath, headers = urllib.urlretrieve(grid_url, "input.zip")
        logger.debug("grid downlaoded")
        with ZipFile(filepath, 'r') as myzip:
            myzip.extractall()
        logger.debug("grid unziped")

        db_settings = _get_db_settings()
        pargs = [
            '', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
            'PG:dbname=\'' + db_settings['dbname'] + '\' host=\'' +
            db_settings['host'] + '\' user=\'' + db_settings['user'] +
            '\' password=\'' + db_settings['password'] + '\'',
            table_name + ".kml", '-lco', 'GEOMETRY_NAME=geom'
        ]
        ogr2ogr.main(pargs)
    return using_kml, nativeCRS


def _get_geojson(using_kml, table_name):
    cur, conn = get_cursor(_get_db_settings())
    if using_kml:
        try:
            cur.execute(
                ('alter table "{}" DROP "description" RESTRICT, '
                 'DROP timestamp RESTRICT, DROP begin RESTRICT, '
                 'DROP "end" RESTRICT, DROP altitudemode RESTRICT, '
                 'DROP tessellate RESTRICT, DROP extrude RESTRICT, '
                 'DROP visibility RESTRICT, DROP draworder RESTRICT, '
                 'DROP icon RESTRICT;').format(table_name)
            )
        except Exception:
            logger.error('KML error', exc_info=True)
    select_query = (
        'SELECT ST_Extent(geom) as box,'
        'ST_Extent(ST_Transform(geom,4326)) as latlngbox, '
        'ST_AsGeoJSON(ST_Extent(ST_Transform(geom,4326))) as geojson '
        'from "{}"').format(table_name)
    cur.execute(select_query)
    logger.debug(select_query)

    bbox, latlngbbox, bgjson = cur.fetchone()
    cur.close()
    conn.close()
    return bbox, latlngbbox, bgjson


def _perform_workspace_requests(datastore, workspace):
    dsdata = json.dumps({
        'dataStore': {
            'name': datastore,
            'connectionParameters': {
                'dbtype': 'postgis',
                "encode functions": "false",
                "jndiReferenceName": "java:comp/env/jdbc/postgres",
                # jndi name you have setup in tomcat http://docs.geoserver.org
                # /stable/en/user/tutorials/tomcat-jndi/tomcat-jndi.html
                # #configuring-a-postgresql-connection-pool
                "Support on the fly geometry simplification": "true",
                "Expose primary keys": "false",
                "Estimated extends": "false"
            }
        }
    })
    logger.debug(dsdata)

    geo_addr, geo_user, geo_pass = _get_geoserver_data()
    # POST creates, PUT updates
    r = requests.post(
        geo_addr + 'rest/workspaces/' + workspace + '/datastores',
        data=dsdata,
        headers={'Content-type': 'application/json'},
        auth=(geo_user, geo_pass))
    logger.debug('POST request {}'.format(r))
    r = requests.put(
        geo_addr + 'rest/workspaces/' + workspace + '/datastores',
        data=dsdata,
        headers={'Content-type': 'application/json'},
        auth=(geo_user, geo_pass))
    logger.debug('PUT request {}'.format(r))


def _apply_sld_resources(sld_resources, workspace):
    geo_addr, geo_user, geo_pass = _get_geoserver_data()
    # POST creates, PUT updates
    for res in sld_resources:
        name = os.path.splitext(os.path.basename(res['url']))[0] + 'x'
        style_url = geo_addr + 'rest/workspaces/' + workspace + '/styles/' + name + '.xml'
        r = requests.get(
            style_url,
            params={'quietOnNotFound': True},
            auth=(geo_user, geo_pass))
        if r.ok:
            r = requests.put(
                style_url,
                data=requests.get(res['url']).content,
                headers={'Content-type': 'application/vnd.ogc.sld+xml'},
                auth=(geo_user, geo_pass))
        else:
            r = requests.post(
                geo_addr + 'rest/workspaces/' + workspace + '/styles.xml',
                data=requests.get(res['url']).content,
                params={
                    'name': name
                },
                headers={'Content-type': 'application/vnd.ogc.sld+xml'},
                auth=(geo_user, geo_pass))
    

def _update_package_with_bbox(bbox, latlngbbox, ftdata,
                              dataset, nativeCRS, bgjson):
    def _clear_box(string):
        return string.replace(
            "BOX", "").replace("(", "").replace(
                ")", "").replace(",", " ").split(" ")

    minx, miny, maxx, maxy = _clear_box(bbox)
    bbox_obj = {'minx': minx, 'maxx': maxx, 'miny': miny, 'maxy': maxy}

    llminx, llminy, llmaxx, llmaxy = _clear_box(latlngbbox)
    llbbox_obj = {
        'minx': llminx,
        'maxx': llmaxx,
        'miny': llminy,
        'maxy': llmaxy
    }

    ftdata['featureType']['nativeBoundingBox'] = bbox_obj
    ftdata['featureType']['latLonBoundingBox'] = llbbox_obj
    update = False
    ftdata['featureType']['srs'] = nativeCRS
    logger.debug(
        'bgjson({}), llbox_obj({})'.format(bgjson, llbbox_obj))
    if 'spatial' not in dataset or dataset['spatial'] != bgjson:
        dataset['spatial'] = bgjson
        update = True
    if update:
        logger.debug(dataset)
        get_action('package_update')(
            {'user': _get_username(), 'model': model}, dataset)
    return bbox_obj


def _create_resources_from_formats(
        ws_addr, layer_name, bbox_obj, existing_formats,
        dataset, ows_resources):
    # FIXME: Why we are iterating over empty list?
    for _format in []:
        url = (
            ws_addr + "wms?request=GetMap&layers=" +
            layer_name + "&bbox=" + bbox_obj['minx'] + "," +
            bbox_obj['miny'] + "," + bbox_obj['maxx'] + "," +
            bbox_obj['maxy'] + "&width=512&height=512&format=" +
            urllib.quote(_format))
        if _format == "image/png" and _format not in existing_formats:
            get_action('resource_create')(
                {'model': model, 'user': _get_username()}, {
                    "package_id": dataset['id'],
                    "name": dataset['title'] + " Preview Image",
                    "description": "View overview image of this dataset",
                    "format": _format,
                    "url": url,
                    "last_modified": datetime.now().isoformat()
                })
        if _format == "kml" and _format not in existing_formats:
            get_action('resource_create')(
                {'user': _get_username(), 'model': model}, {
                    "package_id": dataset['id'],
                    "name": dataset['title'] + " KML",
                    "description": (
                        "View a map of this dataset in web "
                        "and desktop spatial data tools"
                        " including Google Earth"),
                    "format": _format,
                    "url": url,
                    "last_modified": datetime.now().isoformat()
            })
    if "wms" not in existing_formats:
        get_action('resource_create')(
            {'model': model, 'user': _get_username()}, {
                "package_id": dataset['id'],
                "name": dataset['title'] + " - Preview this Dataset (WMS)",
                "description": ("View the data in this "
                                "dataset online via an online map"),
                "format": "wms",
                "url": ws_addr + "wms?request=GetCapabilities",
                "wms_layer": layer_name,
                "last_modified": datetime.now().isoformat()
            })
    else:
        for ows in ows_resources:
            ows['last_modified'] = datetime.now().isoformat()
            get_action('resource_update')(
                {'model': model, 'user': _get_username()}, ows)
    if "wfs" not in existing_formats:
        get_action('resource_create')(
            {'model': model, 'user': _get_username()}, {
                "package_id": dataset['id'],
                "name": dataset['title'] + " Web Feature Service API Link",
                "description": "WFS API Link for use in Desktop GIS tools",
                "format": "wfs",
                "url": ws_addr + "wfs",
                "wfs_layer": layer_name,
                "last_modified": datetime.now().isoformat()
        })
    # SXTPDFINXZCB-292 - Remove CSV creation, as this
    # causes a number of issues with the datapusher
    for _format in ['json', 'geojson']:
        url = (ws_addr + "wfs?request=GetFeature&typeName=" +
               layer_name + "&outputFormat=" + urllib.quote(_format))
        if _format in [
                "json", "geojson"
        ] and not any([x in existing_formats for x in ["json", "geojson"]]):
            get_action('resource_create')(
                {'model': model, 'user': _get_username()}, {
                    "package_id": dataset['id'],
                    "name": dataset['title'] + " GeoJSON",
                    "description": ("For use in web-based data "
                                    "visualisation of this collection"),
                    "format": "geojson",
                    "url": url,
                    "last_modified": datetime.now().isoformat()
                })


def _prepare_everything(
        dataset, shp_resources, kml_resources, grid_resources, tempdir):
    # clear old data table
    table_name = _clear_old_table(dataset)

    # download resource to tmpfile
    print(table_name)
    print(dataset['id'], _get_tmp_path())
    print(tempdir)
    os.chdir(tempdir)
    logger.debug(tempdir + " created")
    using_kml, nativeCRS = _convert_resources(
        shp_resources, table_name, dataset, tempdir,
        kml_resources,  grid_resources)

    # create geoserver workspace/layers http://boundlessgeo.com
    # /2012/10/adding-layers-to-geoserver-using-the-rest-api/
    # name workspace after dataset
    geo_addr, geo_user, geo_pass = _get_geoserver_data()
    workspace = dataset['name']
    requests.post(
        geo_addr + 'rest/workspaces',
        data=json.dumps({
            'workspace': {
                'name': workspace
            }
        }),
        headers={'Content-type': 'application/json'},
        auth=(geo_user, geo_pass))
    # load bounding boxes from database
    return using_kml, table_name, workspace, nativeCRS


def do_ingesting(dataset_id, force):
    tempdir = None
    try:
        dataset = get_action('package_show')(
            {'model': model, 'user': _get_username()},
            {'id': dataset_id})
        logger.info('Loaded dataset {}'.format(dataset['name']))

        _check_if_may_skip(dataset, force)

        grouped_resources = _group_resources(dataset)
        (ows_resources, kml_resources,
         shp_resources, grid_resources, sld_resources) = grouped_resources
        if not any(grouped_resources):
            raise IngestionSkip("No geodata format files detected")

        # if geoserver api link does not exist or api
        # link is out of date with data, continue
        _check_ows_amount(ows_resources, dataset)
        tempdir = tempfile.mkdtemp(suffix=dataset['id'], dir=_get_tmp_path())

        # clear old data table
        (using_kml, table_name,
         workspace, nativeCRS) = _prepare_everything(
             dataset, shp_resources, kml_resources, grid_resources, tempdir)

        # load bounding boxes from database
        bbox, latlngbbox, bgjson = _get_geojson(
            using_kml, table_name)
        logger.debug(bbox)

        datastore = workspace + 'ds'
        _perform_workspace_requests(datastore, workspace)
        _apply_sld_resources(sld_resources, workspace)
        # name layer after resource title
        layer_name = 'ckan_' + table_name
        ftdata = {
            'featureType': {
                'name': layer_name,
                'nativeName': table_name,
                'title': dataset['title']
            }
        }
        if bbox:
            bbox_obj = _update_package_with_bbox(bbox, latlngbbox, ftdata,
                                                 dataset, nativeCRS, bgjson)

        ftdata = json.dumps(ftdata)
        geo_addr, geo_user, geo_pass = _get_geoserver_data()
        logger.debug(
            geo_addr + 'rest/workspaces/' + workspace +
            '/datastores/' + datastore + "/featuretypes")
        logger.debug(ftdata)
        r = requests.post(
            geo_addr + 'rest/workspaces/' + workspace + '/datastores/' +
            datastore + "/featuretypes",
            data=ftdata,
            headers={'Content-Type': 'application/json'},
            auth=(geo_user, geo_pass))
        logger.debug(r)
        # generate wms/wfs api links, kml, png resources and add to package
        existing_formats = []
        for resource in dataset['resources']:
            existing_formats.append(resource['format'].lower())
        # TODO append only if format not already in resources list
        ws_addr = "http://data.gov.au/geoserver/" + dataset['name'] + "/"
        _create_resources_from_formats(
            ws_addr, layer_name, bbox_obj, existing_formats,
            dataset, ows_resources)

        msg = ('{title}\n'
               '{site_url}/api/action/package_show?id={id}\n'
               '{site_url}/dataset/{name}').format(
                   title=dataset['title'], site_url=SITE_URL,
                   id=dataset['id'], name=dataset['name'])
        success(msg)
    except (IngestionSkip, IngestionFail) as e:
        logger.info('{}: {}'.format(type(e), e))
    except Exception as e:
        logger.error(
            "failed to ingest {0} with error {1}".format(dataset_id, str(e)))
    finally:
        if tempdir:
            clean_temp(tempdir)
