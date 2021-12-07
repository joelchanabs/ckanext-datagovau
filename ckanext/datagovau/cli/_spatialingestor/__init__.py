"""
spatial ingestor for data.gov.au
<greg.vonnessi@linkdigital.com.au>
1.0 28/11/2013 initial implementation
1.1 25/03/2014 new create_resource technique for CKAN editing
1.2 08/09/2014 projection guessing, JNDI database connection and better
               modification detection
1.3 24/09/2014 grid raster support
1.4 16/01/2015 unzip files into flat structure, record wms layer
               name for future expansion
"""
from __future__ import annotations

import glob
import grp
import logging
import os
import pwd
import shutil
import subprocess
import contextlib
from typing import (
    Any,
    Container,
    Iterable,
    NamedTuple,
    Optional,
    Dict,
    List,
    NoReturn,
    TypeVar,
)
import urllib
import re

from datetime import datetime

import lxml.etree as et
import psycopg2

import ckan.plugins.toolkit as tk

from osgeo import osr
from ckanext.datagovau import utils
from osgeo_utils import gdal_retile
from osgeo_utils.samples import ogr2ogr

from .geoserver import get_geoserver
from .exc import BadConfig, IngestionFail

log = logging.getLogger(__name__)

ResourceGroup = List[Dict[str, Any]]

T = TypeVar("T")


def _contains(value: Container[T], parts: Iterable[T]) -> bool:
    return any(part in value for part in parts)


class GroupedResources(NamedTuple):
    shp: ResourceGroup
    kml: ResourceGroup
    tab: ResourceGroup
    tiff: ResourceGroup
    grid: ResourceGroup
    sld: ResourceGroup

    @classmethod
    def from_dataset(cls, dataset: dict[str, Any]):
        shp = []
        kml = []
        tab = []
        tiff = []
        grid = []
        sld = []

        source_formats = map(str.lower, _get_source_formats())

        for resource in dataset["resources"]:
            fmt = resource["format"].lower()
            is_source = _contains(fmt, source_formats)

            if "/geoserver" in resource["url"]:
                continue

            if _contains(fmt, {"sld"}):
                sld.append(resource)
            elif is_source:
                if _contains(fmt, {"kml", "kmz"}):
                    kml.append(resource)
                elif _contains(fmt, {"shp", "shapefile", "shz"}):
                    shp.append(resource)
                elif _contains(fmt, {"tab", "mapinfo"}):
                    tab.append(resource)
                elif _contains(fmt, {"grid"}):
                    grid.append(resource)
                elif _contains(fmt, {"geotif", "geotiff"}):
                    tiff.append(resource)

        return cls(shp, kml, tab, tiff, grid, sld)


def _get_geoserver_data_dir(native_name: str) -> str:
    name = tk.config.get(
        "ckanext.datagovau.spatialingestor.geoserver.base_dir"
    ).rstrip("/")
    return name + "/" + native_name


def _get_db_settings():

    regex = [
        "^\\s*(?P<db_type>\\w*)",
        "://",
        "(?P<db_user>[^:]*)",
        ":?",
        "(?P<db_pass>[^@]*)",
        "@",
        "(?P<db_host>[^/:]*)",
        ":?",
        "(?P<db_port>[^/]*)",
        "/",
        "(?P<db_name>[\\w.-]*)",
    ]

    url = _get_datastore_url()
    match = re.match("".join(regex), url)
    if not match:
        raise BadConfig(f"Invalid datastore.url: {url}")
    postgis_info = match.groupdict()

    db_port = postgis_info.get("db_port", "")
    if db_port == "":
        db_port = None

    return dict(
        dbname=postgis_info.get("db_name"),
        user=postgis_info.get("db_user"),
        password=postgis_info.get("db_pass"),
        host=postgis_info.get("db_host"),
        port=db_port,
    )


def _get_db_param_string(db_settings):
    result = (
        "PG:dbname='"
        + db_settings["dbname"]
        + "' host='"
        + db_settings["host"]
        + "' user='"
        + db_settings["user"]
        + "' password='"
        + db_settings["password"]
        + "'"
    )

    if db_settings.get("port"):
        result += " port='" + db_settings["port"] + "'"

    return result


def _clean_dir(tempdir: str):
    shutil.rmtree(tempdir, ignore_errors=True)


def _get_cursor():
    # Connect to an existing database
    try:
        conn = psycopg2.connect(GEOSERVER_DATASTORE_URL)
    except:
        _failure("I am unable to connect to the database.")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    conn.set_isolation_level(0)
    # Execute a command: this creates a new table
    # cur.execute("create extension postgis")
    return cur, conn


def _failure(msg: str) -> NoReturn:
    log.error(msg)
    raise IngestionFail(msg)


def _clear_old_table(dataset: dict[str, Any]) -> str:
    cur, conn = _get_cursor()
    table_name = "ckan_" + dataset["id"].replace("-", "_")
    cur.execute('DROP TABLE IF EXISTS "' + table_name + '"')
    cur.close()
    conn.close()
    return table_name


def _create_geoserver_data_dir(native_name: str) -> str:
    data_output_dir = _get_geoserver_data_dir(native_name)
    os.makedirs(data_output_dir, exist_ok=True)
    return data_output_dir


def _set_geoserver_ownership(data_dir):
    uid = pwd.getpwnam(
        tk.config.get("ckanext.datagovau.spatialingestor.geoserver.os_user")
    ).pw_uid
    gid = grp.getgrnam(
        tk.config.get("ckanext.datagovau.spatialingestor.geoserver.os_group")
    ).gr_gid
    os.chown(data_dir, uid, gid)
    for root, dirs, files in os.walk(data_dir):
        for momo in dirs:
            os.chown(os.path.join(root, momo), uid, gid)
        for momo in files:
            os.chown(os.path.join(root, momo), uid, gid)


def _load_esri_shapefiles(
    shp_res: dict[str, Any], table_name: str, tempdir: str
) -> str:
    log.debug("_load_esri_shapefiles():: shp_res = %s", shp_res)
    log.debug("Using SHP file %s", shp_res["url"])

    if any(shp_res["url"].lower().endswith(x) for x in ["shp", "shapefile"]):
        urllib.request.urlretrieve(shp_res["url"], "input.shp")
        log.debug("SHP downloaded")
    else:
        with open("input.zip", "wb") as f:
            f.write(urllib.request.urlopen(shp_res["url"]).read())
        log.debug("SHP downloaded")

        subprocess.call(["unzip", "-j", "input.zip"])
        log.debug("SHP unzipped")

    shpfiles = glob.glob("*.[sS][hH][pP]")
    prjfiles = glob.glob("*.[pP][rR][jJ]")
    if not shpfiles:
        _failure("No shp files found in zip " + shp_res["url"])
    log.debug("converting to pgsql " + table_name + " " + shpfiles[0])

    if len(prjfiles) > 0:
        prj_txt = open(prjfiles[0], "r").read()
        log.debug(
            "spatialingestor::_load_esri_shapefiles():: prj_txt = ", prj_txt
        )
        sr = osr.SpatialReference()
        sr.ImportFromESRI([prj_txt])
        log.debug("spatialingestor::_load_esri_shapefiles():: sr = ", sr)
        res = sr.AutoIdentifyEPSG()
        if res == 0:  # success
            native_crs = (
                sr.GetAuthorityName(None) + ":" + sr.GetAuthorityCode(None)
            )
        else:
            mapping = {
                "EPSG:28356": ["GDA_1994_MGA_Zone_56", "GDA94_MGA_zone_56"],
                "EPSG:28355": ["GDA_1994_MGA_Zone_55", "GDA94_MGA_zone_55"],
                "EPSG:28354": ["GDA_1994_MGA_Zone_54", "GDA94_MGA_zone_54"],
                "EPSG:4283": [
                    "GCS_GDA_1994",
                    'GEOGCS["GDA94",DATUM["D_GDA_1994",SPHEROID["GRS_1980"',
                ],
                "ESRI:102029": ["Asia_South_Equidistant_Conic"],
                "EPSG:3577": ["Australian_Albers_Equal_Area_Conic_WGS_1984"],
                "EPSG:3857": ["WGS_1984_Web_Mercator_Auxiliary_Sphere"],
                "EPSG:4326": [
                    "MapInfo Generic Lat/Long",
                    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984"',
                ],
            }
            for key, values in mapping.items():
                if any([v in prj_txt for v in values]):
                    native_crs = key
                    break
            else:
                # If searching the mapping items yielded nothing,
                # assign default CRS.
                native_crs = "EPSG:4326"
                # failure(
                #    dataset['title'] + " has unknown projection: " + prj_txt)
    else:
        # if wyndham then GDA_1994_MGA_Zone_55 EPSG:28355
        native_crs = "EPSG:4326"

    pargs = [
        " ",
        "-f",
        "PostgreSQL",
        "--config",
        "PG_USE_COPY",
        "YES",
        _get_db_param_string(_get_db_settings()),
        tempdir,
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "PRECISION=NO",
        "-nln",
        table_name,
        "-t_srs",
        native_crs,
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-overwrite",
    ]

    res = ogr2ogr.main(pargs)
    if not res:
        _failure("Ogr2ogr: Failed to convert file to PostGIS")

    return native_crs


def _load_kml_resources(kml_res: dict[str, Any], table_name: str) -> str:
    kml_res["url"] = kml_res["url"].replace("https", "http")
    log.debug("Using KML file " + kml_res["url"])
    native_crs = "EPSG:4326"
    # if kml ogr2ogr http://gis.stackexchange.com/questions/33102
    # /how-to-import-kml-file-with-custom-data-to-postgres-postgis-database
    if kml_res["format"] == "kmz" or "kmz" in kml_res["url"].lower():
        with open("input.zip", "wb") as f:
            f.write(urllib.request.urlopen(kml_res["url"]).read())
        subprocess.call(["unzip", "-j", "input.zip"])
        log.debug("KMZ unziped")
        kmlfiles = glob.glob("*.[kK][mM][lL]")
        if len(kmlfiles) == 0:
            _failure("No kml files found in zip " + kml_res["url"])
        else:
            kml_file = kmlfiles[0]
    else:
        filepath, headers = urllib.urlretrieve(kml_res["url"], "input.kml")
        kml_file = "input.kml"

    log.debug("Changing kml folder name in " + kml_file)
    tree = et.parse(kml_file)
    element = tree.xpath(
        "//kml:Folder/kml:name",
        namespaces={"kml": "http://www.opengis.net/kml/2.2"},
    )
    if 0 in element:
        element[0].text = table_name
    else:
        log.debug("No kml:Folder tag found")
    find = et.ETXPath(
        "//{http://www.opengis.net/kml/2.2}Folder"
        "/{http://www.opengis.net/kml/2.2}name"
    )
    element = find(tree)
    if len(element):
        for x in range(0, len(element)):
            log.debug(element[x].text)
            element[x].text = table_name
    else:
        log.debug("no Folder tag found")
    find = et.ETXPath(
        "//{http://earth.google.com/kml/2.1}Folder"
        "/{http://earth.google.com/kml/2.1}name"
    )
    element = find(tree)
    if len(element):
        for x in range(0, len(element)):
            element[x].text = table_name
    else:
        log.debug("no Folder tag found")
    with open(table_name + ".kml", "w") as ofile:
        ofile.write(et.tostring(tree))
    log.debug("converting to pgsql " + table_name + ".kml")

    pargs = [
        "",
        "-f",
        "PostgreSQL",
        "--config",
        "PG_USE_COPY",
        "YES",
        _get_db_param_string(_get_db_settings()),
        table_name + ".kml",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "PRECISION=NO",
        "-nln",
        table_name,
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-t_srs",
        native_crs,
        "-overwrite",
    ]

    res = ogr2ogr.main(pargs)
    if not res:
        _failure("Ogr2ogr: Failed to convert file to PostGIS")

    return native_crs


def _load_tab_resources(tab_res: dict[str, Any], table_name: str) -> str:
    url = tab_res["url"].replace("https", "http")
    log.debug("using TAB file " + url)
    with open("input.zip", "wb") as f:
        f.write(urllib.request.urlopen(tab_res["url"]).read())
    log.debug("TAB archive downloaded")

    subprocess.call(["unzip", "-j", "input.zip"])
    log.debug("TAB unziped")

    tabfiles = glob.glob("*.[tT][aA][bB]")
    if len(tabfiles) == 0:
        _failure("No mapinfo tab files found in zip " + tab_res["url"])

    tab_file = tabfiles[0]

    native_crs = "EPSG:4326"

    pargs = [
        "",
        "-f",
        "PostgreSQL",
        "--config",
        "PG_USE_COPY",
        "YES",
        _get_db_param_string(_get_db_settings()),
        tab_file,
        "-nln",
        table_name,
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "PRECISION=NO",
        "-t_srs",
        native_crs,
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-overwrite",
    ]

    res = ogr2ogr.main(pargs)
    log.debug(res)
    if not res:
        _failure("Ogr2ogr: Failed to convert file to PostGIS")
    os.environ["PGCLIENTENCODING"] = "windows-1252"
    res = ogr2ogr.main(pargs)
    log.debug(res)
    if not res:
        _failure("Ogr2ogr: Failed to convert file to PostGIS")

    return native_crs


def _load_tiff_resources(tiff_res: dict[str, Any], table_name: str) -> str:
    url = tiff_res["url"].replace("https", "http")
    log.debug("using GeoTIFF file " + url)

    if not any([url.lower().endswith(x) for x in ["tif", "tiff"]]):
        filepath, headers = urllib.urlretrieve(url, "input.zip")
        log.debug("GeoTIFF archive downloaded")

        subprocess.call(["unzip", "-j", filepath])
        log.debug("GeoTIFF unziped")
    else:
        urllib.urlretrieve(url, "input.tiff")

    tifffiles = glob.glob("*.[tT][iI][fF]") + glob.glob("*.[tT][iI][fF][fF]")
    if len(tifffiles) == 0:
        _failure("No TIFF files found in " + tiff_res["url"])

    native_crs = "EPSG:4326"

    large_file = os.stat(tifffiles[0]).st_size > long(
        tk.config.get("ckanext.datagovau.spatialingestor.large_file_threshold")
    )

    if large_file:
        pargs = [
            "gdal_translate",
            "-ot",
            "Byte",
            tifffiles[0],
            table_name + "_temp.tiff",
        ]

        subprocess.call(pargs)

        pargs = [
            "gdalwarp",
            "--config",
            "GDAL_CACHEMAX",
            "500",
            "-wm",
            "500",
            "-multi",
            "-t_srs",
            native_crs,
            "-of",
            "GTiff",
            "-co",
            "TILED=YES",
            "-co",
            "TFW=YES",
            "-co",
            "BIGTIFF=YES",
            "-co",
            "COMPRESS=CCITTFAX4",
            "-co",
            "NBITS=1",
            table_name + "_temp.tiff",
            table_name + ".tiff",
        ]
    else:
        pargs = [
            "gdalwarp",
            "--config",
            "GDAL_CACHEMAX",
            "500",
            "-wm",
            "500",
            "-multi",
            "-t_srs",
            native_crs,
            "-of",
            "GTiff",
            "-co",
            "TILED=YES",
            "-co",
            "TFW=YES",
            "-co",
            "BIGTIFF=YES",
            "-co",
            "COMPRESS=PACKBITS",
            tifffiles[0],
            table_name + ".tiff",
        ]

    subprocess.call(pargs)

    data_output_dir = _create_geoserver_data_dir(table_name)

    if large_file:
        pargs = [
            "",
            "-v",
            "-r",
            "near",
            "-levels",
            "3",
            "-ps",
            "1024",
            "1024",
            "-co",
            "TILED=YES",
            "-co",
            "COMPRESS=CCITTFAX4",
            "-co",
            "NBITS=1",
            "-targetDir",
            data_output_dir,
            table_name + ".tiff",
        ]
    else:
        pargs = [
            "",
            "-v",
            "-r",
            "near",
            "-levels",
            "3",
            "-ps",
            "1024",
            "1024",
            "-co",
            "TILED=YES",
            "-co",
            "COMPRESS=PACKBITS",
            "-targetDir",
            data_output_dir,
            table_name + ".tiff",
        ]

    gdal_retile.main(pargs)

    _set_geoserver_ownership(data_output_dir)
    return native_crs


def _load_grid_resources(
    grid_res: dict[str, Any], table_name: str, tempdir: str
) -> str:
    grid_res["url"] = grid_res["url"].replace("https", "http")
    log.debug("Using ArcGrid file " + grid_res["url"])

    filepath, headers = urllib.urlretrieve(grid_res["url"], "input.zip")
    log.debug("ArcGrid downloaded")

    subprocess.call(["unzip", "-j", filepath])
    log.debug("ArcGrid unzipped")

    native_crs = "EPSG:4326"

    pargs = [
        "gdalwarp",
        "--config",
        "GDAL_CACHEMAX",
        "500",
        "-wm",
        "500",
        "-multi",
        "-t_srs",
        native_crs,
        "-of",
        "GTiff",
        "-co",
        "TILED=YES",
        "-co",
        "TFW=YES",
        "-co",
        "BIGTIFF=YES",
        "-co",
        "COMPRESS=PACKBITS",
        tempdir,
        table_name + "_temp1.tiff",
    ]

    subprocess.call(pargs)

    large_file = os.stat(table_name + "_temp1.tiff").st_size > long(
        tk.config.get("ckanext.datagovau.spatialingestor.large_file_threshold")
    )

    if large_file:
        pargs = [
            "gdal_translate",
            "-ot",
            "Byte",
            table_name + "_temp1.tiff",
            table_name + "_temp2.tiff",
        ]

        subprocess.call(pargs)

        pargs = [
            "gdalwarp",
            "--config",
            "GDAL_CACHEMAX",
            "500",
            "-wm",
            "500",
            "-multi",
            "-t_srs",
            native_crs,
            "-of",
            "GTiff",
            "-co",
            "TILED=YES",
            "-co",
            "TFW=YES",
            "-co",
            "BIGTIFF=YES",
            "-co",
            "COMPRESS=CCITTFAX4",
            "-co",
            "NBITS=1",
            table_name + "_temp2.tiff",
            table_name + ".tiff",
        ]
    else:
        pargs = [
            "gdalwarp",
            "--config",
            "GDAL_CACHEMAX",
            "500",
            "-wm",
            "500",
            "-multi",
            "-t_srs",
            native_crs,
            "-of",
            "GTiff",
            "-co",
            "TILED=YES",
            "-co",
            "TFW=YES",
            "-co",
            "BIGTIFF=YES",
            "-co",
            "COMPRESS=PACKBITS",
            table_name + "_temp1.tiff",
            table_name + ".tiff",
        ]

    subprocess.call(pargs)

    data_output_dir = _create_geoserver_data_dir(table_name)

    if large_file:
        pargs = [
            "",
            "-v",
            "-r",
            "near",
            "-levels",
            "3",
            "-ps",
            "1024",
            "1024",
            "-co",
            "TILED=YES",
            "-co",
            "COMPRESS=CCITTFAX4",
            "-co",
            "NBITS=1",
            "-targetDir",
            data_output_dir,
            table_name + ".tiff",
        ]
    else:
        pargs = [
            "",
            "-v",
            "-r",
            "near",
            "-levels",
            "3",
            "-ps",
            "1024",
            "1024",
            "-co",
            "TILED=YES",
            "-co",
            "COMPRESS=PACKBITS",
            "-targetDir",
            data_output_dir,
            table_name + ".tiff",
        ]

    gdal_retile.main(pargs)

    _set_geoserver_ownership(data_output_dir)
    return native_crs


def _apply_sld(
    name: str, workspace: str, layer_name: str, url=None, filepath=None
):
    server = get_geoserver()

    if url:
        r = server.get_style(workspace, name)
        if r.ok:
            with open("input.sld", "wb") as f:
                f.write(r.content)
            filepath = "input.sld"
        else:
            log.error("error downloading SLD")
            return
    elif filepath:
        log.info("sld downloaded")
        pass
    else:
        log.error("error accessing SLD")
        return

    r = server.get_style(workspace, name, quiet=True)
    if r.ok:
        log.info("Delete out old style in workspace")
        server.delete_style(workspace, name)

    server.create_style(
        workspace, {"style": {"name": name, "filename": name + ".sld"}}
    )

    sld_text = open(filepath, "r").read()
    mapping = {
        "application/vnd.ogc.sld+xml": "www.opengis.net/sld",
        "application/vnd.ogc.se+xml": "www.opengis.net/se",
    }
    for key, value in mapping.items():
        if value in sld_text:
            content_type = key
            break
    else:
        log.error("couldn't pick a sld content type")
        return

    payload = open(filepath, "rb")
    log.info("sld content type: %s", content_type)

    r = server.update_style(workspace, name, payload, content_type, raw=True)

    if r.status_code == 400:
        log.info("Delete out old style in workspace")
        server.delete_style(workspace, name)
        server.update_style(workspace, name, payload, content_type, raw=False)

    server.add_style(
        workspace,
        layer_name,
        name,
        {"layer": {"defaultStyle": {"name": name, "workspace": workspace}}},
    )


def _apply_sld_resources(
    sld_res: dict[str, Any], workspace: str, layer_name: str
):
    # Procedure for updating layer to use default style comes from
    # http://docs.geoserver.org/stable/en/user/rest/examples/curl.html that
    # explains the below steps in the 'Changing a layer style' section

    name = os.path.splitext(os.path.basename(sld_res["url"]))[0]

    server = get_geoserver()
    with server._session() as sess:
        r = sess.get(sld_res["url"])
        if r.ok:
            _apply_sld(
                name, workspace, layer_name, url=sld_res["url"], filepath=None
            )
        else:
            log.error("could not download SLD resource")


def _convert_resources(
    table_name: str, temp_dir: str, resources: GroupedResources
) -> tuple[bool, bool, str]:
    using_kml = False
    using_grid = False
    native_crs = ""

    if len(resources.shp):
        native_crs = _load_esri_shapefiles(
            resources.shp[0], table_name, temp_dir
        )
    elif len(resources.kml):
        using_kml = True
        native_crs = _load_kml_resources(resources.kml[0], table_name)
    elif len(resources.tab):
        native_crs = _load_tab_resources(resources.tab[0], table_name)
    elif len(resources.tiff):
        using_grid = True
        native_crs = _load_tiff_resources(resources.tiff[0], table_name)
    elif len(resources.grid):
        using_grid = True
        native_crs = _load_grid_resources(
            resources.grid[0], table_name, temp_dir
        )

    return using_kml, using_grid, native_crs


def _get_geojson(using_kml: bool, table_name: str) -> tuple[str, str, str]:
    cur, conn = _get_cursor()
    if using_kml:
        try:
            cur.execute(
                (
                    'alter table "{}" DROP "description" RESTRICT, '
                    "DROP timestamp RESTRICT, DROP begin RESTRICT, "
                    'DROP "end" RESTRICT, DROP altitudemode RESTRICT, '
                    "DROP tessellate RESTRICT, DROP extrude RESTRICT, "
                    "DROP visibility RESTRICT, DROP draworder RESTRICT, "
                    "DROP icon RESTRICT;"
                ).format(table_name)
            )
        except Exception:
            log.error("KML error", exc_info=True)
    select_query = (
        "SELECT ST_Extent(geom) as box,"
        "ST_Extent(ST_Transform(geom,4326)) as latlngbox, "
        "ST_AsGeoJSON(ST_Extent(ST_Transform(geom,4326))) as geojson "
        'from "{}"'
    ).format(table_name)
    cur.execute(select_query)
    # logger.debug(select_query)

    bbox, latlngbbox, bgjson = cur.fetchone()
    cur.close()
    conn.close()
    return bbox, latlngbbox, bgjson


def _perform_workspace_requests(
    datastore: str, workspace: str, table_name: Optional[str]
):
    if not table_name:
        dsdata = {
            "dataStore": {
                "name": datastore,
                "connectionParameters": {
                    "dbtype": "postgis",
                    "encode functions": "false",
                    "jndiReferenceName": "java:comp/env/jdbc/postgres",
                    # jndi name you have setup in tomcat http://docs.geoserver.org
                    # /stable/en/user/tutorials/tomcat-jndi/tomcat-jndi.html
                    # #configuring-a-postgresql-connection-pool
                    "Support on the fly geometry simplification": "true",
                    "Expose primary keys": "false",
                    "Estimated extends": "false",
                },
            }
        }
    else:
        dsdata = {
            "coverageStore": {
                "name": datastore,
                "type": "ImagePyramid",
                "enabled": True,
                "url": "file:data/" + table_name,
                "workspace": workspace,
            }
        }

    log.debug("_perform_workspace_requests():: dsdata = %s", dsdata)

    server = get_geoserver()
    r = server.create_store(workspace, bool(table_name), dsdata)
    log.debug("_perform_workspace_requests():: r = %s", r)

    if not r.ok:
        _failure(f"Failed to create Geoserver store {r.url}: {r.content}")


def _update_package_with_bbox(
    bbox, latlngbbox, ftdata, dataset, native_crs, bgjson
):
    def _clear_box(string):
        return (
            string.replace("BOX", "")
            .replace("(", "")
            .replace(")", "")
            .replace(",", " ")
            .split(" ")
        )

    minx, miny, maxx, maxy = _clear_box(bbox)
    bbox_obj = {
        "minx": minx,
        "maxx": maxx,
        "miny": miny,
        "maxy": maxy,
        "crs": native_crs,
    }

    llminx, llminy, llmaxx, llmaxy = _clear_box(latlngbbox)
    llbbox_obj = {
        "minx": llminx,
        "maxx": llmaxx,
        "miny": llminy,
        "maxy": llmaxy,
        "crs": "EPSG:4326",
    }

    ftdata["featureType"]["nativeBoundingBox"] = bbox_obj
    ftdata["featureType"]["latLonBoundingBox"] = llbbox_obj
    if float(llminx) < -180 or float(llmaxx) > 180:
        log.debug("Invalid projection: %s", ftdata)
        _failure(
            dataset["title"]
            + " has invalid automatic projection: "
            + native_crs
        )

    ftdata["featureType"]["srs"] = native_crs
    # logger.debug('bgjson({}), llbox_obj({})'.format(bgjson, llbbox_obj))
    if "spatial" not in dataset or dataset["spatial"] != bgjson:
        dataset["spatial"] = bgjson
        call_action("package_update", dataset)
    return bbox_obj


def _create_resources_from_formats(
    ws_addr, layer_name, bbox_obj, existing_formats, dataset, using_grid
):
    bbox_str = (
        "&bbox="
        + bbox_obj["minx"]
        + ","
        + bbox_obj["miny"]
        + ","
        + bbox_obj["maxx"]
        + ","
        + bbox_obj["maxy"]
        if bbox_obj
        else ""
    )

    for _format in _get_target_formats():  # ['kml', 'image/png']:
        url = (
            ws_addr
            + "wms?request=GetMap&layers="
            + layer_name
            + bbox_str
            + "&width=512&height=512&format="
            + urllib.parse.quote(_format)
        )
        if _format == "image/png" and _format not in existing_formats:
            log.debug("Creating PNG Resource")
            call_action(
                "resource_create",
                {
                    "package_id": dataset["id"],
                    "name": dataset["title"] + " Preview Image",
                    "description": "View overview image of this dataset",
                    "format": _format,
                    "url": url,
                    "last_modified": datetime.now().isoformat(),
                },
            )
        elif _format == "kml":
            if _format not in existing_formats:
                log.debug("Creating KML Resource")
                call_action(
                    "resource_create",
                    {
                        "package_id": dataset["id"],
                        "name": dataset["title"] + " KML",
                        "description": (
                            "View a map of this dataset in web "
                            "and desktop spatial data tools"
                            " including Google Earth"
                        ),
                        "format": _format,
                        "url": url,
                        "last_modified": datetime.now().isoformat(),
                    },
                )
        elif _format in ["wms", "wfs"] and _format not in existing_formats:
            if _format == "wms":
                log.debug("Creating WMS API Endpoint Resource")
                call_action(
                    "resource_create",
                    {
                        "package_id": dataset["id"],
                        "name": dataset["title"]
                        + " - Preview this Dataset (WMS)",
                        "description": (
                            "View the data in this "
                            "dataset online via an online map"
                        ),
                        "format": "wms",
                        "url": ws_addr + "wms?request=GetCapabilities",
                        "wms_layer": layer_name,
                        "last_modified": datetime.now().isoformat(),
                    },
                )
            else:
                log.debug("Creating WFS API Endpoint Resource")
                call_action(
                    "resource_create",
                    {
                        "package_id": dataset["id"],
                        "name": dataset["title"]
                        + " Web Feature Service API Link",
                        "description": (
                            "WFS API Link for use in Desktop GIS tools"
                        ),
                        "format": "wfs",
                        "url": ws_addr + "wfs",
                        "wfs_layer": layer_name,
                        "last_modified": datetime.now().isoformat(),
                    },
                )
        elif _format in ["json", "geojson"] and not using_grid:
            url = (
                ws_addr
                + "wfs?request=GetFeature&typeName="
                + layer_name
                + "&outputFormat="
                + urllib.parse.quote("json")
            )
            if not any([x in existing_formats for x in ["json", "geojson"]]):
                log.debug("Creating GeoJSON Resource")
                call_action(
                    "resource_create",
                    {
                        "package_id": dataset["id"],
                        "name": dataset["title"] + " GeoJSON",
                        "description": (
                            "For use in web-based data "
                            "visualisation of this collection"
                        ),
                        "format": "geojson",
                        "url": url,
                        "last_modified": datetime.now().isoformat(),
                    },
                )


def _delete_resources(dataset):
    geoserver_resources = [
        res
        for res in dataset["resources"]
        if "/geoserver" in res["url"]
        for old_host in ["dga.links.com.au", "data.gov.au"]
        if old_host in res["url"]
    ]

    for res in geoserver_resources:
        call_action("resource_delete", res, True)


def _prepare_everything(
    dataset: dict[str, Any], resources: GroupedResources, tempdir: str
) -> tuple[bool, bool, str, str, str]:
    table_name = _clear_old_table(dataset)
    _clean_dir(_get_geoserver_data_dir(table_name))

    using_kml, using_grid, native_crs = _convert_resources(
        table_name, tempdir, resources
    )

    server = get_geoserver()
    workspace = server.into_workspace(dataset["name"])

    log.debug("_prepare_everything():: GeoServer host = %s", server.host)
    log.debug("_prepare_everything():: workspace = %s", workspace)

    if server.check_workspace(workspace):
        server.drop_workspace(workspace)

    r = server.create_workspace(workspace)

    log.debug(
        "_prepare_everything():: Workspace creation request result r = %s", r
    )
    if not r.ok:
        _failure(f"Failed to create Geoserver workspace: {r.content}")

    # load bounding boxes from database
    return using_kml, using_grid, table_name, workspace, native_crs


def clean_assets(dataset_id: str, skip_grids: bool = False):
    dataset = _get_dataset(dataset_id)
    if not dataset:
        return

    # Skip cleaning datasets that may have a manually ingested grid
    is_grid = {"grid", "geotif"} & {
        r["format"].lower() for r in dataset["resources"]
    }
    if skip_grids and is_grid:
        return

    # clear old data table
    table_name = _clear_old_table(dataset)

    # clear rasterised directory
    _clean_dir(_get_geoserver_data_dir(table_name))

    server = get_geoserver()
    workspace = server.into_workspace(dataset["name"])

    if server.check_workspace(workspace):
        server.drop_workspace(workspace)

    _delete_resources(dataset)


def do_ingesting(dataset_id: str, force: bool):
    if not force and may_skip(dataset_id):
        return

    dataset = _get_dataset(dataset_id)
    assert dataset, "Dataset cannot be missing"
    log.info("Ingesting %s", dataset["id"])
    resources = GroupedResources.from_dataset(dataset)

    with utils.temp_dir(dataset["id"], "/tmp") as tempdir:
        log.debug("do_ingesting():: tempdir = %s", tempdir)
        os.chdir(tempdir)

        try:
            (
                using_kml,
                using_grid,
                table_name,
                workspace,
                native_crs,
            ) = _prepare_everything(dataset, resources, tempdir)
        except IngestionFail as e:
            log.info("%s: %s", type(e), e)
            clean_assets(dataset_id)
            return

        datastore = workspace + ("cs" if using_grid else "ds")
        log.debug(
            "do_ingesting():: before _perform_workplace_requests().  datastore"
            " = %s",
            datastore,
        )
        try:
            _perform_workspace_requests(
                datastore, workspace, table_name if using_grid else None
            )
        except IngestionFail as e:
            log.info("{}: {}".format(type(e), e))
            clean_assets(dataset_id)
            return

        server = get_geoserver()
        layer_name = table_name

        if using_grid:
            layer_data = {
                "coverage": {
                    "name": layer_name,
                    "nativeName": table_name,
                    "title": dataset["title"],
                    "srs": native_crs,
                    "coverageParameters": {
                        "AllowMultithreading": False,
                        "SUGGESTED_TILE_SIZE": "1024,1024",
                        "USE_JAI_IMAGEREAD": False,
                    },
                }
            }
        else:
            layer_data = {
                "featureType": {
                    "name": layer_name,
                    "nativeName": table_name,
                    "title": dataset["title"],
                    "srs": native_crs,
                    "datastore": datastore,
                }
            }

        bbox_obj = None
        try:
            if not using_grid:
                bbox, latlngbbox, bgjson = _get_geojson(using_kml, table_name)
                bbox_obj = (
                    _update_package_with_bbox(
                        bbox,
                        latlngbbox,
                        layer_data,
                        dataset,
                        native_crs,
                        bgjson,
                    )
                    if bbox
                    else None
                )
            r = server.create_layer(
                workspace, using_grid, datastore, layer_data
            )
            if not r.ok:
                _failure(
                    f"Failed to create Geoserver layer {r.url}: {r.content}"
                )
        except IngestionFail as e:
            log.info("{}: {}".format(type(e), e))
            clean_assets(dataset_id)
            return

        sldfiles = glob.glob("*.[sS][lL][dD]")
        log.debug(sldfiles, resources.sld)
        if len(sldfiles):
            _apply_sld(
                os.path.splitext(os.path.basename(sldfiles[0]))[0],
                workspace,
                layer_name,
                url=None,
                filepath=sldfiles[0],
            )
        else:
            log.info("no sld file in package")

        # With layers created, we can apply any SLDs
        if len(resources.sld):
            if resources.sld[0].get("url", "") == "":
                log.info("bad sld resource url")
            else:
                _apply_sld_resources(resources.sld[0], workspace, layer_name)
        else:
            log.info("no sld resources or sld url invalid")

        # Delete out all geoserver resources before rebuilding (this simplifies update logic)
        _delete_resources(dataset)
        dataset = _get_dataset(dataset["id"])
        assert dataset

        existing_formats = []
        for resource in dataset["resources"]:
            existing_formats.append(resource["format"].lower())

        ws_addr = (
            server.public_url
            + "/"
            + server.into_workspace(dataset["name"])
            + "/"
        )
        _create_resources_from_formats(
            ws_addr,
            layer_name,
            bbox_obj,
            existing_formats,
            dataset,
            using_grid,
        )

        log.info("Completed!")

        # TODO: should we `clean_assets(dataset_id)` here?


def may_skip(dataset_id: str) -> bool:
    """Skip blacklisted orgs, datasets and datasets, updated by bot."""
    log.debug("Check if may skip %s", dataset_id)
    dataset = _get_dataset(dataset_id)

    if not dataset:
        log.debug("No package found to ingest")
        return True

    org = dataset.get("organization", {}).get("name")
    if not org:
        log.debug(
            "Package must be associate with valid organization to be ingested"
        )
        return True

    if org in _get_blacklisted_orgs():
        log.debug("%s in omitted_orgs blacklist", org)
        return True

    if dataset["name"] in _get_blacklisted_pkgs():
        log.debug("%s in omitted_pkgs blacklist", dataset["name"])
        return True

    if dataset.get("harvest_source_id") or tk.asbool(
        dataset.get("spatial_harvester")
    ):
        log.debug("Harvested datasets are not eligible for ingestion")
        return True

    if dataset["private"]:
        log.debug("Private datasets are not eligible for ingestion")
        return True

    if dataset["state"] != "active":
        log.debug("Dataset must be active to ingest")
        return True

    # SLD resources(last group) are not checked
    grouped_resources = GroupedResources.from_dataset(dataset)[:-1]

    if not any(grouped_resources):
        log.debug("No geodata format files detected")
        return True

    if any(len(x) > 1 for x in grouped_resources):
        log.debug("Can not determine unique spatial file to ingest")
        return True

    activity_list = call_action("package_activity_list", {"id": dataset["id"]})

    user = call_action("user_show", {"id": _get_username()}, True)

    if activity_list and activity_list[0]["user_id"] == user["id"]:
        log.debug("Not updated since last ingest")
        return True

    return False


def _get_dataset(dataset_id: str) -> Optional[dict[str, Any]]:
    with contextlib.suppress(tk.ObjectNotFound):
        return call_action("package_show", {"id": dataset_id}, True)


def _get_username():
    return tk.config.get("ckanext.datagovau.spatialingestor.username", "")


def _get_blacklisted_orgs() -> list[str]:
    return tk.aslist(
        tk.config.get("ckanext.datagovau.spatialingestor.org_blacklist", [])
    )


def _get_blacklisted_pkgs() -> list[str]:
    return tk.aslist(
        tk.config.get("ckanext.datagovau.spatialingestor.pkg_blacklist", [])
    )


def _get_target_formats() -> list[str]:
    return tk.aslist(
        tk.config.get("ckanext.datagovau.spatialingestor.target_formats", [])
    )


def _get_source_formats() -> list[str]:
    return tk.aslist(
        tk.config.get("ckanext.datagovau.spatialingestor.source_formats", [])
    )


def _get_datastore_url() -> str:
    return tk.config["ckanext.datagovau.spatialingestor.datastore.url"]


def call_action(action: str, data: dict[str, Any], ignore_auth=False) -> Any:
    return tk.get_action(action)(
        {"user": _get_username(), "ignore_auth": ignore_auth}, data
    )
