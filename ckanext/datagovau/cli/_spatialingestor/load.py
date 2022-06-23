from __future__ import annotations

import glob
import logging
import os
import subprocess
import zipfile
from typing import Any

import lxml.etree as et
from osgeo import osr
from osgeo_utils import gdal_retile
from osgeo_utils.samples import ogr2ogr

from ckanext.datagovau import utils

from . import config
from .exc import fail

log = logging.getLogger(__name__)


CRS_MAPPING = {
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


def esri(resource: dict[str, Any], table_name: str, tempdir: str) -> str:
    log.debug("_load_esri_shapefiles():: shp_res = %s", resource["id"])
    log.debug("Using SHP file %s", resource["url"])

    if any(resource["url"].lower().endswith(x) for x in ["shp", "shapefile"]):
        utils.download(resource["url"], "input.shp")
    else:
        utils.download(resource["url"], "input.zip")
        archive = zipfile.ZipFile("input.zip")
        archive.extractall()

    shpfiles = glob.glob("*.[sS][hH][pP]")
    prjfiles = glob.glob("*.[pP][rR][jJ]")
    if not shpfiles:
        fail("No shp files found in zip " + resource["url"])
    log.debug(f"converting to pgsql {table_name} {shpfiles[0]}")

    if prjfiles:
        prj_txt = open(prjfiles[0], "r").read()
        log.debug(
            "spatialingestor::_load_esri_shapefiles():: prj_txt = %s", prj_txt
        )
        sr = osr.SpatialReference()
        sr.ImportFromESRI([prj_txt])
        log.debug("spatialingestor::_load_esri_shapefiles():: sr = %s", sr)
        res = sr.AutoIdentifyEPSG()
        if res == 0:  # success
            native_crs = (
                sr.GetAuthorityName(None) + ":" + sr.GetAuthorityCode(None)
            )
        else:
            for key, values in CRS_MAPPING.items():
                if utils.contains(prj_txt, values):
                    native_crs = key
                    break
            else:
                # If searching the mapping items yielded nothing,
                # assign default CRS.
                native_crs = "EPSG:4326"

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
        config.db_param(),
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

    exit_code = _call_ogr2ogr(pargs)
    if exit_code:
        fail("Ogr2ogr: Failed to convert file to PostGIS")

    return native_crs


def kml(resource: dict[str, Any], table_name: str) -> str:
    log.debug("Using KML file %s", resource["url"])
    native_crs = "EPSG:4326"
    # if kml ogr2ogr http://gis.stackexchange.com/questions/33102
    # /how-to-import-kml-file-with-custom-data-to-postgres-postgis-database
    if resource["format"] == "kmz" or "kmz" in resource["url"].lower():
        utils.download(resource["url"], "input.zip")
        archive = zipfile.ZipFile("input.zip")
        archive.extractall()
        log.debug("KMZ unziped")

        kmlfiles = glob.glob("*.[kK][mM][lL]")
        if len(kmlfiles) == 0:
            fail("No kml files found in zip " + resource["url"])
        else:
            kml_file = kmlfiles[0]
    else:
        utils.download(resource["url"], "input.kml")
        kml_file = "input.kml"

    log.debug("Changing kml folder name in %s", kml_file)
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
    with open(table_name + ".kml", "wb") as ofile:
        ofile.write(et.tostring(tree))
    log.debug("converting to pgsql %s.kml", table_name)

    pargs = [
        "",
        "-f",
        "PostgreSQL",
        "--config",
        "PG_USE_COPY",
        "YES",
        config.db_param(),
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

    exit_code = _call_ogr2ogr(pargs)
    if exit_code:
        fail("Ogr2ogr: Failed to convert file to PostGIS")

    return native_crs


def tab(resource: dict[str, Any], table_name: str) -> str:
    log.debug("using TAB file %s", resource["url"])
    utils.download(resource["url"], "input.zip")
    log.debug("TAB archive downloaded")
    archive = zipfile.ZipFile("input.zip")
    archive.extractall()
    log.debug("TAB unziped")

    tabfiles = glob.glob("*.[tT][aA][bB]")
    if len(tabfiles) == 0:
        fail("No mapinfo tab files found in zip " + resource["url"])

    tab_file = tabfiles[0]

    native_crs = "EPSG:4326"

    pargs = [
        "",
        "-f",
        "PostgreSQL",
        "--config",
        "PG_USE_COPY",
        "YES",
        config.db_param(),
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

    exit_code = _call_ogr2ogr(pargs)
    log.debug(exit_code)
    if exit_code:
        fail("Ogr2ogr: Failed to convert file to PostGIS")
    os.environ["PGCLIENTENCODING"] = "windows-1252"

    exit_code = _call_ogr2ogr(pargs)
    log.debug(exit_code)
    if exit_code:
        fail("Ogr2ogr: Failed to convert file to PostGIS")

    return native_crs


def tiff(resource: dict[str, Any], table_name: str) -> str:
    log.debug("using GeoTIFF file %s", resource["url"])

    if not any([resource["url"].lower().endswith(x) for x in ["tif", "tiff"]]):
        utils.download(resource["url"], "input.zip")
        log.debug("GeoTIFF archive downloaded")

        archive = zipfile.ZipFile("input.zip")
        archive.extractall()
        log.debug("GeoTIFF unziped")
    else:
        utils.download(resource["url"], "input.tiff")

    tifffiles = glob.glob("*.[tT][iI][fF]") + glob.glob("*.[tT][iI][fF][fF]")
    if len(tifffiles) == 0:
        fail("No TIFF files found in " + resource["url"])

    native_crs = "EPSG:4326"

    large_file = os.stat(tifffiles[0]).st_size > config.large_size()

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


def grid(resource: dict[str, Any], table_name: str, tempdir: str) -> str:
    log.debug("Using ArcGrid file %s", resource["url"])
    utils.download(resource["url"], "input.zip")
    log.debug("ArcGrid downloaded")

    archive = zipfile.ZipFile("input.zip")
    archive.extractall()
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

    large_file = (
        os.stat(table_name + "_temp1.tiff").st_size > config.large_size()
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


def _call_ogr2ogr(pargs):
    executable = config.ogr2ogr()
    if executable:
        return subprocess.call([executable] + pargs[1:])
    return ogr2ogr.main(pargs)


def _create_geoserver_data_dir(native_name: str) -> str:
    data_output_dir = config.data_dir(native_name)
    os.makedirs(data_output_dir, exist_ok=True)
    return data_output_dir


def _set_geoserver_ownership(data_dir):
    uid, gid = config.os_owner()
    os.chown(data_dir, uid, gid)
    for root, dirs, files in os.walk(data_dir):
        for momo in dirs:
            os.chown(os.path.join(root, momo), uid, gid)
        for momo in files:
            os.chown(os.path.join(root, momo), uid, gid)
