import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any
import urllib
import zipfile
import contextlib
from datetime import datetime

import click
import ckanapi

import ckan.plugins.toolkit as tk

path = None


@contextlib.contextmanager
def temp_dir(suffix: str, dir: str):
    path = tempfile.mkdtemp(suffix=suffix, dir=dir)
    try:
        yield path
    finally:
        shutil.rmtree(path)


@click.group()
def maintain():
    """Maintenance tasks"""
    pass


@maintain.command()
@click.argument("id_", metavar="DATASET_ID")
@click.option("--tmp-dir", "/tmp")
@click.option("-u", "--username")
def zip_extract(id_, tmp_dir, username=None):
    """ZIP extractor for data.gov.au"""
    ckan = ckanapi.LocalCKAN(username)
    try:
        dataset = ckan.action.package_show(id=id_)
    except ckanapi.NotFound:
        tk.error_shout(f"Dataset {id_} not found. ")
        raise click.Abort()

    current_user = ckan.action.user_show(id=ckan.username)
    activity_list = ckan.action.package_activity_list(id=dataset["id"])
    # checking that bot was not last editor ensures no infinite loop
    # todo scan for last date of non-bot edit
    # Or last modified date could be compared with zip_extracted resources scan dates set.
    if activity_list and activity_list[0]["user_id"] == current_user["id"]:
        click.secho("No changes since last extraction.", fg="green")
        return

    for resource in dataset["resources"]:
        with temp_dir(dataset["id"], tmp_dir) as path:
            _extract_resource(resource, ckan, path)


def _extract_resource(
    resource: dict[str, Any], ckan: ckanapi.LocalCKAN, path: str
):
    if resource["format"].lower() != "zip":
        return
    if not resource.get("zip_extract", ""):
        return

    # download resource to tmpfile
    os.chdir(path)

    # urlretrieve does not work with https. We can't really get files through an https connection without
    # going through a proxy
    print("using ZIP file " + resource["url"].replace("https", "http"))
    (filepath, headers) = urllib.urlretrieve(
        resource["url"].replace("https", "http"), "input.zip"
    )
    print("zip downloaded")
    # use unzip program rather than python's zipfile library for maximum compatibility
    rv = subprocess.call(["unzip", filepath])
    # with ZipFile(filepath, 'r') as myzip:
    #    myzip.extractall()
    # zipfile.ZipFile(filepath, 'r').extractall()
    print("zip unzipped")

    interesting_extensions = [
        "csv",
        "xls",
        "xlsx",
        "json",
        "geojson",
        "shp",
        "kml",
    ]
    # Multiple files transform to multiple file resources
    resource_files = []

    def update_resource(file, path):
        print("updating/creating " + file)
        existing = False
        res_id = None
        for res in dataset["resources"]:
            if res["name"] == file:
                existing = True
                res["last_modified"] = datetime.now().isoformat()
                print("Updating resource {0}".format(res["id"]))
                ckan.call_action(
                    "resource_update", res, files={"upload": open(path)}
                )
                res_id = res["id"]
                break
        if not existing:
            print("Creating new resource for file")
            res = ckan.call_action(
                "resource_create",
                {
                    "package_id": dataset["id"],
                    "name": file,
                    "url": file,
                    "parent_res": resource["id"],
                    "zip_extracted": "True",
                    "last_modified": datetime.now().isoformat(),
                },
                files={"upload": open(path)},
            )
            res_id = res["id"]
        # Give datapusher 10 seconds to start
        datapusher_working = True
        datapusher_present = True
        poll_time = 10
        count = 0
        timeout = 1200
        have_displayed = False
        last_update_checked = False
        print("Checking to see if datapusher has been triggered...")
        while datapusher_working and datapusher_present and count < timeout:
            count += 1
            if count % poll_time == 0:
                try:
                    datapusher_task = ckan.call_action(
                        "task_status_show",
                        {
                            "entity_id": res_id,
                            "task_type": "datapusher",
                            "key": "datapusher",
                        },
                    )
                    datapusher_working = datapusher_task.get("state", "") in [
                        "pending",
                        "submitting",
                    ]
                    if datapusher_working and not last_update_checked:
                        if (
                            "last_updated" not in datapusher_task
                            or (
                                datetime.utcnow()
                                - date_str_to_datetime(
                                    datapusher_task["last_updated"]
                                )
                            ).total_seconds()
                            > 86400
                        ):
                            print(
                                "Datapusher is in a stale pending state,"
                                " re-submitting job..."
                            )
                            sys.stdout.write(
                                "Waiting for datapusher to ingest"
                                " resource... \\"
                            )
                            sys.stdout.flush()
                            ckan.call_action(
                                "datapusher_submit", {"resource_id": res_id}
                            )
                        last_update_checked = True
                except:
                    datapusher_present = False

            if datapusher_working and datapusher_present:
                if count == 1:
                    sys.stdout.write(
                        "Waiting for datapusher to ingest resource... \\"
                    )
                elif (count - 1) % 4 == 0:
                    sys.stdout.write(
                        "\rWaiting for datapusher to ingest resource... \\"
                    )
                elif (count - 1) % 4 == 1:
                    sys.stdout.write(
                        "\rWaiting for datapusher to ingest resource... |"
                    )
                elif (count - 1) % 4 == 2:
                    sys.stdout.write(
                        "\rWaiting for datapusher to ingest resource... /"
                    )
                elif (count - 1) % 4 == 3:
                    sys.stdout.write(
                        "\rWaiting for datapusher to ingest resource... -"
                    )
                sys.stdout.flush()
                have_displayed = True
            time.sleep(1)
        else:
            if datapusher_present:
                if have_displayed:
                    print(
                        "\nDatapusher has finished pushing resource,"
                        " continuing with Zip extraction..."
                    )
                else:
                    print(
                        "Datapusher has finished pushing resource, continuing"
                        " with Zip extraction..."
                    )

    def count_interesting(path):
        for g in os.listdir(path):
            if g.split(".").pop().lower() in interesting_extensions:
                return 1
        return 0

    def recurse_directory(path):
        if (
            len([fn for fn in os.listdir(path)]) < 3
            and len([ndir for ndir in os.listdir(path) if os.path.isdir(ndir)])
            == 1
        ):
            for f in os.listdir(path):
                if os.path.isdir(os.path.join(path, f)):
                    return recurse_directory(os.path.join(path, f))

        os.chdir(path)
        numInteresting = len(
            [
                f
                for f in os.listdir(path)
                if (
                    os.path.isfile(os.path.join(path, f))
                    and (f.split(".").pop().lower() in interesting_extensions)
                )
            ]
        )
        for f in os.listdir(path):
            if os.path.isfile(os.path.join(path, f)) and numInteresting > 1:
                if f.split(".").pop().lower() in interesting_extensions:
                    update_resource(f, os.path.join(path, f))
            if os.path.isdir(os.path.join(path, f)):
                # only zip up folders if they contain at least one interesting file
                if count_interesting(os.path.join(path, f)) > 0:
                    zipf = zipfile.ZipFile(
                        f + ".zip", "w", zipfile.ZIP_DEFLATED
                    )
                    zipdir(f, zipf)
                    zipf.close()
                    update_resource(f, f + ".zip")

    recurse_directory(path)


def date_str_to_datetime(date_str):
    """Convert ISO-like formatted datestring to datetime object.

    This function converts ISO format date- and datetime-strings into
    datetime objects.  Times may be specified down to the microsecond.  UTC
    offset or timezone information may **not** be included in the string.

    Note - Although originally documented as parsing ISO date(-times), this
           function doesn't fully adhere to the format.  This function will
           throw a ValueError if the string contains UTC offset information.
           So in that sense, it is less liberal than ISO format.  On the
           other hand, it is more liberal of the accepted delimiters between
           the values in the string.  Also, it allows microsecond precision,
           despite that not being part of the ISO format.
    """

    time_tuple = re.split("[^\d]+", date_str, maxsplit=5)

    # Extract seconds and microseconds
    if len(time_tuple) >= 6:
        m = re.match(
            "(?P<seconds>\d{2})(\.(?P<microseconds>\d{6}))?$", time_tuple[5]
        )
        if not m:
            raise ValueError(
                "Unable to parse %s as seconds.microseconds" % time_tuple[5]
            )
        seconds = int(m.groupdict().get("seconds"))
        microseconds = int(m.groupdict(0).get("microseconds"))
        time_tuple = time_tuple[:5] + [seconds, microseconds]

    return datetime(*map(int, time_tuple))


# https://stackoverflow.com/questions/1855095/how-to-create-a-zip-archive-of-a-directory
def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
