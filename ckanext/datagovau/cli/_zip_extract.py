from __future__ import annotations
import os
from datetime import datetime
import logging
import zipfile

from typing import Any, Iterable
from werkzeug.datastructures import FileStorage

import ckanapi
import requests
import ckan.plugins.toolkit as tk

CONFIG_INTERESTING_EXTENSIONS = (
    "ckanext.datagovau.zip-extractor.interesting_extensions"
)
DEFAULT_INTERESTING_EXTENSIONS = "csv"

log = logging.getLogger(__name__)


def _interesting_extensions() -> list[str]:
    return tk.aslist(
        tk.config.get(
            CONFIG_INTERESTING_EXTENSIONS, DEFAULT_INTERESTING_EXTENSIONS
        ).lower()
    )


def get_dataset_ids(ckan: ckanapi.LocalCKAN, days: int) -> Iterable[str]:
    fq = " AND ".join(
        [
            "+res_format:ZIP",
            "-harvest_portal:*",
            "-harvest_source_id:*",
            f"metadata_modified:[NOW-{days}DAY TO NOW]",
        ]
    )
    total = ckan.action.package_search(fq=fq, rows=0)["count"]
    chunk_size = 20
    for start in range(0, total, chunk_size):
        packages = ckan.action.package_search(
            fq=fq, rows=chunk_size, start=start
        )["results"]
        yield from (p["id"] for p in packages)


def select_extractable_resources(
    ckan: ckanapi.LocalCKAN, dataset_ids: Iterable[str]
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:

    current_user = ckan.action.user_show(id=ckan.username)
    for id_ in dataset_ids:
        log.info("-" * 80)
        log.info(f"Processing dataset {id_}")
        try:
            dataset = ckan.action.package_show(id=id_)
        except ckanapi.NotFound:
            log.error(f"Dataset {id_} not found.")
            continue

        activity_list = ckan.action.package_activity_list(id=dataset["id"])
        # checking that bot was not last editor ensures no infinite loop
        # todo scan for last date of non-bot edit
        # Or last modified date could be compared with zip_extracted resources scan dates set.
        if activity_list and activity_list[0]["user_id"] == current_user["id"]:
            log.info("No changes since last extraction.")
            continue
        yield from (
            (r, dataset)
            for r in dataset["resources"]
            if "zip" in r["format"].lower() and r.get("zip_extract", "")
        )


def extract_resource(
    resource: dict[str, Any], path: str
) -> Iterable[tuple[str, str]]:
    os.chdir(path)

    log.info(
        "Downloading resource %s from URL %s into %s",
        resource["id"],
        resource["url"],
        path,
    )

    try:
        resp = requests.get(resource["url"], stream=True)
    except requests.RequestException:
        log.exception("Cannot connect to URL")
        return

    if not resp.ok:
        log.error(
            "Cannot retrive resource: %s %s",
            resp.status_code,
            resp.reason,
        )
        return
    with open("input.zip", "wb") as dest:
        for chunk in resp.iter_content(1024 * 1024):
            dest.write(chunk)

    archive = zipfile.ZipFile("input.zip")
    archive.extractall()
    yield from recurse_directory(path)


def update_resource(
    filename: str,
    filepath: str,
    ckan: ckanapi.LocalCKAN,
    resource: dict[str, Any],
    dataset: dict[str, Any],
) -> str:
    with open(filepath, "rb") as stream:
        upload = FileStorage(stream, filename, filename)
        for res in dataset["resources"]:
            if res["name"] == filename:
                res["last_modified"] = datetime.utcnow().isoformat()
                log.info("Updating resource %s", res["id"])
                res["upload"] = upload
                ckan.call_action(
                    "resource_update",
                    res,
                )
                break
        else:

            log.info("Creating new resource for file")
            res = ckan.call_action(
                "resource_create",
                {
                    "package_id": dataset["id"],
                    "name": filename,
                    "url": filename,
                    "parent_res": resource["id"],
                    "zip_extracted": True,
                    "last_modified": datetime.utcnow().isoformat(),
                    "upload": upload,
                },
            )

    return res["id"]


def has_interesting_files(path: str) -> bool:
    for g in os.listdir(path):
        if g.split(".").pop().lower() in _interesting_extensions():
            return True
    return False


def recurse_directory(path: str) -> Iterable[tuple[str, str]]:
    if (
        len([fn for fn in os.listdir(path)]) < 3
        and len([ndir for ndir in os.listdir(path) if os.path.isdir(ndir)])
        == 1
    ):
        for f in os.listdir(path):
            if os.path.isdir(os.path.join(path, f)):
                yield from recurse_directory(os.path.join(path, f))

    os.chdir(path)
    numInteresting = len(
        [
            f
            for f in os.listdir(path)
            if (
                os.path.isfile(os.path.join(path, f))
                and (f.split(".").pop().lower() in _interesting_extensions())
            )
        ]
    )
    for f in os.listdir(path):
        if os.path.isfile(os.path.join(path, f)) and numInteresting:
            if f.split(".").pop().lower() in _interesting_extensions():
                yield (f, os.path.join(path, f))
        if os.path.isdir(os.path.join(path, f)):
            # only zip up folders if they contain at least one interesting file
            if has_interesting_files(os.path.join(path, f)):
                zipf = zipfile.ZipFile(f + ".zip", "w", zipfile.ZIP_DEFLATED)
                zipdir(f, zipf)
                zipf.close()
                yield (f + ".zip", f + ".zip")


def zipdir(path: str, ziph: zipfile.ZipFile):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
