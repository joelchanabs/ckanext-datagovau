from __future__ import annotations

import logging
import ckan.model as model
import ckan.plugins.toolkit as tk

from typing import Iterable, Optional
from sqlalchemy.exc import ProgrammingError

import click
import ckanapi

from ..utils import temp_dir

log = logging.getLogger(__name__)


@click.group()
@click.help_option("-h", "--help")
def maintain():
    """Maintenance tasks"""
    pass


@maintain.command()
@click.argument("ids", nargs=-1)
@click.option("-u", "--username", help="CKAN user who performs extraction.")
@click.option(
    "--tmp-dir", default="/tmp", help="Root folder for temporal files"
)
@click.option(
    "--days-to-buffer",
    "days",
    default=3,
    type=int,
    help="Extract datasets modified up to <days> ago",
)
@click.option(
    "--skip-errors",
    is_flag=True,
    help="Do not interrupt extraction even after an error",
)
@click.help_option("-h", "--help")
@click.pass_context
def zip_extract(
    ctx: click.Context,
    ids: Iterable[str],
    tmp_dir: str,
    username: Optional[str],
    days: int,
    skip_errors: bool,
):
    """ZIP extractor for data.gov.au"""
    ckan = ckanapi.LocalCKAN(username)
    from . import _zip_extract as z

    if not ids:
        ids = z.get_dataset_ids(ckan, days)
    with ctx.meta["flask_app"].test_request_context():
        for resource, dataset in z.select_extractable_resources(ckan, ids):
            with temp_dir(resource["id"], tmp_dir) as path:
                for result in z.extract_resource(resource, path):
                    try:
                        z.update_resource(*result, ckan, resource, dataset)
                    except ckanapi.ValidationError:
                        log.error(
                            "Cannot update resource {} from dataset {}".format(
                                resource["id"], dataset["id"]
                            )
                        )
                        if skip_errors:
                            continue
                        raise


@maintain.command()
@click.help_option("-h", "--help")
def force_purge_orgs():
    """Force purge of trashed organizations. If the organization has child packages, they become unowned"""
    sql_commands = [
        "delete from group_extra_revision where group_id in (select id from \"group\" where \"state\"='deleted' AND is_organization='t');",
        "delete from group_extra where group_id in (select id from \"group\" where \"state\"='deleted' AND is_organization='t');",
        "delete from member where group_id in (select id from \"group\" where \"state\"='deleted' AND is_organization='t');",
        "delete from \"group\" where \"state\"='deleted' AND is_organization='t');",
    ]

    _execute_sql_delete_commands(sql_commands)


@maintain.command()
def force_purge_pkgs():
    """Force purge of trashed organizations"""
    sql_commands = [
        "delete from package_extra pe where pe.package_id in (select id from package where name='stevetest');",
        "delete from package where name='stevetest';",
        "delete from related_dataset where dataset_id in (select id from package where \"state\"='deleted');",
        "delete from harvest_object_extra where harvest_object_id in (select id from harvest_object where package_id in (select id from package where \"state\"='deleted'));",
        "delete from harvest_object where package_id in (select id from package where \"state\"='deleted');",
        "delete from harvest_object where package_id in (select id from package where \"state\"='deleted');",
        "delete from package_extra where package_id in (select id from package where \"state\"='deleted');",
        "delete from package where \"state\"='deleted';"
    ]

    _execute_sql_delete_commands(sql_commands)


def _execute_sql_delete_commands(commands):
    for command in commands:
        try:
            model.Session.execute(command)
            model.Session.commit()
        except ProgrammingError as e:
            log.warning(f"Could not execute command \"{command}\". Table does not exist.")
            model.Session.rollback()