from __future__ import annotations

import logging
import ckan.model as model
import ckan.plugins.toolkit as tk

from typing import Iterable, Optional

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
@click.option(
    "--purge-related-pkgs",
    "purge_related_pkgs",
    is_flag=True,
    default=False,
    help="Removes public packages related to organization (if exist). If False just prints names of those packages.",
)
@click.help_option("-h", "--help")
def force_purge_orgs(purge_related_pkgs):
    """Force purge of trashed organizations"""
    deleted_org_ids = (
        d.id for d in model.Session.query(model.Group)
        if d.state == 'deleted'
    )

    for org_id in deleted_org_ids:
        related_pkgs = model.Session.query(model.Package).filter(model.Package.owner_org == org_id).all()
        for related_pkg in related_pkgs:
            if related_pkg.state == "deleted" or related_pkg.private == True or purge_related_pkgs:
                tk.get_action("dataset_purge")({"ignore_auth": True}, {"id": related_pkg.id})
            else:
                print(related_pkg.name)
        if not related_pkg or purge_related_pkgs:
            tk.get_action("organization_purge")({"ignore_auth": True}, {"id": org_id})


# @maintain.command()
# def force_purge_pkgs():
#     """Force purge of trashed organizations"""
#     deleted_pkg_ids = (
#             d.id for d in model.Session.query(model.Package)
#                 if d.state=='deleted'
#     )