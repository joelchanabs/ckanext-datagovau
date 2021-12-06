from __future__ import annotations

import click
from ckan import model
import ckan.plugins.toolkit as tk


@click.group("spatial-ingestor", short_help="Ingest spatial data")
def spatial_ingestor():
    pass


@spatial_ingestor.command("ingest")
@click.argument("scope")
@click.option("-f", "--force", help="Enforce ingestions.", is_flag=True)
@click.option(
    "-o",
    "--organization",
    multiple=True,
    default=[
        "3965c5cd-d88f-4735-92db-af28d3ad9155",
        "a56f8067-b250-4c32-9609-f2191dc88a3a",
    ],
)
def perform_ingest(scope: str, force: bool, organization: tuple[str]):
    """
    Performs ingest of spatial data for scope of data.

    Usage::
        ckan spatial-ingestor <scope> [--force]

        where scope is one of: 'all', 'updated', 'updated-orgs', or <dataset-id>
        and force option unconditionally enforces ingestion.
    """
    from ._spatialingestor import do_ingesting

    query = model.Session.query(model.Package).filter_by(state="active")
    if scope == "updated-orgs":
        query = query.filter(model.Package.owner_org.in_(organization))
    elif scope not in ("all", "updated", "updated-orgs"):
        query = query.filter(
            (model.Package.name == scope) | (model.Package.id == scope)
        )

    with click.progressbar(query) as bar:
        for pkg in bar:
            do_ingesting(pkg.id, force)


@spatial_ingestor.command("purge")
@click.option("-s", "--skip-grids", is_flag=True, default=True)
@click.argument("scope")
def perform_purge(scope: str, skip_grids: bool):
    """
    Performs purge of nominated scope.

    Usage:
        ckan spatial-ingestor purge <scope>

        where scope is one of: 'all', 'erroneous', or <dataset-id>.
    """
    from ._spatialingestor import clean_assets, may_skip

    query = model.Session.query(model.Package)
    if scope not in ["all", "erroneous"]:
        query = query.filter(
            (model.Package.name == scope) | (model.Package.id == scope)
        )

    with click.progressbar(query) as bar:
        for pkg in bar:
            if scope == "erroneous" and not may_skip(pkg.id):
                clean_assets(pkg.id, skip_grids=False)
            else:
                clean_assets(pkg.id, skip_grids=skip_grids)


# datagovau spatial-ingestor dropuser subcommand.
@spatial_ingestor.command("dropuser")
@click.argument("username")
def perform_drop_user(username: str):
    """
    Deletes nominated user.

    Usage:
        ckan spatial-ingestor dropuser <username>
    """
    user: model.User = model.User.get(username)
    if user is None:
        tk.error_shout(f"User <{username}> not found")
        raise click.Abort()

    groups = user.get_groups()
    if groups:
        tk.error_shout(
            "User is a member of groups/organizations: %s"
            % ", ".join(g.display_name for g in groups)
        )
        raise click.Abort()

    pkgs = model.Session.query(model.Package).filter_by(
        creator_user_id=user.id
    )
    if pkgs.count():
        tk.error_shout(
            "There are some(%d) datasets created by this user: %s"
            % (pkgs.count(), [pkg.name for pkg in pkgs])
        )
        raise click.Abort()

    activities = (
        model.Session.query(model.Activity)
        .filter_by(user_id=user.id)
        .filter(model.Activity.activity_type.contains("package"))
    )
    if activities.count():
        tk.error_shout(
            "There are some(%d) activity records that mentions user"
            % activities.count()
        )
        raise click.Abort()

    model.Session.delete(user)
    model.Session.commit()
    click.secho("Done", fg="green")
