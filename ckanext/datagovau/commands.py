from ckan.lib.cli import CkanCommand


# No other CKAN imports allowed until _load_config is run,
# or logging is disabled


def _exec_processing(args, process, data_type):
    import pylons.config as config
    import ckan.model as model
    import ckan.lib.cli as cli
    import ckan.plugins.toolkit as tk
    from lib import process_all

    if args:
        try:
            pkg = model.Package.get(args[0])
        except:
            pkg = None

        if pkg is None:
            print ("You have not specified a valid package name or id, aborting.")
            return

        pkg_id = pkg.id
    else:
        pkg_id = None

    core_url = config.get('ckan.site_url', 'http://localhost:8000/')
    cxt = {'user': model.User.get(config.get('ckan.dataingestor.ckan_user', 'default')).name,
           'postgis': cli.parse_db_config('ckan.dataingestor.postgis_url'),
           'geoserver': cli.parse_db_config('ckan.dataingestor.geoserver_url'),
           'geoserver_public_url': config.get('ckan.dataingestor.public_geoserver', core_url + '/geoserver'),
           'org_blacklist': list(set(tk.aslist(config.get('ckan.dataingestor.spatial.org_blacklist', [])))),
           'pkg_blacklist': list(set(tk.aslist(config.get('ckan.dataingestor.spatial.pkg_blacklist', [])))),
           'user_blacklist': list(set(map(lambda x: model.User.get(x).id,
                                          tk.aslist(config.get('ckan.dataingestor.spatial.user_blacklist', []))))),
           'target_spatial_formats': list(
               set(map(lambda x: x.upper(), tk.aslist(config.get('ckan.dataingestor.spatial.target_formats', []))))),
           'target_zip_formats': list(
               set(map(lambda x: x.upper(), tk.aslist(config.get('ckan.dataingestor.zip.target_formats', []))))),
            'temporary_directory': config.get('ckan.dataingestor.temporary_directory', '/tmp/ckan_ingest')}

    process_all(cxt, pkg_id, process, data_type)


class PurgeZip(CkanCommand):
    """ Purges ZIP child resources from a package or all packages.

    Usage (single package): paster purgezip <package_id>
    Usage (all packages): paster purgezip

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        _exec_processing(self.args, 'purge', 'zip')


class PurgeSpatial(CkanCommand):
    """ Purges Spatial child resources from a package or all packages.

    Usage (single package): paster purgespatial <package_id>
    Usage (all packages): paster purgespatial

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        _exec_processing(self.args, 'purge', 'spatial')


class RebuildZip(CkanCommand):
    """ Rebuild ZIP child resources from a package or all packages.

    Usage (single package): paster rebuildzip <package_id>
    Usage (all packages): paster rebuildzip

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        _exec_processing(self.args, 'rebuild', 'zip')


class RebuildSpatial(CkanCommand):
    """ Rebuilds Spatial child resources from a package or all packages.

    Usage (single package): paster rebuildspatial <package_id>
    Usage (all packages): paster rebuildspatial

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()

        _exec_processing(self.args, 'rebuild', 'spatial')
