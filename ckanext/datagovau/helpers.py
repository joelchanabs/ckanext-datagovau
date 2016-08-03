import logging
import os
import time

import ckan.lib as lib
import ckan.lib.cli as cli
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import ckanext.datastore.db as datastore_db
import feedparser
from ckan.lib import uploader, formatters
from ckanext.datapusher.plugin import DEFAULT_FORMATS as DATAPUSHER_DEFAULT_FORMATS
from pylons import config

log = logging.getLogger('ckanext_datagovau')

MSG_SPATIAL_PREFIX = 'Spatial Ingestor:'
MSG_SPATIAL_SKIP_SUFFIX = 'skipping spatial ingestion.'
MSG_ZIP_PREFIX = 'Zip Extractor:'
MSG_ZIP_SKIP_SUFFIX = 'skipping Zip extraction.'


def get_user_datasets(user_dict):
    # Need to test packages carefully to make sure they haven't been purged from the DB (like what happens
    # in a harvest purge), as the activity list does not have the associated entries cleaned out.
    # [SXTPDFINXZCB-145]
    def pkg_test(input):
        try:
            result = input['data'].get('package')

            # Test just to catch an exception if need be
            data = logic.get_action('package_show')(context, {'id': input['data']['package']['id']})
        except:
            result = False
        return result

    context = {'model': model, 'user': user_dict['name']}
    created_datasets_list = user_dict['datasets']

    active_datasets_list = [logic.get_action('package_show')(context, {'id': x['data']['package']['id']}) for x in
                            lib.helpers.get_action('user_activity_list', {'id': user_dict['id']}) if pkg_test(x)]
    raw_list = sorted(active_datasets_list + created_datasets_list, key=lambda pkg: pkg['state'])
    filtered_dict = {}
    for dataset in raw_list:
        if dataset['id'] not in filtered_dict.keys():
            filtered_dict[dataset['id']] = dataset
    return filtered_dict.values()


def get_user_datasets_public(user_dict):
    return [pkg for pkg in get_user_datasets(user_dict) if pkg['state'] == 'active']


def get_ddg_site_statistics():
    def fetch_ddg_stats():
        stats = {'dataset_count': logic.get_action('package_search')({}, {"rows": 1})['count'],
                 'group_count': len(logic.get_action('group_list')({}, {})),
                 'organization_count': len(logic.get_action('organization_list')({}, {})), 'unpub_data_count': 0}

        for fDict in \
                logic.get_action('package_search')({}, {"facet.field": ["unpublished"], "rows": 1})['search_facets'][
                    'unpublished'][
                    'items']:
            if fDict['name'] == "Unpublished datasets":
                stats['unpub_data_count'] = fDict['count']
                break

        result = model.Session.execute(
            '''select count(*) from related r
               left join related_dataset rd on r.id = rd.related_id
               where rd.status = 'active' or rd.id is null''').first()[0]
        stats['related_count'] = result

        stats['open_count'] = logic.get_action('package_search')({}, {"fq": "isopen:true", "rows": 1})['count']

        stats['api_count'] = logic.get_action('resource_search')({}, {"query": ["format:wms"]})['count'] + len(
            datastore_db.get_all_resources_ids_in_datastore())

        return stats

    if toolkit.asbool(config.get('ckanext.stats.cache_enabled', 'True')):
        from pylons import cache

        key = 'ddg_site_stats'
        res_stats = cache.get_cache('ddg_ext', type='memory').get_value(key=key,
                                                                        createfunc=fetch_ddg_stats,
                                                                        expiretime=toolkit.asint(
                                                                            config.get(
                                                                                'ckanext.stats.cache_fast_timeout',
                                                                                '600')))
    else:
        res_stats = fetch_ddg_stats()

    return res_stats


def get_resource_file_size(rsc):
    if rsc.get('url_type') == 'upload':
        upload = uploader.ResourceUpload(rsc)
        value = None
        try:
            value = os.path.getsize(upload.get_path(rsc['id']))
            value = formatters.localised_filesize(int(value))
        except Exception:
            # Sometimes values that can't be converted to ints can sneak
            # into the db. In this case, just leave them as they are.
            pass
        return value
    return None


def blogfeed():
    d = feedparser.parse('https://blog.data.gov.au/blogs/rss.xml')
    for entry in d.entries:
        entry.date = time.strftime("%a, %d %b %Y", entry.published_parsed)
    return d


def zipextractor_status(resource_id):
    try:
        return toolkit.get_action('zipextractor_status')(
            {}, {'resource_id': resource_id})
    except toolkit.ObjectNotFound:
        return {
            'status': 'unknown'
        }


def get_zip_context(ctx=config):
    symbols = {
        'customary': ('b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'),
        'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                          'zetta', 'iotta'),
        'iec': ('bi', 'ki', 'mi', 'gi', 'ti', 'pi', 'ei', 'zi', 'yi'),
        'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                    'zebi', 'yobi'),
        'standard': ('b', 'kb', 'mb', 'gb', 'tb', 'pb', 'eb', 'zb', 'yi'),
    }

    def human2bytes(s):
        num = ""
        while s and s[0:1].isdigit() or s[0:1] == '.':
            num += s[0]
            s = s[1:]
        num = float(num)
        suffix = s.strip().lower()

        for name, sset in symbols.items():
            if suffix in sset:
                return int(num * (1 << sset.index(suffix) * 10))

        return int(num)

    core_url = ctx.get('ckan.site_url', 'http://localhost:8000/')

    if 'datapusher' in ctx.get('ckan.plugins'):
        datapusher_formats = list(set(map(lambda x: x.upper(), toolkit.aslist(
            ctx.get('ckan.datapusher.formats', ' '.join(DATAPUSHER_DEFAULT_FORMATS))))))
    else:
        datapusher_formats = []

    return {'user': model.User.get(ctx.get('ckan.zipextractor.ckan_user', 'default')).name,
            'ckan_api_url': ctx.get('ckan.zipextractor.ckan_api_url', core_url),
            'resource_create_url': ctx.get('ckan.zipextractor.resource_create_url',
                                           core_url + "/api/3/action/resource_create"),
            'auto_process': toolkit.asbool(ctx.get('ckan.zipextractor.auto_extract', 'False')),
            'org_blacklist': list(
                set(toolkit.aslist(ctx.get('ckan.zipextractor.org_blacklist', '')))),
            'pkg_blacklist': list(
                set(toolkit.aslist(ctx.get('ckan.zipextractor.pkg_blacklist', '')))),
            'user_blacklist': list(set(map(lambda x: model.User.get(x).id,
                                           toolkit.aslist(
                                               ctx.get('ckan.zipextractor.user_blacklist',
                                                       ''))))),
            'max_zip_resource_filesize': human2bytes(ctx.get('ckan.zipextractor.max_zip_resource_filesize', '100MB')),
            'target_zip_formats': list(set(map(lambda x: x.upper(), toolkit.aslist(
                ctx.get('ckan.zipextractor.target_formats', ''))))),
            'wait_for_datapusher': toolkit.asbool(ctx.get('ckan.zipextractor.wait_for_datapusher', 'False')),
            'datapusher_submit_timeout': toolkit.asint(ctx.get('ckan.zipextractor.datapusher_submit_timeout', '30')),
            'datapusher_completion_timeout': toolkit.asint(
                ctx.get('ckan.zipextractor.datapusher_completion_timeout', '1800')),
            'temporary_directory': ctx.get('ckan.zipextractor.temporary_directory',
                                           '/tmp/zipextractor'),
            'datapusher_default_formats': datapusher_formats,
            'config_file_path': os.path.abspath(ctx['__file__'])}


def spatialingestor_status(resource_id):
    try:
        return toolkit.get_action('spatialingestor_status')(
            {}, {'resource_id': resource_id})
    except toolkit.ObjectNotFound:
        return {
            'status': 'unknown'
        }


def get_spatial_context(ctx=config):
    for config_option in ('ckan.spatialingestor.postgis_url', 'ckan.spatialingestor.internal_geoserver_url',):
        if not ctx.get(config_option):
            raise Exception(
                'Config option `{0}` must be set to use the SpatialIngestor.'.format(config_option))

    core_url = ctx.get('ckan.site_url', 'http://localhost:8000/')
    return {'user': model.User.get(ctx.get('ckan.spatialingestor.ckan_user', 'default')).name,
            'ckan_api_url': ctx.get('ckan.spatialingestor.ckan_api_url', core_url),
            'postgis': cli.parse_db_config('ckan.spatialingestor.postgis_url'),
            'geoserver': cli.parse_db_config('ckan.spatialingestor.internal_geoserver_url'),
            'geoserver_public_url': ctx.get('ckan.spatialingestor.public_geoserver_url',
                                            core_url + '/geoserver'),
            'auto_process': toolkit.asbool(ctx.get('ckan.spatialingestor.auto_extract', 'True')),
            'org_blacklist': list(
                set(toolkit.aslist(ctx.get('ckan.spatialingestor.org_blacklist', [])))),
            'pkg_blacklist': list(
                set(toolkit.aslist(ctx.get('ckan.spatialingestor.pkg_blacklist', [])))),
            'user_blacklist': list(set(map(lambda x: model.User.get(x).id,
                                           toolkit.aslist(
                                               ctx.get('ckan.spatialingestor.user_blacklist',
                                                       []))))),
            'target_spatial_formats': list(set(map(lambda x: x.upper(),
                                                   toolkit.aslist(
                                                       ctx.get(
                                                           'ckan.spatialingestor.target_formats',
                                                           []))))),
            'temporary_directory': ctx.get('ckan.spatialingestor.temporary_directory',
                                           '/tmp/spatialingestor'),
            'config_file_path': os.path.abspath(ctx['__file__'])}


def get_zip_input_format(resource):
    check_string = resource.get('__extras', {}).get('format', resource.get('format', resource.get('url', ''))).upper()
    if check_string.endswith("ZIP"):
        return 'ZIP'
    else:
        return None


def get_spatial_input_format(resource):
    check_string = resource.get('__extras', {}).get('format', resource.get('format', resource.get('url', ''))).upper()

    if any([check_string.endswith(x) for x in ["SHP", "SHAPEFILE"]]):
        return 'SHP'
    elif check_string.endswith("KML"):
        return 'KML'
    elif check_string.endswith("KMZ"):
        return 'KMZ'
    elif check_string.endswith("GRID"):
        return 'GRID'
    else:
        return None


def check_blacklists(context, package):
    message = ''
    if package['organization']['name'] in context['org_blacklist']:
        message = "{0} in organization blacklist".format(package['organization']['name'])
    elif package['name'] in context['pkg_blacklist']:
        message = "{0} in package blacklist".format(package['name'])
    else:
        activity_list = toolkit.get_action('package_activity_list')(context, {
            'id': package['id'],
        })
        if activity_list[0]['user_id'] in context['user_blacklist']:
            message = "{0} was last edited by blacklisted user".format(activity_list[0]['user_id'])

    return message


def is_zip_resource(resource):
    # Only ingest if the right format
    return get_zip_input_format(resource) is not None


def is_spatial_resource(resource):
    # Do not expand spatial children
    if resource.get('spatial_child_of', None) is not None:
        return False

    # Only ingest if the right format
    return get_spatial_input_format(resource) is not None


def status_description(data_dict):
    '''
    :param status:
    :return:
    '''
    _ = toolkit._

    if data_dict and data_dict.get('status'):
        captions = {
            'complete': _('Complete'),
            'pending': _('Pending'),
            'submitting': _('Submitting'),
            'error': _('Error'),
        }

        return captions.get(data_dict['status'], data_dict['status'].capitalize())
    else:
        return _('Not Uploaded Yet')
