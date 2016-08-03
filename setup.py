from setuptools import setup, find_packages

version = '0.1'

setup(
	name='ckanext-datagovau',
	version=version,
	description='Extension for customising CKAN for data.gov.au',
	long_description='',
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Greg von Nessi',
	author_email='greg.vonnessi@linkdigital.com.au',
	url='',
	license='',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.datagovau'],
	include_package_data=True,
	zip_safe=False,
	install_requires=['requests', 'feedparser', 'pylons', 'ckan', 'python-dateutil'],
	entry_points=\
	"""
        [ckan.plugins]
		datagovau = ckanext.datagovau.plugin:DataGovAuPlugin
		datagovau_hierarchy = ckanext.datagovau.plugin:HierarchyForm
		zipextractor = ckanext.datagovau.plugin:ZipExtractorPlugin
		spatialingestor = ckanext.datagovau.plugin:SpatialIngestorPlugin

		[ckan.celery_task]
        tasks = ckanext.datagovau.celery_import:task_imports

        [paste.paster_command]
        purgezip = ckanext.datagovau.commands:PurgeZip
        purgespatial = ckanext.datagovau.commands:PurgeSpatial
        rebuildzip = ckanext.datagovau.commands:RebuildZip
        rebuildspatial = ckanext.datagovau.commands:RebuildSpatial
        cleandatastore = ckanext.datagovau.commands:CleanDatastore
        purgelegacyspatial = ckanext.datagovau.commands:PurgeLegacySpatial
	""",
)
