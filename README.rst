This CKAN Extension customises a CKAN instance for the hosting of data.gov.au.

It comprises:

* A custom Package edit form that defaults to cc-by licence
* Replaces links with http/https protocol independent versions
* Provides HTML to users to embed data previews on their own website
* A cut down licenses.json file

This extension is complimented by ckanext-agls for AGLS metadata and ckanext-googleanalytics for Google Analytics tracking of API usage

Installation
============

To install this package, from your CKAN virtualenv, run the following from your CKAN base folder (e.g. ``pyenv/``)::

  pip install -e git+https://github.com/okfn/ckanext-datagovau#egg=ckanext-datagovau

Then activate it by setting ``ckan.plugins = datagovau`` in your main ``ini``-file.

To add the cut down licenses.json set ``licenses_group_url = http://%(ckan.site_url)/licenses.json``
or copy ``ckanext/datagovau/theme/public/licenses.json`` to the same folder as your CKAN config ini file
and set ``licenses_group_url = file://%(here)s/licenses.json``


