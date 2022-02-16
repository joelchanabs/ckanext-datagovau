This CKAN Extension customises a CKAN instance for the hosting of data.gov.au.

It comprises:

* A custom Package edit form that defaults to cc-by licence
* Replaces links with http/https protocol independent versions
* Provides HTML to users to embed data previews on their own website
* A cut down licenses.json file

This extension is complemented by ckanext-agls for AGLS metadata, ckanext-googleanalytics for Google Analytics tracking of API usage and ckanext-dga-stats for the customised site statistics page.


Development
===========

1. Instal dev-requirements::

     pip install -r dev-requirements.txt

2. Initialize git-hooks::

     pre-commit install

Testing
+++++++

Run all the tests::

  pytest ckanext/datagovau/tests
