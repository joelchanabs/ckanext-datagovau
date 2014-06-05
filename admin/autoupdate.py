#!/usr/bin/python
# coding=utf-8
'''
auto update batch job for ckan
<alex.sadleir@linkdigital.com.au>
1.0  26/05/2014  initial implementation

TODO
- archive files in filestore
- update frequency reduced based on dataset metadata
- emails on HTTP errors
'''

import requests
import ckanapi
import csv
import sys
import psycopg2
import json
from subprocess import Popen, PIPE

def updateresource(resource_id, dataset_id):
    print ' --- '
    ckan = ckanapi.RemoteCKAN(api_url,api_key)
    #ckan = ckanapi.RemoteCKAN('http://demo.ckan.org')
    resource = ckan.action.resource_show(id=resource_id)
    print 'updating '+resource['name']+'('+resource_id+', '+dataset_id+')'
    print resource
    url = resource['url'] 
    #last_modified= 'Mon, 24 Feb 2014 01:48:29 GMT'
    #etag='"1393206509.38-638"'
    headers={}
    if 'etag' in resource:
        headers['If-None-Match'] = resource['etag']
    if 'file_last_modified' in resource:
        headers["If-Modified-Since"] = resource['file_last_modified']
    print headers
    r = requests.head(url, headers=headers)
    if r.status_code == 304:
        print 'not modified'
        return
    else:
        print r.status_code
        print r.headers
        if 'last-modified' in r.headers:
            resource['file_last_modified'] = r.headers['last-modified']
        if 'etag' in r.headers:
            resource['etag'] = r.headers['etag']
        #save updated resource
        print resource
        result = ckan.call_action('resource_update',resource)
        if resource['format'].lower() == 'shp' or resource['format'].lower() == 'kml':
            print "geoingest!" 
            pargs= ['dga-spatialingestor.py', db_settings_json, api_url, api_key, dataset_id]
            print pargs
            p = Popen(pargs)#, stdout=PIPE, stderr=PIPE)
            p.communicate()
        else:
            print "datapusher!"
            # https://github.com/ckan/ckan/blob/master/ckanext/datapusher/logic/action.py#L19
            ckan.action.datapusher_submit(resource_id=resource_id)
        
if len(sys.argv) != 4:
    print "autoupdate ingester. command line: postgis_url api_url api_key"
    sys.exit(-1)
else:
    (path, db_settings_json, api_url, api_key) = sys.argv
    db_settings = json.loads(db_settings_json)
    datastore_db_settings = dict(db_settings)
    datastore_db_settings['dbname'] = db_settings['datastore_dbname']
    datastore_db_settings_json = json.dumps(datastore_db_settings)

#until https://github.com/ckan/ckan/pull/1732 is merged, use database directly

try:
    conn = psycopg2.connect(dbname=db_settings['dbname'], user=db_settings['user'], password=db_settings['password'], host=db_settings['host'])
except:
    failure("I am unable to connect to the database.")
# Open a cursor to perform database operations
cur = conn.cursor()
conn.set_isolation_level(0)
cur.execute('select resource.id resource_id, package.id dataset_id  from resource inner join resource_group on resource.resource_group_id = resource_group.id inner join package on resource_group.package_id = package.id where resource.extras like \'%"autoupdate": "active"%\';')
row = cur.fetchone() 
while row is not None:
    updateresource(row[0],row[1])
    # process
    row = cur.fetchone()
cur.close()
conn.close()
