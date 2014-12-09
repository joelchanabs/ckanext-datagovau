#!/usr/bin/python
# coding=utf-8

import ckanapi # get from https://github.com/open-data/ckanapi

import os
import tempfile
import urllib

class LoaderError(Exception):
    pass

# Instantiate the CKAN client.
server = 'data.gov.au'


ckandirect = ckanapi.RemoteCKAN('http://' + server, apikey=api_key)

if __name__ == "__main__":
    pkg_name = 'budget-2014-15-tables-and-data'
    pkg = ckandirect.action.package_show(id=pkg_name)
    #print pkg
    for resource in pkg['resources']:
        print "Updating resource: " + resource['name']
        resource_category = resource['name'].split('-')[0].strip()
        if 's3-ap-southeast-2.amazonaws.com/datagovau' in resource['url']:

            #print content
            fname = resource['url'].split('/')[-1].encode("ascii", "ignore").replace("/", "")
            urllib.urlretrieve(resource['url'], fname)

            ckandirect.action.resource_update(id =resource['id'], upload=open(fname))
            print "Resource successfully updated with new data URL"
        else:
            print "Error uploading file: " + resource['url']
