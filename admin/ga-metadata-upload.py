# coding=utf-8
'''
geoscience XML metadata upload for data.gov.au
<alex.sadleir@linkdigital.com.au>
1.0  20/01/2014  initial implementation
'''
import ckanclient
from ckanclient import CkanApiError
import re

import ckanapi # get from https://github.com/open-data/ckanapi

from datetime import datetime
import os, hashlib
import urllib2
from urllib2 import (urlopen,
                     Request,
                     HTTPError, URLError)
from urllib import urlencode
from os.path import basename
import urlparse
from urlparse import urlsplit
import tempfile


class LoaderError(Exception):
    pass

#http://diveintopython.org/http_web_services/etags.html
class NotModifiedHandler(urllib2.BaseHandler):
    def http_error_304(self, req, fp, code, message, headers):
        addinfourl = urllib2.addinfourl(fp, headers, req.get_full_url())
        addinfourl.code = code
        return addinfourl

def _post_multipart(ckan, url, fields, files):
        '''Post fields and files to an http host as multipart/form-data.

        Taken from
        http://code.activestate.com/recipes/146306-http-client-to-post-using-multipartform-data/

        :param fields: a sequence of (name, value) tuples for regular form
            fields
        :param files: a sequence of (name, filename, value) tuples for data to
            be uploaded as files

        :returns: the server's response page

        '''
        content_type, body = ckan._encode_multipart_formdata(fields, files)
        headers = {'Content-Type': content_type}

        # If we got a relative url from api, and we need to build a absolute
        url = urlparse.urljoin(ckan.base_location, url)

        # If we are posting to ckan, we need to add ckan auth headers.
        if url.startswith(urlparse.urljoin(ckan.base_location, '/')):
            print ckan.api_key
            headers.update({
                'Authorization': ckan.api_key,
                'X-CKAN-API-Key': ckan.api_key,
                })

        request = Request(url, data=body, headers=headers)
        response = urlopen(request)
        return response.getcode(), response.read()

def upload_file (ckan, file_path):
    '''Upload a file to a CKAN instance via CKAN's FileStore API.

    The CKAN instance must have file storage enabled.

    A timestamped directory is created on the server to store the file as
    if it had been uploaded via the graphical interface. On success, the
    URL of the file is returned along with an empty error message. On
    failure, the URL is an empty string.

    :param file_path: path to the file to upload, on the local filesystem
    :type file_path: string

    :returns: a (url, errmsg) 2-tuple containing the URL of the
        successufully uploaded file on the CKAN server (string, an empty
        string if the upload failed) and any error message from the server
        (string, an empty string if there was no error)
    :rtype: (string, string) 2-tuple

    '''
    # see ckan/public/application.js:makeUploadKey for why the file_key
    # is derived this way.
    ts = datetime.isoformat(datetime.now()).replace(':','').split('.')[0]
    norm_name  = os.path.basename(file_path).replace(' ', '-')
    file_key = os.path.join(ts, norm_name)

    auth_dict = ckan.storage_auth_get('/form/'+file_key, {})

    fields = [(kv['name'].encode('ascii'), kv['value'].encode('ascii'))
              for kv in auth_dict['fields']]
    files  = [('file', os.path.basename(file_key), open(file_path, 'rb').read())]

    # , httpmsg, msg2
    errcode, msg= _post_multipart(ckan,auth_dict['action'].encode('ascii'), fields, files)

    if errcode == 200:
        file_metadata = ckan.storage_metadata_get(file_key)
        return file_metadata['_location'], ''
    else:
        return '', str(errcode) + " " + msg #+ " " + str(httpmsg.items())

def fetchURL(url):
    #url = canonurl(url)
    req = urllib2.Request(url)
    print "Fetching %s" % (url)
    if url.startswith("mailto") or url.startswith("javascript") or url.startswith("#") or url == None or url == "":
        print >> sys.stderr, "Not a valid HTTP url"
        return (None, None, None)
    req.add_header("User-Agent", "Mozilla/4.0 (compatible; data.gov.au webspider)")
    opener = urllib2.build_opener(NotModifiedHandler())
    try:
        url_handle = opener.open(req, None, 20)
        print "Fetch completed"
        content = url_handle.read()
        localName = basename(urlsplit(url)[2])
        if url_handle.info().has_key('Content-Disposition'):
            # If the response has Content-Disposition, we take file name from it
            localName = url_handle.info()['Content-Disposition'].split('filename=')[1]
            if localName[0] == '"' or localName[0] == "'":
                localName = localName[1:-1]
        return (localName, None, content)
        #store as attachment epoch-filename
    except (urllib2.URLError) as e:

        error = ""
        if hasattr(e, 'reason'):
            error = "error %s in downloading %s" % (str(e.reason), url)
        elif hasattr(e, 'code'):
            error = "error %s in downloading %s" % (e.code, url)
        print error
        return (None, None, None)

# Instantiate the CKAN client.
api_key = '97a4946a-7b80-4121-b7d9-52b39f6ae05b'
server = 'data.gov.au'

ckan = ckanclient.CkanClient(base_location='http://' + server + '/api',
                             api_key=api_key)
ckandirect = ckanapi.RemoteCKAN('http://' + server, apikey=api_key)

category_urls = {
    "air conditioners": "http://reg.energyrating.gov.au/comparator/product_types/64/search/?&export_format=csv",
    "clothes dryers": "http://reg.energyrating.gov.au/comparator/product_types/35/search/?&export_format=csv",
}

if __name__ == "__main__":
        pkg_name = 'geoscience-australia-resources'
        pkg = ckandirect.action.package_show(id=pkg_name)
        #print pkg
        for resource in pkg['resources']:
            print "Updating resource: " + resource['name']
            resource_category = resource['name'].split('-')[0].strip()
            if resource_category.lower() not in category_urls:
                print ("Appliance category not found: " + resource_category)
            else:
                print "CSV temporarily downloaded to " +category_urls[resource_category]
                url, msg = upload_file(ckan,category_urls[resource_category])
                if msg == '' :
                    resource['url'] = urlparse.urljoin(ckan.base_location, url)
                    print "CSV uploaded to "+resource['url']
                    #print resource
                    ckandirect.action.resource_update(id=resource['id'],url=resource['url'],name=resource['name'],
                                                      last_modified=datetime.now().isoformat())
                    print "Resource successfully updated with new data URL"
                else:
                    raise Exception("Error uploading file: " + msg)


