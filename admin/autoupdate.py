import requests
import ckanapi
# copy (select id,url,format,extras from resource where extras like '%"autoupdate": "active"%') TO STDOUT WITH CSV;
# f759e4b6-723c-4863-8a26-1529d689cad8,http://data.gov.au/geoserver/geelong-roofprints-kml/wms?request=GetCapabilities,wms,"{""autoupdate"": ""active""}"
import fileinput
import csv

def updateresource(resource_id):
    url = 'http://data.disclosurelo.gs'
    api_key = ''
    db_credentials = ''
    print resource_id
    ckan = ckanapi.RemoteCKAN('http://data.disclosurelo.gs')
    #ckan = ckanapi.RemoteCKAN('http://demo.ckan.org')
    resource = ckan.action.resource_show(id=resource_id)
    print resource
    url = resource['url'] 
    #last_modified= 'Mon, 24 Feb 2014 01:48:29 GMT'
    #etag='"1393206509.38-638"'
    headers={}
    if 'etag' in resource:
        headers['If-None-Match'] = resource['etag']
    if 'file_last_modified' in resource:
        headers["If-Modified-Since"] = resource['file_last_modified']
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
#    result = ckan.action.resource_update(id,resource)
        if 'format' == 'shp':
            print "geoingest!" db_settings_json, api_url, api_key, resource['dataset_id']
        else:
            ckan.action.datapusher_submit(resource_id=id)
        
if len(sys.argv) != 5:
    print "spatial ingester. command line: postgis_url api_url api_key"
    sys.exit(errno.EACCES)
else:
    (path, db_settings_json, api_url, api_key) = sys.argv
    db_settings = json.loads(db_settings_json)

for line in fileinput.input():
    row = csv.reader(line)
    updateresource(line.replace("\n",""))
