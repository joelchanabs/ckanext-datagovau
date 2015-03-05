import json
import requests
import sys
import os
import time
import math, os
import boto
import urllib
from filechunkio import FileChunkIO # pip install FileChunkIO
import datetime

now = datetime.datetime.now()

print
print "Current date and time using str method of datetime object:"
print str(now)

# DA BA setup

DATASTORE_LOGIN_URI = 'https://data.bioregionalassessments.gov.au/datastore/login'
DATASTORE_LOGOUT_URI = 'https://data.bioregionalassessments.gov.au/datastore/logout'
DATASTORE_LOGIN_USR = sys.argv[1]
DATASTORE_LOGIN_PWD = sys.argv[2]
DATASTORE_DATASET_REGISTER_URI = 'https://data.bioregionalassessments.gov.au/datastore/dataset/'

s = requests.Session()
r = s.post(DATASTORE_LOGIN_URI, data={'usr': DATASTORE_LOGIN_USR, 'pwd': DATASTORE_LOGIN_PWD})

if r.status_code != 200:
    raise StandardError('error in login')

get_json = True
if get_json:
    r = s.get(DATASTORE_DATASET_REGISTER_URI + '?_format=application/json-ld', allow_redirects=True)
    if r.status_code != 200:
        raise StandardError('error in index list')

    datasets_simple = []
    datasets = json.loads(r.text)
else:
    datasets = json.load(open('./ba.json','rb'))

#S3 setup

# Connect to S3
c = boto.connect_s3()
# Use environment variables:
# AWS_ACCESS_KEY_ID - Your AWS Access Key ID
# AWS_SECRET_ACCESS_KEY - Your AWS Secret Access Key
b = c.get_bucket('datagovau')


# Datasets filtering
datasets_simple = []
for dataset in datasets:
    if dataset['http://data.bioregionalassessments.gov.au/def/ba#write_status'][0]['@value'] == 'registered' \
            and now.strftime('%Y-%m') in dataset['http://purl.org/dc/elements/1.1/created'][0]['@value']:
        datasets_simple.append({
            'id': dataset['@id'].split('/')[-1],
            'data_path': dataset['http://data.bioregionalassessments.gov.au/def/ba#data_path'][0]['@value'],
            'folder_name': dataset['http://data.bioregionalassessments.gov.au/def/ba#folder_name'][0]['@value'],
            'created': dataset['http://purl.org/dc/elements/1.1/created'][0]['@value']
        })

# Datasets processing
for dataset in datasets_simple:
    print ""
    directory = "."+dataset['data_path']+dataset['folder_name']
    filename = directory+ "/"+dataset['id']+".zip"
    source_size = 0
    if not os.path.exists(directory):
        os.makedirs(directory)
    if os.path.exists(filename):
        print filename,"exists on filesystem"
        source_size = os.stat(filename).st_size
    else:
        url = "https://data.bioregionalassessments.gov.au/datastore/dataset/"+dataset['id']+"/OSM/CBR/LW_BA/dms/repository"+dataset['data_path']+urllib.quote_plus(dataset['folder_name'])+"/"+dataset['id']+".zip"
        print url
        r = s.get(url, stream=True)
        #r = s.head("https://data.bioregionalassessments.gov.au/datastore/dataset/"+dataset['id']+"?_view=download")
        if r.status_code != 200:
            print 'error downloading dataset '+dataset['id']
            print url
        else:
            # http://stackoverflow.com/a/16696317/684978
            print dataset['folder_name'] +" "+str(int(r.headers['Content-Length'])/1024/1024) +" MB"
            start = time.clock()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()
            elapsed = (time.clock() - start)
            print "downloaded in ",elapsed

    s3_path = filename.replace('./','/bioregionalassessments/')
    #
    s3key = b.get_key(s3_path)
    do_s3_upload = False
    if s3key:
        print s3_path,"exists on S3"
        if int(s3key.content_length) != int(source_size):
            print 'filesizes differ ',s3key.content_length, source_size
            do_s3_upload = True
    else:
        do_s3_upload = True

    if do_s3_upload:
        # Get file info

        # Create a multipart upload request
        mp = b.initiate_multipart_upload(s3_path)

        # Use a chunk size of 150 MiB (feel free to change this)
        chunk_size = 152428800
        chunk_count = int(math.ceil(source_size / chunk_size))

        # Send the file parts, using FileChunkIO to create a file-like object
        # that points to a certain byte range within the original file. We
        # set bytes to never exceed the original file size.
        for i in range(chunk_count + 1):
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(filename, 'r', offset=offset,
                             bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)

        # Finish the upload
        mp.complete_upload()
        print s3_path,"uploaded to S3"
