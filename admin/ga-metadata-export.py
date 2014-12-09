# coding=utf-8
'''
geoscience XML metadata export for data.gov.au
<alex.sadleir@linkdigital.com.au>
1.0  20/01/2014  initial implementation
'''
import xmltodict
import psycopg2
from bs4 import BeautifulSoup as Soup
import csv

try:
    conn=psycopg2.connect("dbname='ckantest' user='maxious' password=''")
except:
    print "I am unable to connect to the database."

cur = conn.cursor()
try:
    cur.execute("SELECT guid,content from harvest_object where content is not null")
except:
    print "I can't SELECT from bar"
c = csv.writer(open("gaexport.csv", "wb"))
c.writerow(['id','title','description','metadataURL','dataURL','dataFormat','licence'])
rows = cur.fetchall()
for row in rows:
    print row[0].replace('{','').replace('}','')+".xml"
    text_file = open('gaexport/'+row[0].replace('{','').replace('}','')+".xml", "w")
    text_file.write( row[1])
    text_file.close
    xml= xmltodict.parse( row[1])
    soup= Soup( row[1], "xml")
    namespace = ""
    if 'gmd:MD_Metadata' in xml:
        metadata = xml['gmd:MD_Metadata']
        namespace = "gmd:"
    if 'MD_Metadata' in xml:
	    metadata = xml['MD_Metadata']
    #print xml
    result = {"id": row[0], 
              "title": metadata[namespace+'identificationInfo'][namespace+'MD_DataIdentification'][namespace+'citation'][namespace+'CI_Citation'][namespace+'title']['gco:CharacterString'].encode('ascii', 'ignore').replace('\n',' '),
              "description": metadata[namespace+'identificationInfo'][namespace+'MD_DataIdentification'][namespace+'abstract']['gco:CharacterString'].encode('ascii', 'ignore').replace('\n',' '),
            "metadataURL": '',
	      "licence": ''
             }
    if metadata.get(namespace+'dataSetURI'):
        result["metadataURL"] = metadata[namespace+'dataSetURI']['gco:CharacterString']
    for x in soup.findAll('otherConstraints'):
	    result['licence'] = result['licence'] + ''.join(x.stripped_strings).encode('ascii', 'ignore').replace('\n',' ') + ';'
    has_urls = False
    #print result
    for res in soup.findAll('CI_OnlineResource'):
        print ''.join(res.stripped_strings)
        url = ''.join(res.find('linkage').stripped_strings).encode('ascii', 'ignore').replace('\n',' ')
        if url and res.find('protocol'):
            format = ''.join(res.find('protocol').stripped_strings).encode('ascii', 'ignore').replace('\n',' ')
            has_urls = True
            c.writerow([result['id'],result['title'],result['description'],result['metadataURL'], url, format, result['licence']])
    if not has_urls:
       c.writerow([result['id'],result['title'],result['description'],result['metadataURL'],None, result['licence']])
