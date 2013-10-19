paster --plugin=ckan db clean --config=development.ini
echo "drop extension postgis cascade;" | psql -d ckantest
paster --plugin=ckan db clean --config=development.ini

#to initiate for first time instead of load from dump
#paster --plugin=ckan db init --config=development.in
#paster --plugin=ckan user add maxious password=snmc email=maxious@gmail.com
#paster --plugin=ckan sysadmin add maxious
#paster --plugin=ckan db dump dump.db

#paster --plugin=ckan db load --config=development.ini dump.db
paster --plugin=ckan db load --config=development.ini dump.harvest.db
echo "create extension postgis;" | psql -d ckantest
#sleep 2
paster --plugin=ckan search-index rebuild --config=development.ini
#rm -r /tmp/pairtree_*
