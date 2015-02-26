import ckanapi # download from https://github.com/ckan/ckanapi and include in your scraper repo

destination = ckanapi.RemoteCKAN()
results = destination.action.package_search(rows=1000,fq="-extras_contact_point:* and -harvest_source_title:*")
print results['count']

user_cache = {}
def get_user(id):
    if id not in user_cache:
        user_cache[id] = destination.action.user_show(id=dataset['creator_user_id'], include_datasets=False).get('email')
    return user_cache[id]
for result in results['results']:

    print ""
    print "loading",result['name']
    if 'taxation' not in result['name']:
        dataset = destination.action.package_show(id=result['name'])
        if 'data_state' not in dataset:
            dataset[u'data_state'] = 'active'
        if 'temporal_coverage_from' not in dataset:
            dataset[u'temporal_coverage_from'] = dataset['metadata_created']
        if 'spatial_coverage' not in dataset:
            dataset[u'spatial_coverage'] = 'GA1'
        if 'jurisdiction' not in dataset:
            dataset[u'jurisdiction'] = 'Commonwealth of Australia'
        if 'update_freq' not in dataset:
            dataset[u'update_freq'] = 'As Needed'
        if 'contact_point' not in dataset and 'creator_user_id' in dataset:
            try:
                user = get_user(dataset['creator_user_id'])
            except Exception:
                user = "data.gov@finance.gov.au"
            dataset[u'contact_point'] = user
            dataset[u'extras'].append({u'key': u'contact_point', u'value': user})
        try:
            result = destination.call_action('package_update', dataset)
            print "dataset " + result['name'] + ' updated'
        except Exception,e:
            print ""
            print "dataset " + result['name'] + ' failed'
            print str(e)
            print ""
