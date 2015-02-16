import ckanapi # download from https://github.com/ckan/ckanapi and include in your scraper repo
from owslib.wms import WebMapService
from string import Template

update = True

destination = ckanapi.RemoteCKAN("","")
extent_template = Template('''
    {"type": "Polygon", "coordinates": [[[$xmin, $ymin], [$xmax, $ymin], [$xmax, $ymax], [$xmin, $ymax], [$xmin, $ymin]]]}
    ''')
results = destination.action.package_search(rows=500,fq="res_format:wms")
print results['count']

wms_cache = {}
def get_wms(url):
    url = url.replace('http:// https://','https://')
    if url not in wms_cache:
        wms_cache[url] = WebMapService(url)
    return wms_cache[url]

def layer_harvest(res,result,wms, layer_id):
    if 'wms_layer' not in res:
        res['wms_layer'] = layer_id
        if update:
            destination.call_action('resource_update',res)
        print "resource "+res['name']+" updated"

    if bad_extent:
        bbox_values = list(wms.contents[layer_id].boundingBoxWGS84)
        if abs(sum(bbox_values)) > 1000:
           raise StandardError("Bounding box not WGS84")
        bbox = {}
        bbox['minx'] = float(bbox_values[0])
        bbox['miny'] = float(bbox_values[1])
        bbox['maxx'] = float(bbox_values[2])
        bbox['maxy'] = float(bbox_values[3])
        # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry
        extent_string = extent_template.substitute(
            xmin=bbox['minx'], ymin=bbox['miny'], xmax=bbox['maxx'], ymax=bbox['maxy']
        )
        dataset = destination.action.package_show(id=result['name'])
        dataset['spatial'] = extent_string.strip()
        dataset['spatial_coverage'] = dataset['spatial']
        if 'data_state' not in dataset:
            dataset['data_state'] = 'Active'
        if 'contact_point' not in dataset:
            dataset['contact_point'] = 'data.gov@finance.gov.au'
        if update:
            destination.call_action('package_update', dataset)
        print "dataset spatial coverage " + result['name'] + ' updated'

for result in results['results']:

        bad_extent = False
        if 'spatial' not in result:
            bad_extent = True
        else:
            bad_extent = 'Point' in result['spatial']
        for res in result['resources']:
            if 'aims' not in res['name'] \
                    and res['format'] == 'wms' \
                    and ('wms_layer' not in res or bad_extent):
                try:
                    wms = get_wms(res['url'])
                    layers = list(wms.contents)
                    if len(layers) == 1:
                        layer_harvest(res,result,wms,layers[0])
                    else:
                        if res['name'] in layers:
                            layer_harvest(res,result,wms,res['name'])
                except Exception, e:
                    print "Exception loading "+result['name']+" WMS URL "+res['url']
                    print str(e)
