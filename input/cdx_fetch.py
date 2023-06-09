#!/usr/bin/env python

import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='ia')
#url = 's0.2mdn.net/sadbundle/*'
#url = 's0.2mdn.net/simgad/*'
#url = 's0.2mdn.net/ads/*'
#url = 's0.2mdn.net/ads/studio/cached_libs/*'
url = 's0.2mdn.net/sadbundle/*'
label = url.replace('/', '_')
warcinfo = {
    'software': 'pypi_cdx_toolkit webads',
    'isPartOf': label + '-INTERNETARCHIVE',
    'description': 'warc extraction',
    'format': 'WARC file version 1.0',
}

writer = cdx_toolkit.warc.get_writer(
    label, 'INTERNETARCHIVE', warcinfo, warc_version='1.1')

# for obj in cdx.iter(url, limit=10000, filter=['mime:text/html', 'status:200']):
# for obj in cdx.iter(url, limit=10000, filter=['type:image']):
for obj in cdx.iter(url, limit=10000):
    url = obj['url']
    status = obj['status']
    timestamp = obj['timestamp']

    print('considering extracting url', url, 'timestamp', timestamp)
    if status != '200':
        print(' skipping because status was {}, not 200'.format(status))
        continue

    try:
        record = obj.fetch_warc_record()
    except RuntimeError:
        print(' skipping capture for RuntimeError 404: %s %s', url, timestamp)
        continue
    writer.write_record(record)

    print(' wrote', url)
