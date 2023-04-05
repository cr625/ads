from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord
from pandas import DataFrame
from itertools import islice

path_to_warc = "/home/chris/ads/crawl-data/11425/ARCHIVEIT-11425-CRAWL_SELECTED_SEEDS-JOB711732-SEED1891913-20181119161118079-00000-4ksqfmda.warc.gz"

warc_file = open(path_to_warc, 'rb')
records = ArchiveIterator(warc_file, arc2warc=True)

# flatten for pandas
example_record = islice(records, 10)


def extract_warc_info():
    for record in records:
        # format = record.format # warc or arc
        # response, request, revisit, metadata, resource, conversion, or warcinfo
        rec_type = record.rec_type
        rec_headers = record.rec_headers
        rec_content_type = record.content_type
        rec_length = record.length
        rec_payload_length = record.payload_length
        rec_digest_checker = record.digest_checker
        rec_read_content = record.content_stream().read()

        warc_target_uri = rec_headers.get_header('WARC-Target-URI')
        a = rec_headers.get_header('WARC-Type')
        b = rec_headers.get_header('WARC-Block-Digest')
        c = rec_headers.get_header('WARC-IP-Address')
        d = rec_headers.get_header('WARC-Refers-To')
        e = rec_headers.get_header('WARC-Profile')  # revisit only
        f = rec_headers.get_header('WARC-Identified-Payload-Type')
        g = rec_headers.get_header('WARC-Date')
        h = rec_headers.get_header('WARC-Record-ID')
        i = rec_headers.get_header('WARC-Payload-Digest')
        j = rec_headers.get_header('WARC-Target-URI')
        k = rec_headers.get_header('WARC-Refers-To-Target-URI')
        l = rec_headers.get_header('Concurrent-To')
        m = rec_headers.get_header('WARC-Truncated')
        n = rec_headers.get_header('WARC-Warcinfo-ID')
        o = rec_headers.get_header('Filename')  # warcinfo only
        q = rec_headers.get_header('WARC-Segment-Number')
        r = rec_headers.get_header('WARC-Segment-Total-Length')
        s = rec_headers.get_header('WARC-Segment-Origin-ID')  # continuation

        http_headers = record.http_headers
        if http_headers is not None:
            http_headers_status = http_headers.statusline
            http_headers_headers = http_headers.headers
            http_headers_protocol = http_headers.protocol

            # item
            http_content_length = http_headers.get_header('Content-Length')
            http_content_type = http_headers.get_header('Content-Type')
            http_host = http_headers.get_header('Host')
            http_origin = http_headers.get_header('Origin')
            http_referer = http_headers.get_header('Referer')
            # warc headers

            out = {'WARC-Type': a, 'WARC-Block-Digest': b, 'WARC-IP-Address': c, 'WARC-Refers-To': d, 'WARC-Profile': e,
                   'WARC-Identified-Payload-Type': f, 'WARC-Date': g, 'WARC-Record-ID': h, 'WARC-Payload-Digest': i,
                   'WARC-Target-URI': j, 'WARC-Refers-To-Target-URI': k, 'Concurrent-To': l, 'WARC-Truncated': m,
                   'WARC-Warcinfo-ID': n, 'Filename': o, 'WARC-Segment-Number': q, 'WARC-Segment-Total-Length': r,
                   'WARC-Segment-Origin-ID': s}

            print(http_headers_headers)

    if record.rec_type == 'response':
        print(record.rec_headers.get_header('WARC-Target-URI'))
        print(record.http_headers.get_header('Content-Type'))
        print(record.http_headers.get_header('Content-Length'))
        print(record.http_headers.get_header('Host'))
        print(record.http_headers.get_header('Origin'))
        print(record.http_headers.get_header('Referer'))
        print(record.http_headers.get_header('User-Agent'))
        print(record.http_headers.get_header('Accept'))
        print(record.http_headers.get_header('Accept-Encoding'))
        print(record.http_headers.get_header('Accept-Language'))
        print(record.http_headers.get_header('Cookie'))
        print(record.http_headers.get_header('Connection'))
        print(record.http_headers.get_header('Upgrade-Insecure-Requests'))
        print(record.http_headers.get_header('Cache-Control'))
        print(record.http_headers.get_header('TE'))
        print(record.http_headers.get_header('Pragma'))
        print(record.http_headers.get_header('If-Modified-Since'))
        print(record.http_headers.get_header('If-None-Match'))
        print(record.http_headers.get_header('X-Forwarded-For'))
        print(record.http_headers.get_header('X-Forwarded-Host'))
        print(record.http_headers.get_header('X-Forwarded-Server'))
        print(record.http_headers.get_header('X-Real-IP'))
        print(record.http_headers.get_header('X-Forwarded-Proto'))
        print(record.http_headers.get_header('X-Forwarded-Port'))
        print(record.http_headers.get_header('X-Forwarded-For'))
        print(record.http_headers.get_header('X-Forwarded-Host'))
        print(record.http_headers.get_header('X-Forwarded-Server'))
        print(record.http_headers.get_header('X-Real-IP'))
        print(record.http_headers.get_header('X-Forwarded-Proto'))
        print(record.http_headers.get_header('X-Forwarded-Port'))
        print(record.http_headers.get_header('X-Forwarded-For'))
        print(record.http_headers.get_header('X-Forwarded-Host'))
        print(record.http_headers.get_header('X-Forwarded-Server'))
        print(record.http_headers.get_header('X-Real-IP'))
        print(record.http_headers.get_header('X-Forwarded-Proto'))
        print(record.http_headers.get_header('X-Forwarded-Port'))
        print(record.http_headers.get_header('X-Forwarded-For'))
        print(record.http_headers.get_header('X-Archive-Orig-Host'))


if __name__ == "__main__":
    extract_warc_info()
