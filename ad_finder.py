from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord
from pandas import DataFrame
from itertools import islice

path_to_warc = "/home/cr625/ads/crawl-data/CC-MAIN-2016-07/11425/ARCHIVEIT-11425-CRAWL_SELECTED_SEEDS-JOB711732-SEED1891913-20181119161118079-00000-4ksqfmda.warc.gz"

warc_file = open(path_to_warc, 'rb')
records = ArchiveIterator(warc_file, arc2warc=True)

# flatten for pandas
example_record = islice(records, 10)


def extract_warc_info():
    for record in example_record:
        # format = record.format # warc or arc
        type = record.rec_type  # response, request, warcinfo, etc.
        headers = record.rec_headers
        target_uri = headers.get_header('WARC-Target-URI')
        content_length = headers.get_header('Content-Length')
        content_type = headers.get_header('Content-Type')
        a = headers.get_header('WARC-Type')
        b = headers.get_header('WARC-Block-Digest')
        c = headers.get_header('WARC-IP-Address')
        d = headers.get_header('WARC-Refers-To')
        e = headers.get_header('WARC-Profile')  # revisit only
        f = headers.get_header('WARC-Identified-Payload-Type')
        g = headers.get_header('WARC-Date')
        h = headers.get_header('WARC-Record-ID')
        i = headers.get_header('WARC-Payload-Digest')
        j = headers.get_header('WARC-Target-URI')
        k = headers.get_header('WARC-Refers-To-Target-URI')
        l = headers.get_header('Concurrent-To')
        m = headers.get_header('WARC-Truncated')
        n = headers.get_header('WARC-Warcinfo-ID')
        o = headers.get_header('Filename')  # warcinfo only
        q = headers.get_header('WARC-Segment-Number')
        r = headers.get_header('WARC-Segment-Total-Length')
        s = headers.get_header('WARC-Segment-Origin-ID')  # continuation only

        http_headers = record.http_headers
        out = {'WARC-Type': a, 'WARC-Block-Digest': b, 'WARC-IP-Address': c, 'WARC-Refers-To': d, 'WARC-Profile': e,
               'WARC-Identified-Payload-Type': f, 'WARC-Date': g, 'WARC-Record-ID': h, 'WARC-Payload-Digest': i,
               'WARC-Target-URI': j, 'WARC-Refers-To-Target-URI': k, 'Concurrent-To': l, 'WARC-Truncated': m,
               'WARC-Warcinfo-ID': n, 'Filename': o, 'WARC-Segment-Number': q, 'WARC-Segment-Total-Length': r,
               'WARC-Segment-Origin-ID': s}
        #http_content_type = http_headers.get_header('Content-Type')

        print(record.http_headers)


'''
        http_content_type = http_headers.get_header('Content-Type')
        http_content_length = http_headers.get_header('Content-Length')
        http_status = http_headers.get_header('Status')

        rec_content_type = record.content_type
        rec_length = record.length
        rec_payload_length = record.payload_length
        rec_read_content = record.content_stream().read()
        rec_digest_checker = record.digest_checker
'''


if __name__ == "__main__":
    extract_warc_info()
