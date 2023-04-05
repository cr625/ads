import os
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

import pandas

dir = 'crawl-data/11425'
dir_list = os.listdir(dir)

path = os.path.join(dir, dir_list[0])


def process_record(record):
    rows = []
    row = pandas.Series({"type": record.rec_type,
                         "headers": record.rec_headers,
                         "content_type": record.content_type,
                         "length": record.length,
                         "payload_length": record.payload_length,
                         "digest_checker": record.digest_checker,
                         "content": record.content_stream().read(),
                         "http_headers": record.http_headers})
    rows.append(row)
    return pandas.DataFrame(rows)


def process_records(records):
    for record in records:
        yield process_record(record)


def process_file(path):
    warc_file = open(path, 'rb')
    records = ArchiveIterator(warc_file, arc2warc=True)
    return pandas.concat(process_records(records))


def process_files():
    results = []
    for file in dir_list[1:]:
        path = os.path.join(dir, file)
        result = process_file(path)
        results.append(result)
    return pandas.concat(results)
