import unicodedata
import os
import json
import logging
import gzip
import dbm
import requests
from clize import clize, run

env = os.environ.get('CLASSY_CRAWLER_ENV', 'dev')
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.WARN)
logger = logging.getLogger('classy_crawler.startup')
logger.setLevel(level=logging.DEBUG if env == 'dev' else logging.INFO)

authors_db = None
authors_db_filename = os.path.dirname(os.path.realpath(__file__)) + '/authors'

@clize
def make_author_cache():
    global authors_db
    authors_db = dbm.open(authors_db_filename, 'c')
    f = gzip.open('ol_dump_authors_latest.txt.gz')
    count = 0
    for line in f:
        count += 1
        stuff = line.split('\t')
        data = json.loads(stuff[4])
        author = data.get('name', None)
        key = data.get('key', None)
        if author and key:
            author = unicodedata.normalize('NFKD', author).encode('ascii', 'ignore')
            authors_db[key] = author
        if count % 1000 == 0:
            print count


def load_author_cache():
    global authors_db
    authors_db = dbm.open(authors_db_filename, 'r')


def english_book_iterator():
    # 24897627 lines to scan
    f = gzip.open('ol_dump_editions_latest.txt.gz')
    for line in f:
        things = line.split('\t')
        data = things[4]
        edition = json.loads(data)
        languages = edition.get('languages', [{'key': u'/languages/eng'}])
        for language in languages:
            lang = language.get('key', None)
            if lang == '/languages/eng':
                yield edition


@clize
def prepare_for_amazon():
    load_author_cache()
    output = 'amazon.jsonl'
    with open(output, 'w') as f:
        count = 0
        for edition in english_book_iterator():
            count += 1
            amazon = amazon_edition(**edition)
            print >> f, json.dumps(amazon)
            if count % 10000 == 0:
                print count


@clize
def generate_chunks_for_amazon():
    input = 'amazon.jsonl'
    chunk_status = {'segment': 0, 'data': "[", 'accumulated_length': 1}

    def blow_chunks():
        chunk_status['data'] = chunk_status['data'] + "]"
        output = "chunk{:.0f}.json".format(chunk_status['segment'])
        print output
        chunk_status['segment'] = chunk_status['segment'] + 1

        with open(output, 'w') as output_file:
            print >> output_file, chunk_status['data']
        chunk_status['data'] = "["
        chunk_status['accumulated_length'] = 1

    with open(input, 'r') as f:
        for line in f:
            line_length = len(line)
            if (line_length + chunk_status['accumulated_length'] + 1) > 5242000:
                # if this line would make us exceed 5MB, dump
                blow_chunks()
            else:
                if chunk_status['accumulated_length'] > 1:
                    chunk_status['data'] = chunk_status['data'] + "," + line
                    chunk_status['accumulated_length'] = chunk_status['accumulated_length'] + line_length + 1
                else:
                    chunk_status['data'] = chunk_status['data'] + line
                    chunk_status['accumulated_length'] = chunk_status['accumulated_length'] + line_length
    if chunk_status['accumulated_length'] > 1:
        blow_chunks()



def amazon_edition(key=None, title=None, isbn_10=None, isbn_13=None, authors=None, **kwargs):
    global authors_db
    if not title:
        return None
    if not key:
        return None
    if isbn_10 is None:
        isbn_10 = []
    if isbn_13 is None:
        isbn_13 = []
    if authors is None:
        authors = []
    authors_names = []
    for author in authors:
        author_name = authors_db.get(author, None)
        if author_name:
            authors_names.append(author_name)
    isbns = isbn_10 + isbn_13
    return {"type": "add",
            "id":   key,
            "fields": {
                "title": title,
                "authors": authors_names,
                "isbns": isbns
                        }
            }


# COMMAND LINE INVOCATION
if __name__ == '__main__':
    run((make_author_cache, make_author_cache, prepare_for_amazon, generate_chunks_for_amazon))
