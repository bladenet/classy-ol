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

# /type/edition	/books/OL10000179M	4	2010-04-24T17:54:01.503315	{"publishers": ["Stationery Office"], "physical_format": "Hardcover", "subtitle": "26 January - 4 February 1998", "title": "Parliamentary Debates, House of Lords, 1997-98", "isbn_10": ["0107805855"], "identifiers": {"goodreads": ["2862283"]}, "isbn_13": ["9780107805852"], "edition_name": "5th edition", "languages": [{"key": "/languages/eng"}], "number_of_pages": 124, "last_modified": {"type": "/type/datetime", "value": "2010-04-24T17:54:01.503315"}, "latest_revision": 4, "key": "/books/OL10000179M", "authors": [{"key": "/authors/OL2645811A"}], "publish_date": "January 1999", "works": [{"key": "/works/OL7925994W"}], "type": {"key": "/type/edition"}, "subjects": ["Bibliographies, catalogues, discographies", "POLITICS & GOVERNMENT", "Reference works", "Bibliographies & Indexes", "Reference"], "revision": 4}

def make_book(isbn, data):
    global authors_db
    data = data.copy()
    data.pop('isbn_10', None)
    data.pop('isbn_13', None)
    data['isbn'] = isbn
    authors = data.pop('authors', [])
    authors_names = []
    for author_dict in authors:
        key = author_dict.get('key', None)
        if key:
            key = unicodedata.normalize('NFKD', key).encode('ascii', 'ignore')
            author_name = authors_db.get(key, None)
            if author_name:
                authors_names.append(authors_db[author_name])
    # authors_names = [authors_db[author] for author in authors]
    authors_string = ", ".join(authors_names)
    data['author'] = authors_string
    publish_date = data.pop('publish_date', None)
    if publish_date:
        data['publication_date'] = publish_date
    key = data.pop('key', None)
    if key:
        data['open_library_id'] = key
    return data


def book_iterator():
    # load_author_cache()
    # 24897627 lines to scan
    f = gzip.open('ol_dump_editions_latest.txt.gz')
    for line in f:
        (type, olid, edition, date, json_data) = line.split('\t')
        parsed_json = json.loads(json_data)
        isbn_10 = parsed_json.get('isbn_10', None)
        isbn_13 = parsed_json.get('isbn_13', None)
        if isbn_13:
            for isbn in isbn_13:
                yield make_book(isbn, parsed_json)
        elif isbn_10:
            for isbn in isbn_10:
                yield make_book(isbn, parsed_json)


@clize
def upload_books():
    num_books = 24897627 # in the main file
    import math

    one_percent = math.ceil(num_books / 100)
    count = 0
    books_to_upload = []
    for book in book_iterator():
        count += 1
        books_to_upload.append(book)
        if (count % 100) == 0:
            upload_book_set(books_to_upload)
            books_to_upload = []
        if count % one_percent == 0:
            print "{:.0f}%".format((count * 100) / num_books)
    if len(books_to_upload) > 0:
        upload_book_set(books_to_upload)


def upload_book_set(books):
    global s
    message = { "books": books}
    print message
    #response = s.post('http://classy-ol.herokuapp.com/addBook/', data=json.dumps(message))
#    response = s.post('http://localhost:5000/addBook/', data=json.dumps(message))
#    print response


@clize
def verify_upload():
    s = requests.session()
    input_filename = os.path.dirname(os.path.realpath(__file__)) + '/udata.json'
    with open(input_filename, 'r') as f:
        datas = f.read()
        data = json.loads(datas)
        response = s.post('http://localhost:5000/addBook/', data=json.dumps(data))
        print response



# COMMAND LINE INVOCATION
if __name__ == '__main__':
    run((make_author_cache, make_author_cache, verify_upload))
