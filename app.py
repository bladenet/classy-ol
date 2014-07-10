import cStringIO
import gzip
import dao
import psycopg2
import json
import urllib
import os

from flask import Flask, Response, request, redirect
from flask_peewee.auth import Auth
from flask_peewee.db import Database
from flask_peewee.admin import Admin
from flask_peewee.rest import RestAPI, UserAuthentication
from flask_peewee.utils import get_dictionary_from_model
from peewee import create_model_tables, drop_model_tables

from dao.model import *
from dao import *

import json
import httplib2

from datetime import timedelta
from flask import make_response, request, current_app
from functools import update_wrapper

app = Flask(__name__)
app.debug = bool(os.environ.get('APP_DEBUG', False))

# set the secret key.  keep this really secret:
app.secret_key = 'princessbride'

def Q_OR(q, nq):
    return q | nq if q else nq


@app.route('/')
def home():
    return '<h2>Classy OL Database</h2><p>Check out the <a href="https://github.com/bladenet/classy-ol" title="github repo">github repo</a>.</p>'


@app.route('/resetdb')
def resetdb():
    drop_model_tables([OLBook], fail_silently=True)
    create_model_tables([OLBook], fail_silently=True)
    return '<h2>Database Reset</h2>'


def get_dictionary(obj, additional_fields):
    # note that this will fail miserably if any of these additional fields refer to model objects
    data = get_dictionary_from_model(obj)
    for field in additional_fields:
        data[field] = getattr(obj, field)
    return data


@app.route("/api/search/<text>/")
@app.route("/api/search/<text>/<int:limit>")
def search(text, limit=25):
    text = urllib.unquote(text).decode('utf8').strip()
    text = text.replace(" ", " | ")

    if text == "":
        return Response(response=str(json.dumps({'query': text, 'results': []})), mimetype='application/json')

    # tsvector = "to_tsvector('english', title || ' ' || isbn || ' ' || author)"
    tsvector = "setweight(to_tsvector('english', coalesce(title,'')), 'A') || ' ' || setweight(to_tsvector('english', coalesce(author,'')), 'B')|| ' ' || setweight(to_tsvector('english', coalesce(isbn,'')), 'C')"
    tsquery = "to_tsquery('" + text + ":*')"
    rank = SQL("ts_rank(" + tsvector + ", " + tsquery + ")").alias('rank')
    rank2 = SQL("ts_rank_cd(" + tsvector + ", " + tsquery + ")").alias('rank_cd')

    query = OLBook.select(OLBook, rank, rank2).limit(limit)

    clause = SQL(tsvector + " @@ " + tsquery)
    query = query.where(clause)
    query = query.order_by(SQL('rank DESC'))

    results = query.execute()

    objects = [get_dictionary(i, ['rank', 'rank_cd']) for i in results]

    resj = json.dumps({
        'query': text,
        'results': objects})

    return Response(response=str(resj), mimetype='application/json')


@app.route("/addBook/", methods=['POST'])
def addBook():
    output = {"errors": []}
    if request.method == 'POST':
        books_json = request.get_json(force=True)

        if books_json and "books" in books_json:
            for book_json in books_json["books"]:
                isbn = book_json.get('isbn', 'no-such-isbn')
                try:
                    existing_book = OLBook.get(OLBook.isbn==isbn)
                    # ok, here we merge the existing book with the new values if the new values are "interesting"
                    for key, val in book_json.iteritems():
                        orig = getattr(existing_book, key, None)
                        if orig is None:
                            setattr(existing_book, key, val)
                        else:
                            if (val is not None) and (val is not 'Not Found') and (val is not ""):
                                setattr(existing_book, key, val)
                    # do we need to catch any save error?
                    existing_book.save()
                except DoesNotExist, e:
                    try:
                        OLBook.create(isbn=book_json.get('isbn', None),
                                      title=book_json.get('title', None),
                                      author=book_json.get('author', None),
                                      publication_date=book_json.get('publication_date', None),
                                      open_library_id = book_json["open_library_id"],
                                      data = book_json)

                    except KeyError:
                        output["errors"].append(book_json)

            return Response(response=str(json.dumps(output)), mimetype='application/json')
        else:
            return 'JSON Error'

    return 'Method Error'


@app.errorhandler(404)
def not_found(e):
    return '<h2>404 Error</h2>'


if __name__ == '__main__':
    create_model_tables([OLBook], fail_silently=True)
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)