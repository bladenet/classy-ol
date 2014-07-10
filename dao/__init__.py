import pwd
import os
import urlparse
from playhouse.postgres_ext import *

# get username for local system
username = pwd.getpwuid(os.getuid())[0]

# using get will return `none` if key is not present rather than raise a `KeyError`
HEROKU_POSTGRESQL_URL = os.getenv('DATABASE_URL', os.getenv('HEROKU_POSTGRESQL_AMBER_URL', 'postgres://' + username + '@localhost:5432/classy_ol'))
db_parsed_url = urlparse.urlparse(HEROKU_POSTGRESQL_URL)

database = PostgresqlExtDatabase(db_parsed_url.path[1:],user=db_parsed_url.username,password=db_parsed_url.password,host=db_parsed_url.hostname,port=db_parsed_url.port, autocommit=True, autorollback=True)