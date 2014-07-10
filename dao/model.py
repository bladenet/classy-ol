from flask_peewee.rest import RestResource
from peewee import *
from dao import database
from playhouse.postgres_ext import JSONField

class BaseModel(Model):
    class Meta:
        database = database
        

class OLBook(BaseModel):
    isbn = TextField(null=True)  # ? is it possible to have a book without an ISBN?  I think so
    title = TextField(null=True)
    author = TextField(null=True)
    publisher = TextField(null=True)
    publication_date = DateField(null=True)
    open_library_id = TextField()
    data = JSONField(null=True)
    
