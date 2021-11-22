from mongoengine import *

from utils import mongo_to_dict_helper


class File(Document):
    fileName = StringField(required=True)
    size = IntField(required=True)
    content_type = StringField()
    storage_mode = StringField()
    storage_details = StringField()
    created = DateField()
    meta = {'collection': 'files'}

    def to_dict(self):
        return mongo_to_dict_helper(self)
