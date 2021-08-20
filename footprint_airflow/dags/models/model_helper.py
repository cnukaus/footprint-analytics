from abc import ABCMeta

import pymongo
from bson import ObjectId

from utils.date_util import DateUtil
from config import project_config

default_db_uri = project_config.mongodb_defi_up_uri

default_db_name = project_config.db_name


class ModelHelper(metaclass=ABCMeta):
    collection = None
    collection_name = None
    db = None
    db_name = default_db_name
    index_list = []
    db_uri = default_db_uri

    def __init__(self):
        self.db = pymongo.MongoClient(self.db_uri, serverSelectionTimeoutMS=60000)[self.db_name]
        self.collection = self.db[self.collection_name]
        # self._keep_index()

    def _keep_index(self):
        """create index"""
        for index_data in self.index_list:
            self.collection.create_index(keys=index_data if isinstance(index_data, list) else [index_data],
                                         background=True)

    @staticmethod
    def id_to_string(x: dict) -> dict:
        x['_id'] = str(x['_id'])
        return x

    @staticmethod
    def string_to_id(x: dict) -> dict:
        x['_id'] = ObjectId(x['_id'])
        return x

    def find_list(self, query: dict, projection: dict = None, *args, **kwargs) -> list:
        result = self.collection.find(query, projection, *args, **kwargs)
        return list(result)

    def find_one(self, query: dict, projection: dict = None) -> (object, None):
        return self.collection.find_one(query, projection)

    def find_by_id(self, _id: str, projection: dict = None) -> (object, None):
        return self.collection.find_one({'_id': ObjectId(_id)}, projection)

    def count(self, query: dict) -> int:
        return self.collection.count(query)

    def update(self, *args, **kwargs):
        return self.collection.update(*args, **kwargs)

    def remove(self, *args, **kwargs):
        return self.collection.remove(*args, **kwargs)

    def update_one(self, query: dict, set_dict: dict, upsert: bool = False):
        # default is set
        now = DateUtil.utc_current()
        set_dict.update({
            "updatedAt": now
        })
        result = self.collection.update_one(
            query,
            {
                '$set': set_dict,
                "$setOnInsert": {"createdAt": now}
            },
            upsert
        )
        return result

    def update_many(self, query: dict, set_dict: dict, upsert: bool = False):
        now = DateUtil.ind_current()
        set_dict.update({
            "updatedAt": now
        })
        result = self.collection.update(
            query,
            {
                '$set': set_dict,
                "$setOnInsert": {"createdAt": now}
            },
            upsert,
            multi=True
        )
        return result

    def update_by_id(self, _id, set_dict: dict):
        return self.update_one({'_id': ObjectId(_id)}, set_dict)

    def insert_one(self, data: dict):
        now = DateUtil.ind_current()
        data = {
            'createdAt': now,
            'updatedAt': now,
            **data
        }
        try:
            return self.collection.insert_one(data)
        except BaseException as e:
            print(e)
            return

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)

    def distinct(self, *args, **kwargs):
        return self.collection.distinct(*args, **kwargs)

    def aggregate(self, *args, **kwargs):
        return self.collection.aggregate(*args, **kwargs)

    def insert_many(self, data: list, ordered=True, *args, **kwargs):
        return self.collection.insert_many(data, ordered=ordered, *args, **kwargs)
