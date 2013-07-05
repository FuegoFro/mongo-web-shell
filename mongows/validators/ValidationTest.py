import os
from mongows.mws.db import get_db
from mongows.mws.util import UseResId


class ValidationTest:
    def __init__(self, res_id):
        self.res_id = res_id
        self.db = get_db()

    # Collection must exactly equal the data set
    def collection_equals(self, collection, data):
        with UseResId(self.res_id):
            result = list(self.db[collection].find())
            return sorted(result) == sorted(data)

    # Data must be a subset of collection
    def collection_contains(self, collection, data):
        with UseResId(self.res_id):
            result = list(self.db[collection].find({'$or': data}, {'_id': 0}))
            return all(x in result for x in data)

    # Collection must contain one or more of the elements in data
    def collection_contains_any(self, collection, data):
        with UseResId(self.res_id):
            result = list(self.db[collection].find({'$or': data}, {'_id': 0}))
            return any(x in result for x in data)

    # Collection does not contain any of the elements in data
    def collection_contains_none(self, collection, data):
        return not self.collection_contains_any(collection, data)


def get_file_in_dir(module_file, file_name):
    script_path = os.path.realpath(module_file)
    return os.path.join(os.path.dirname(script_path), file_name)