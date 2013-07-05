import os
from bson.json_util import loads
from mongows.validators.ValidationTest import ValidationTest, get_file_in_dir


class ValidationTestCase:
    def __init__(self, res_id):
        self.test_case = ValidationTest(res_id)

    def run(self):
        path = get_file_in_dir(__file__, 'images_done.json')
        with open(path) as reference_file:
            reference = loads(reference_file.read())
            collections_equal = self.test_case.collection_equals('images',
                                                                 reference)
            return 'pass' if collections_equal else 'fail'
