from mongows.initializers.util import load_data_from_mongodump
from mongows.validators.ValidationTest import get_file_in_dir


def run(res_id):
    path = get_file_in_dir(__file__, 'albums.bson')
    load_data_from_mongodump(res_id, path, 'albums',)

    path = get_file_in_dir(__file__, 'images_init.bson')
    load_data_from_mongodump(res_id, path, 'images',)
