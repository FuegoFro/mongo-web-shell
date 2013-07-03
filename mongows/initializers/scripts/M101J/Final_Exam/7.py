import os
from mongows.initializers.util import (
    load_data_from_mongodump,
)


def run(res_id):
    base_path = os.path.join(
        os.getcwd(),
        'mongows',
        'initializers',
        'scripts',
        'M101J',
        'Final_Exam'
    )
    path = os.path.join(base_path, 'albums.bson')
    load_data_from_mongodump(res_id, path, 'albums',)

    path = os.path.join(base_path, 'images_init.bson')
    load_data_from_mongodump(res_id, path, 'images',)
