import yaml
import os.path


COLLECTIONS = {
    col['uuid']: col
    for col in
    yaml.safe_load(open(os.path.join(os.path.dirname(__file__), 'collections.yaml'), 'r'))['collections']
}


def collection_by_id(id):
    for col in COLLECTIONS.values():
        if col['_id'] == id:
            return col
    raise Exception('Collection {} not found.'.format(id))
