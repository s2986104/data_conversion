import json
import os.path


GCMS = json.load(open(os.path.join(os.path.dirname(__file__), 'gcm.json'), 'r'))
