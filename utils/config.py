import os
import codecs

import pymongo
import yaml


default_config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yml')
# print(default_config_path)


def load_yaml(path):
    with codecs.open(path, encoding='utf-8') as f:
        return yaml.safe_load(f)

class Config:
    def __init__(self, path=None):
        if path is None:
            path = default_config_path
        self.base = load_yaml(path)['base']
        self.persist_db = pymongo.MongoClient(self.base["mongo_url"])[self.base["mongo_db"]]


    
