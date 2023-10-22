"""
Description: Test MongoDB modules (Neocortex)
"""
# !!! - This test assumes kafka is already running - !!!
import pytest
import json
from pymongo import MongoClient
from shared.neocortex import InitializeNeocortex

# Lets initialize the DB
initialize_neocortex = InitializeNeocortex('test')
initialize_neocortex.initialize()

# Load the settings and connect
with open("cfg/test/settings.json", 'r') as settings_file:
	settings_json = settings_file.read()
settings = json.loads(settings_json)
rupert_mongo = MongoClient(settings['database']['db_host'], settings['database']['db_port'],
  username=settings['database']['admin_user'], password=settings['database']['admin_password'])
rupert_db = rupert_mongo['admin']

def test_database():
	"""Confirm database was created"""
	results = rupert_mongo.list_database_names()
	assert settings['database']['rupert_db_name'] in results