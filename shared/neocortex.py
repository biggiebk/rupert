"""
	Description: Module pertaining to working the mongodb.
"""

import sys
import json
from os.path import exists
from pymongo import MongoClient
from beartype import beartype

class Neocortex():

	"""
		Description: Reusable database class
		Responsible for:
			1. Connecting DB
			2. CRUD Operations
	"""
	@beartype
	def __init__(self, environment: str) -> None:
		"""
			Description: Contrstruct for initializing the database
			Responsible for:
				1. Confirm the db settings file exists
				2. Load settings
				3. Init other variables
				4. Creating system collections, schemas, and indexes
		"""

		self.environment = environment

		# if settings file does not exist exit
		if not exists("cfg/%s/settings.json" %(self.environment)):
			print("Settings file not found: %s" %(sys.argv[1]))
			exit()

		# Load db connection settings
		print('Loading DB settings')
		with open("cfg/%s/settings.json" %(self.environment), 'r') as settings_file:
			settings_json = settings_file.read()
		self.settings = json.loads(settings_json)
		self.rupert_db = None

		self. rupert_mongo = MongoClient(self.settings['database']['db_host'], self.settings['database']['db_port'],
		  username=self.settings['database']['rupert_user'], password=self.settings['database']['rupert_password'])
		self.rupert_db = self.rupert_mongo[self.settings['database']['rupert_db_name']]

class InitializeNeocortex():
	"""
		Description: Initializes the database for rupert
		Responsible for:
			1. Creating the admin user
			2. Creating the database
			3. Creating the read/write user
			4. Creating system collections, schemas, and indexes
	"""
	@beartype
	def __init__(self, environment: str) -> None:
		"""
			Description: Contrstruct for initializing the database
			Responsible for:
				1. Confirm the db settings file exists
				2. Load settings
				3. Init other variables
				4. Creating system collections, schemas, and indexes
		"""

		self.environment = environment

		# if settings file does not exist exit
		if not exists("cfg/%s/settings.json" %(self.environment)):
			print("Settings file not found: %s" %(sys.argv[1]))
			exit()

		# Load db connection settings
		print('Loading DB settings')
		with open("cfg/%s/settings.json" %(self.environment), 'r') as settings_file:
			settings_json = settings_file.read()
		self.settings = json.loads(settings_json)

		self.rupert_mongo = None
		self.rupert_db = None

	@beartype
	def initialize(self) -> None:
		"""
			Description: Initializes the database for rupert
			Responsible for:
				1. Creating the admin user
				2. Creating the database
				3. Creating the read/write user
				4. Creating collections, schemas, and indexes
		"""
		print('Initial DB connection')
		self.rupert_mongo = MongoClient(self.settings['database']['db_host'], self.settings['database']['db_port'])
		# create admin user
		print("Creating admin user: %s" %(self.settings['database']['admin_user']))
		self.rupert_db = self.rupert_mongo['admin']
		self.rupert_db.command("createUser", self.settings['database']['admin_user'],
			pwd=self.settings['database']['admin_password'], roles=[{'role': "userAdminAnyDatabase", 'db': "admin"},
			"readWriteAnyDatabase"])

		# disconnect and create rupert user
		print("Disconnecting")
		self.rupert_db = None
		self.rupert_mongo.close()
		print("Print connecting as %s" %(self.settings['database']['admin_user']))
		self.rupert_mongo = MongoClient(self.settings['database']['db_host'], self.settings['database']['db_port'],
		  username=self.settings['database']['admin_user'], password=self.settings['database']['admin_password'])
		self.rupert_db = self.rupert_mongo['admin']
		print("Creating read/write user: %s" %(self.settings['database']['rupert_user']))
		self.rupert_db.command("createUser", self.settings['database']['rupert_user'], pwd=self.settings['database']['rupert_password'],
			roles=[{ 'role': "readWrite", 'db': self.settings['database']['rupert_db_name'] }])
		
		self.__close()

		print("Connecting as: %s" %(self.settings['database']['rupert_user']))
		self. rupert_mongo = MongoClient(self.settings['database']['db_host'], self.settings['database']['db_port'],
		  username=self.settings['database']['rupert_user'], password=self.settings['database']['rupert_password'])
		print("Create DB (%s) and required collections" %(self.settings['database']['rupert_db_name']))
		self.rupert_db = self.rupert_mongo[self.settings['database']['rupert_db_name']]
	
	def __close(self) -> None:
		""" Close DB connection"""
		# disconnect, connect as rupert user, and create collections
		print('Disconnecting')
		self.rupert_db = None
		self.rupert_mongo.close()

class Schema():
	"""
		Description: Module for working with MongoDB Schema
		Responsible for:
			1. Create/Delete Collections
			2. Manage Collection Schemata
			3. Manage Indexes
	"""
	@beartype
	def __init__(self, environment: str) -> None:
		"""
			Description: Contrstruct for initializing the database
			Responsible for:
				1. Confirm the db settings file exists
				2. Load settings
				3. Init other variables
				4. Creating system collections, schemas, and indexes
		"""

		self.environment = environment