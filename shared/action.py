"""
Description: Parent module for creating actions.
"""
import json
import beartype
from pymongo import MongoClient

class Action():
	"""
		Description: Parent class used by synapses.
		Responsible for:
			1. CRUD interface to MongoDB
				- Create
				- Read
				- Update
				- Delete
			2. Contains trigger method
	"""

	@beartype
	def __init__(self, settings_file: str) -> None:
		"""
			Construct for action classes.
			Responsible for:
				1. Loading settings.
			Requires:
				settings_file - Path to the synapse setting file
		"""
		self.settings_file = settings_file
		self.settings = {}
		self.__load_settings()

# CRUD
	def create(self) -> None:
		"""Creates/saves actions"""
		pass

	def read(self) -> None:
		"""Reads actions"""
		pass

	def update(self) -> None:
		"""Updates actions"""
		pass

	def delete(self) -> None:
		"""Deletes actions"""
		pass

# Processing
	def trigger(self) -> None:
		"""Triggers the action"""
		pass

# Private methods
	@beartype
	def __load_settings(self) -> None:
		"""
			Description: Parent class used by other synapes.
			Responsible for:
				1. Loading settings
			Requires:
				settings_file - Path to the synapses setting file
		"""
		with open(self.settings_file, 'r', encoding='utf-8') as settings:
			settings_json = settings.read()
		self.settings = json.loads(settings_json)


class ActionSchema():
	"""
		Description: Class responsible for working with Action Schemas (Mongo Collections)
		Responsible for:
			1. Create schema into MongoDB
			2. Delete schema from MongoDB
			3. Upgrade schema in Mongo
			4. Archive/backup collection
	"""

	@beartype
	def __init__(self, settings_file: str) -> None:
		"""
			Construct for action classes.
			Responsible for:
				1. Loading settings.
			Requires:
				settings_file - Path to the synapse setting file
		"""
		self.settings_file = settings_file
		self.settings = {}
		self.__load_settings()

	@beartype
	def create(self) -> None:
		"""Create a schema/collection in mongodb"""
		pass


	@beartype
	def delete(self) -> None:
		"""Delete a schema/collection from mongodb"""
		pass

	@beartype
	def upgrade(self) -> None:
		"""This will come later"""
		pass

# Private methods
	@beartype
	def __load_settings(self) -> None:
		"""
			Description: Parent class used by other synapes.
			Responsible for:
				1. Loading settings
			Requires:
				settings_file - Path to the synapses setting file
		"""
		with open(self.settings_file, 'r', encoding='utf-8') as settings:
			settings_json = settings.read()
		self.settings = json.loads(settings_json)
