"""
Description: Parent module laregely used to ensure a consistent
interface for communicating with kafka
"""

import sys
import json
from os.path import exists
from beartype import beartype
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

class InitializeSynapse():
	"""
		Description: Initializes Kafka for synapses
		Responsible for:
			1. Creating topics
	"""
	@beartype
	def __init__(self, environment: str) -> None:
		"""
			Description: Contrstruct for initializing Kafka
			Responsible for:
				1. Confirm the kafka settings file exists
				2. Load settings
				3. Init other variables
		"""

		self.environment = environment

		# if settings file does not exist exit
		if not exists(f"cfg/{self.environment}/settings.json"):
			print(f"Settings file not found: {sys.argv[1]}")
			sys.exit()

		# Load db connection settings
		print('Loading kafka settings')
		with open(f"cfg/{self.environment}/settings.json", 'r', encoding="utf-8") as settings_file:
			settings_json = settings_file.read()
		self.settings = json.loads(settings_json)

		self.admin_client = KafkaAdminClient(
			bootstrap_servers=f"{self.settings['kafka']['connection']['ip']}" +
				f":{self.settings['kafka']['connection']['port']}")


	@beartype
	def get_topics(self) -> set:
		"""
			Description: Resets Kafaka for synapses
			Responsible for:
				1. retrieving a list of topics
		"""
		print('Getting topics')
		consumer = KafkaConsumer(bootstrap_servers=f"{self.settings['kafka']['connection']['ip']}" +
			f":{self.settings['kafka']['connection']['port']}")
		return consumer.topics()

	@beartype
	def initialize(self) -> None:
		"""
			Description: Initializes Kafaka for synapses
			Responsible for:
				1. Creates topics
		"""

		topics = []
		print('Creating Topics')
		for topic in self.settings['kafka']['topics']:
			print(f"  {self.settings['kafka']['topics'][topic]}")
			topics.append(NewTopic(name=self.settings['kafka']['topics'][topic],
				num_partitions=1, replication_factor=1))

		self.admin_client.create_topics(new_topics=topics, validate_only=False)

	@beartype
	def reset(self) -> None:
		"""
			Description: Resets Kafaka for synapses
			Responsible for:
				1. Deletes topics
		"""
		topics = []
		print('Deleting Topics')
		for topic in self.settings['kafka']['topics']:
			print(f"  {self.settings['kafka']['topics'][topic]}")
			topics.append(self.settings['kafka']['topics'][topic])

		self.admin_client.delete_topics(topics)
