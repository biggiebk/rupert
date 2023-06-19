"""
Description: Parent module laregely used to ensure a consistent
interface for communicating with kafka
"""

import sys
import json
import time
from os.path import exists
from beartype import beartype
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

class Synapse():
	"""
		Description: Parent class used by synapses.
		Responsible for:
			1. Basic constructor for starting synapses.
			2. Initiates Kafka cosumer
			3. Contains method to push to Kafka as producer
	"""
	@beartype
	def __init__(self, settings_file: str) -> None:
		"""
			Construct for synapse classes.
			Responsible for:
				1. Loading settings.
				2. Initate topic consumer
			Requires:
				settings_file - Path to the synapse setting file
		"""
		self.settings_file = settings_file
		self.settings = {}
		self.__load_settings()
		self.settings['settings_file'] = settings_file
		self.consumer_cfg = self.settings['kafka']['connection'] | self.settings['kafka']['consumer']
		self.consumer = None
		self.close_consumer = False

	@beartype
	def listen(self, topic: str) -> None:
		"""
			Description: Connects to Kafka and consumes a topic
			Responsible for:
				1. Connecting to Kafka and listening for events/messages
				2. Calls the process_event method
			Requires:
				Nothing
			Raises:
				RuntimeError if called on a closed consumer
		"""
		self.consumer = Consumer(self.consumer_cfg)
		self.consumer.subscribe([self.settings['kafka']['topics'][topic]])
		while True:
			if self.close_consumer:
				self.consumer.close()
				sys.exit()
			msg = self.consumer.poll(1.0)
			if msg is None:
				pass
			elif msg.error():
				print(msg.error())
			else:
				self.process_event(msg)

	@beartype
	def process_event(self, consumer_message) -> None:
		"""
			Description: Each synapse should overide this method. The code here mostly
				is to support testing connectivity with Kafka
			Responsible for:
				1. Converts the messages value to string
				2. Returns the string
				3. Quits the class
			Requires:
				consuer_message
		"""
		print(consumer_message.value().decode('utf-8'))
		time.sleep(10)
		self.stop()

	@beartype
	def reload(self):
		"""
			Reloads the synapse
				1. Reloads the configuration
		"""
		self.__load_settings()

	@beartype
#	def send(self, topic, event_bytes: bytes) -> None:
	def send(self, topic, event_bytes) -> None:
		"""
			Description: Sends a byte array to Kafka as a producer
			Responsible for:
				1. Send event bytes to topic
			Requires:
				1. topic - Name of the topic to send message/event to (string)
				2. event_bytes - array of bytes
			Raises:
				BufferError - if the internal producer message queue is full 
				KafkaException - for other errors, see exception code
				NotImplementedError - if timestamp is specified without underlying library support.
		"""
		producer = Producer(self.settings['kafka']['connection'])
		producer.produce(self.settings['kafka']['topics'][topic], event_bytes)
		producer.poll(10000)
		producer.flush()

	@beartype
	def stop(self) -> None:
		"""Closes the the consumer and exits"""
		self.close_consumer = True

	## Private methods, best not to overide anything beyond this point

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


class InitializeSynapse():
	"""
		Description: Initializes Kafka for synapses
		Responsible for:
			1. Creating topics
	"""
	@beartype
	def __init__(self, settings_file: str) -> None:
		"""
			Description: Contrstruct for initializing Kafka
			Responsible for:
				1. Confirm the kafka settings file exists
				2. Load settings
				3. Init other variables
		"""

		self.settings_file = settings_file

		if not exists(settings_file):
			print(f"Settings file not found: {settings_file}")
			sys.exit()

		# Load db connection settings
		print('Loading kafka settings')
		with open(settings_file, 'r', encoding="utf-8") as settings_file_content:
			settings_json = settings_file_content.read()
		self.settings = json.loads(settings_json)

		self.consumer_cfg = self.settings['kafka']['connection'] | self.settings['kafka']['consumer']
		self.admin_client = AdminClient(self.settings['kafka']['connection'])


	@beartype
	def get_topics(self) -> dict:
		"""
			Description: Resets Kafaka for synapses
			Responsible for:
				1. retrieving a list of topics
			Raises
				KafkaException
		"""
		consumer = Consumer(self.consumer_cfg)
		return consumer.list_topics().topics

	@beartype
	def initialize(self) -> None:
		"""
			Description: Initializes Kafaka for synapses
			Responsible for:
				1. Creates topics
			Raises:
        KafkaException - Operation failed locally or on broker.
        TypeException - Invalid input.
        ValueException - Invalid input.
		"""

		topics = []
		print('Creating Topics')
		for topic in self.settings['kafka']['topics']:
			print(f"  {self.settings['kafka']['topics'][topic]}")
			topics.append(NewTopic(topic=self.settings['kafka']['topics'][topic],
				num_partitions=1, replication_factor=1))

		topic_futures = self.admin_client.create_topics(new_topics=topics, validate_only=False)

		# Wait for create to complete or for an exception to be thrown
		for topic, future in topic_futures.items():
			future.result()  # The result itself is None

	@beartype
	def reset(self) -> None:
		"""
			Description: Resets Kafaka for synapses
			Responsible for:
				1. Deletes topics
			Raises:
        KafkaException - Operation failed locally or on broker.
        TypeException - Invalid input.
        ValueException - Invalid input.
		"""
		topics = []
		print('Deleting Topics')
		for topic in self.settings['kafka']['topics']:
			print(f"  {self.settings['kafka']['topics'][topic]}")
			topics.append(self.settings['kafka']['topics'][topic])

		topic_futures = self.admin_client.delete_topics(topics)

		# Wait for delete to complete or for an exception to be thrown
		for topic, future in topic_futures.items():
			future.result()  # The result itself is None
