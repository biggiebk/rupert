#!/usr/bin/python3
"""
Description: Confirm the ability to communicate with Kafka as a consumer and producer
"""
# !!! - This test assumes kafka is already running - !!!
import time
import pytest
import json
import threading
from shared.synapse import InitializeSynapse, Synapse

# !!! - This test assumes kafka is already running - !!!

test_msg = "I see you, this test works4"

# Ensure things are reset
initialize_synapse = InitializeSynapse('cfg/test/settings.json')
initialize_synapse.reset()


# sleep for a bit
time.sleep(30)
# Lets initialize kafka
initialize_synapse.initialize()

## Start the tests

def kafka_event():
	"""
		function to enable testing of both the producer and consumer in elements
	"""
	kafka_consumer = Synapse('cfg/test/settings.json')
	# Start the consumer
	thread = threading.Thread(target=kafka_consumer.listen, args=(["initialize_test"]))
	thread.isDaemon
	thread.start()
	time.sleep(1)
	kafka_producer = Synapse('cfg/test/settings.json')
	kafka_producer.send("initialize_test",test_msg)
	time.sleep(1)

def test_kafka(capsys):
	"""Calls the Kafaka event"""
	kafka_event()
	time.sleep(10)
	captured = capsys.readouterr()
	assert captured.out == f"{test_msg}\n"

with open('cfg/test/settings.json', 'r') as kafka_file:
	kafka_json = kafka_file.read()
kafka = json.loads(kafka_json)

@pytest.mark.parametrize("topic", kafka['kafka']['topics'])
def test_topics_exist(topic):
	existing_topics = initialize_synapse.get_topics()
	assert kafka['kafka']['topics'][topic] in existing_topics
