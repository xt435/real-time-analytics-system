# 1. read from kafka, kafka broker, kafka topic
# 2. write back to kafka, kafka broker, new kafka topic

import atexit
import sys
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError, Kafka
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = ""
new_topic = ""
kafka_broker = ""

def shutdown_hook(producer):
	try:
		logger.info('flush pending message to kafa')
		producer.flush(20)
		logger.ino('finish flushing pending message')
	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending messages to kafka')
	finally:
		try:
			producer.close(20)
		except Exception as e:
			logger.warn('Failed to close kafka connection')

def process(timeobj, rd):
	print(rdd)

if __name__ == '__main__':
	if len(sys.argv) != 4:
		print('Usage: stream-processing [topic] [new topic] [kafka-broker')
		exit(1)

	topic, new_topic, kafka_broker = sys.argv[1:]

	# - setup connection to spark cluster
	sc = SparkContext("local[2]", "StockAveragePrice")
	# sc.setLogLevel('ERROR')
	ssc = StreamingContext(sc, 50)

	# - create a data stream from spark
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})

	# - for each RDD, do something
	# TODO
	directKafkaStream.foreachRDD(process)

	# - instantiate kafka producer
	kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

	# - setup proper shutdown hook
	# TODO
	atexit.register(shutdown_hook, kafka_producer)
