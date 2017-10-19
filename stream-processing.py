# 1. read from kafka, kafka broker, kafka topic
# 2. write back to kafka, kafka broker, new kafka topic

import atexit
import sys
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = None
new_topic = None
kafka_broker = None
kafka_producer = None

def shutdown_hook(producer):
	"""
	Used to be called before the shutdown
	@param producer: instance of a kafka producer
	@return None
	"""
	try:
		logger.info('flush pending message to kafa')
		producer.flush(10)
		logger.info('finish flushing pending message')
	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
	finally:
		try:
			logger.info('Closing kafka connection')
			producer.close(10)
		except Exception as e:
			logger.warn('Failed to close kafka connection, caused by: %s', e.message)

def process(timeobj, rdd):
	# - calculate average
	num_of_records = rdd.count()
	if num_of_records == 0:
		logger.info('Found no record')
		return
	price_sum = rdd \
				.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePriceOnly'))) \
				.reduce(lambda a, b: a + b)
	average = price_sum / num_of_records
	logger.info('Received %d records from Kafka, average price is %f' % (num_of_records, average))
	current_time = time.time()
	# - write back to kafka
	# {timestamp, average}
	data = json.dumps({
		'timestamp': current_time,
		'average': average
	})
	try:
		kafka_producer.send(new_topic, value=data)
	except KafkaError as error:
		logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

if __name__ == '__main__':
	if len(sys.argv) != 4:
		print('Usage: stream-processing [topic] [new topic] [kafka-broker')
		exit(1)

	topic, new_topic, kafka_broker = sys.argv[1:]

	# - setup connection to spark cluster, create SparkContext and StreamingContext
	sc = SparkContext("local[2]", "StockAveragePrice")
	sc.setLogLevel('ERROR')
	ssc = StreamingContext(sc, 3)

	# - instantiate a kafka stream for processing
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})

	# - for each RDD, calculate its average price
	directKafkaStream.foreachRDD(process)

	# - instantiate a simple kafka producer
	kafka_producer = KafkaProducer(
		bootstrap_servers=kafka_broker
	)

	# - setup proper shutdown hook
	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination()
