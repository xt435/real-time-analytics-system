# - need to read from kafka, topic
# - need to write to cassandra, table

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import argparse
import atexit
import json
import logging
import time
import datetime

# - default kafka topic to read from
topic_name = 'stock-analyzer'

# - default kafka broker location
kafka_broker = '127.0.0.1:9092'

# - default cassandra nodes to connect
# contact_points = '192.168.99.101'
cassandra_broker = '127.0.0.1:9042'

# - default keyspace to use
keyspace = 'stock'

# - default table to use
data_table = ''

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')

# - TRACE DEBUG INFO WARNING ERROR
logger.setLevel(logging.DEBUG)


def persist_data(stock_data, cassandra_session):
	"""
	@param stock_data
	@param cassandra_session, a session created using cassandra-driver
	@return None
	"""
	logger.debug('Start to persist data to cassandra %s', stock_data)
	# stock_data is like this: [{"LastTradeTime": "4:00pm", "LastTradePriceOnly": "160.47"}]
	parsed = json.loads(stock_data)[0]
	symbol = parsed.get('StockSymbol')
	price = float(parsed.get('LastTradePriceOnly'))
	# tradetime = parsed.get('LastTradeDateTime')
	tradetime = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
	statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (data_table, symbol, tradetime, price)
	cassandra_session.execute(statement)
	logger.info('Persistend data to cassandra for symbol: %s, price: %f, tradetime: %s' % (symbol, price, tradetime))

	# persist stock data into cassandra
	# :param stock_data:
	#     the stock data looks like this:
	#     [{
	#         "Index": "NASDAQ",
	#         "LastTradeWithCurrency": "109.36",
	#         "LastTradeDateTime": "2016-08-19T16:00:02Z",
	#         "LastTradePrice": "109.36",
	#         "LastTradeTime": "4:00PM EDT",
	#         "LastTradeDateTimeLong": "Aug 19, 4:00PM EDT",
	#         "StockSymbol": "AAPL",
	#         "ID": "22144"
	#     }]
	# :param cassandra_session:
	# :return: None


	# try:

	# except Exception:
	#     logger.error('Failed to persist data to cassandra %s', stock_data)


def shutdown_hook(consumer, session):
	"""
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param session: instance of a cassandra session
    :return: None
    """
	try:
		logger.info('Closing Kafka Consumer')
		consumer.close()
		logger.info('Kafka Consumer closed')
		logger.info('Closing Cassandra Session')
		session.shutdown()
		logger.info('Cassandra Session closed')
	except KafkaError as kafka_error:
		logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
	finally:
		logger.info('Existing program')


if __name__ == '__main__':
	# - setup command line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help='the kafka topic to subscribe from')
	parser.add_argument('kafka_broker', help='the location of the kafka broker')
	parser.add_argument('keyspace', help='the keyspace to use in cassandra')
	parser.add_argument('data_table', help='the data table to use')   
	# parser.add_argument('contact_points', help='the contact points for cassandra')
	parser.add_argument('cassandra_broker', help='the location of cassandra cluster')

	# - parse arguments
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	keyspace = args.keyspace
	data_table = args.data_table
	# contact_points = args.contact_points
	cassandra_broker = args.cassandra_broker

	# - initiate a simple kafka consumer
	consumer = KafkaConsumer(
		topic_name,
		bootstrap_servers=kafka_broker
	)

	# - initiate a cassandra session
	cassandra_cluster = Cluster(
		# - assume cassandra_broker is '127.0.0.1,127.0.0.2'  ->   ['127.0.0.1', '127.0.0.2']
		contact_points=cassandra_broker.split(',')
	)
	session = cassandra_cluster.connect(keyspace)

	# - setup proper shutdown hook
	atexit.register(shutdown_hook, consumer, session)

	for msg in consumer:
		# - implement a function to save data to cassandra
		persist_data(msg.value, session)