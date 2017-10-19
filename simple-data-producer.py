# - identify kafka cluster and topic, in order to send events
# - For a given stock, grab its information for every 1 sec
# - symbol: AAPL, GOOG

# from googlefinance import getQuotes
from rtstock.stock import Stock
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import json
import time
import datetime
import logging
import schedule
import atexit
import random

# - default kafka setting
topic_name = 'stock-analyzer'
kafka_broker = '127.0.0.1:9002'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
# - TRACE DEBUG INFO WARNING ERROR
logger.setLevel(logging.DEBUG)

def fetch_price(producer, symbol):
	"""
	helper function to get stock data and send to kafka
	@param producer - instance of a kafka producer
	@param symbol - symbol of the stock, string type
	@return None
	"""
	logger.debug('Start to fetch stock price for %s', symbol)
	try:
		# stock = Stock(symbol)
		# latest_price = stock.get_latest_price();
		# latest_price.append({'symbol':symbol})
		# data = json.dumps(latest_price)
		# logger.debug('Get stock info %s', data)
		# producer.send(topic=topic_name, value=data, timestamp_ms=time.time())
		# logger.debug('Sent stock price for %s to kafka', symbol)
		
		price = random.randint(60, 180)
		timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
		payload = ('[{"StockSymbol":"AAPL","LastTradePriceOnly":%d,"LastTradeDateTime":"%s"}]' % (price, timestamp)).encode('utf-8')

		logger.debug('Get stock info %s', payload)
		producer.send(topic=topic_name, value=payload, timestamp_ms=time.time())
		logger.debug('Sent stock price for %s to kafka', symbol)

	except KafkaTimeoutError as timeout_error:
		logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error))
	except Exception:
		logger.warn('Failed to get stock price for %s', symbol)

def shutdown_hoot(producer):
	try:
		producer.flush(10)
		logger.info('Finished flushing pending messages')
	except KafkaError as KafkaError:
		logger.warn('Failed to flush pending messages to kafka')
	finally:
		try:
			producer.close()
			logger.info('Kafka connection closed')
		except Exception as e:
			logger.warn('Failed to close kafka connection')

if __name__ == '__main__':
	# - setup commandline arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help='the symbol of the stock')
	parser.add_argument('topic_name', help='the kafka topic')
	parser.add_argument('kafka_broker', help='the location of kafka broker')

	# - parse argument
	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	logger.debug('The broker that will respond to a Metadata API Request is: %s', kafka_broker)

	# - initiate a kafka producer
	producer = KafkaProducer(
		bootstrap_servers=kafka_broker
	)

	# - schedule to run every 10 sec
	schedule.every(1).seconds.do(fetch_price, producer, symbol)

	# - setup proper shutdown hook
	atexit.register(shutdown_hoot, producer)

	while True:
		schedule.run_pending()
		time.sleep(1)
