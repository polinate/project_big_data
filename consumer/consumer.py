import json
import time
import psycopg2
import logging
import kafka
from settings import DSL

logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='consumer.log',
    filemode='w'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

time.sleep(30)

def consume_loop(consumer, gp_con, count):
	cur = gp_con.cursor()

	for msg in consumer:
		count = count + 1
		data = json.loads(msg.value)
		cur.execute('INSERT INTO raw_data (sensor_id, longitude, latitude, controller_id, datetime, temperature) VALUES(%s, %s, %s, %s, %s, %s)', tuple(data.values()))
		print('Saved!')
		logger.info(str(count) + ' - count of data at the moment')


def run_consumer(gp_con, count):
	consumer = kafka.KafkaConsumer('generated-data', bootstrap_servers='kafka:9092')

	consume_loop(consumer, gp_con, count)



def main():
	while True:
		try:
			gp_con = psycopg2.connect(**DSL)
			break
		except:
			time.sleep(5)

	gp_con.autocommit = True
	count = 0

	query = """CREATE TABLE raw_data (
	id SERIAL,
	sensor_id BIGINT,
	longitude FLOAT,
	latitude FLOAT,
	controller_id BIGINT,
	datetime TIMESTAMP WITH TIME ZONE,
	temperature INTEGER
);"""

	cur = gp_con.cursor()
	try:
		cur.execute(query)
	except:
		pass

	run_consumer(gp_con, count)


if __name__ == '__main__':
	main()
