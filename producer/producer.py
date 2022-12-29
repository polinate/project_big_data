import json
import logging
import random
import requests
import threading
import time
from dataclasses import dataclass
from datetime import datetime

import kafka

logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='producer.log',
    filemode='w'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

time.sleep(30)

p = kafka.KafkaProducer(bootstrap_servers='kafka:9092')
print('Kafka Producer has been initiated...')
logger.info('Kafka Producer has been initiated...')


def get_start_id() -> int:
    return random.randint(10000000, 99999999)


def get_russia_coordinates() -> tuple:
    response = requests.get('https://api.3geonames.org/?randomland=RU&json=1')
    json_response = response.json()
    return json_response['nearest']['latt'], json_response['nearest']['longt']


@dataclass
class Sensor:
    id: int
    latitude: str = ''
    longitude: str = ''

    def generate_data(self):
        date = datetime.utcnow()
        temperature = random.randint(-20, 20)
        controller_id = hash(self.id)
        return {
            'sensor_id': self.id,
            'longitude': self.longitude,
            'latitude': self.latitude,
            'controller_id': controller_id,
            'datetime': str(date),
            'temperature': temperature,
        }


def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


def generate(sensor: Sensor):
    while True:
        data = sensor.generate_data()
        p.send('generated-data', bytes(json.dumps(data), 'utf-8'))
        p.flush()
        print('sent!')
        logger.info(json.dumps(data))
        time.sleep(5)


def main():
    start_id = get_start_id()
    for i in range(10):
        coordinates = get_russia_coordinates()
        sensor = Sensor(
            id=start_id+i,
            latitude=coordinates[0],
            longitude=coordinates[1],
        )
        thread = threading.Thread(target=generate, args=(sensor, ))
        thread.start()


if __name__ == '__main__':
    main()
