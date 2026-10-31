import json
from time import sleep

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from confluent_kafka import Producer
import structlog

from producer.queries import CURRENTLY_RUNNING_TRAINS

DIGITRAFFIC_URL = "https://rata.digitraffic.fi/api/v2/graphql/graphql"


logger = structlog.get_logger()


conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'train-locations-producer'
}

producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Ошибка доставки: {err}')
    else:
        logger.info(f'Доставлено в {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')


def request():
    transport = AIOHTTPTransport(url=DIGITRAFFIC_URL)
    client = Client(transport=transport)

    query = gql(CURRENTLY_RUNNING_TRAINS)
    result = client.execute(query)

    for train in result.get("currentlyRunningTrains"):
        producer.poll(0)
        producer.produce(
            topic='train-locations',
            key=f'key-{train.get("trainNumber")}',
            value=json.dumps(train).encode('utf-8'),
            callback=delivery_report
        )

    producer.flush()


if __name__ == "__main__":
    while True:
        request()
        sleep(60)
