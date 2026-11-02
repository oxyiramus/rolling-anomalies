import os
import json
import asyncio
from time import monotonic

import structlog
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from confluent_kafka.aio import AIOProducer
from dotenv import load_dotenv

from producer.queries import CURRENTLY_RUNNING_TRAINS

logger = structlog.get_logger()
load_dotenv("producer/.env")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Ошибка доставки: {err}')


async def send_all(producer, msgs):
    logger.debug("Begin send_all")

    futures = []
    for msg in msgs:
        delivery_future = await producer.produce(
            topic='train-locations',
            key=f'key-{msg.get("trainNumber")}'.encode('utf-8'),
            value=json.dumps(msg).encode('utf-8')
        )
        futures.append(delivery_future)

    await producer.flush()
    await asyncio.gather(*futures)


async def request(session):
    logger.debug("Begin request")

    result = await session.execute(gql(CURRENTLY_RUNNING_TRAINS))
    return result.get("currentlyRunningTrains")


async def main():
    POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", 15))
    next_start = monotonic()

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'train-locations-producer',
        'acks': 'all',
        'enable.idempotence': True
    }

    producer = AIOProducer(conf)

    transport = AIOHTTPTransport(
        url="https://rata.digitraffic.fi/api/v2/graphql/graphql",
        headers={
            'Digitraffic-User': 'RailAnomaly/1.0',
            'Accept-Encoding': 'gzip'
        },
    )
    client = Client(transport=transport, execute_timeout=10)
    session = await client.connect_async(reconnecting=True)

    try:
        while True:

            logger.debug("Begin iteration")
            now = monotonic()
            if next_start > now:
                await asyncio.sleep(next_start - now)

            next_start += POLLING_INTERVAL

            try:
                batch = await request(session)
                await send_all(producer, batch)

            except Exception as e:
                logger.error(f'Ошибка запроса: {e}')

            if monotonic() > next_start:
                next_start = monotonic() + POLLING_INTERVAL

    finally:
        await producer.close()
        await client.close_async()


if __name__ == "__main__":
    asyncio.run(main())
