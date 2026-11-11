"""
Producer to provide train locations data flow.
"""


import asyncio
import json
import os
from time import monotonic

import structlog
from confluent_kafka.aio import AIOProducer
from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

logger = structlog.get_logger()
load_dotenv("producer/.env")

DIGITRAFFIC_URL = "https://rata.digitraffic.fi/api/v2/graphql/graphql"
CURRENTLY_RUNNING_TRAINS = """
    query {
        currentlyRunningTrains {
            trainNumber
            departureDate
            version
            trainLocations(orderBy: {timestamp: DESCENDING}, take: 1) {
                speed
                timestamp
                location
            }
        }
    }
"""
KAFKA_TOPIC = "train-locations"


def create_producer() -> AIOProducer:
    """
    Creates and returns async Kafka producer instance.
    """

    return AIOProducer({
        "bootstrap.servers": "kafka:29092",
        "client.id": "train-locations-producer",
        "acks": "all",
        "enable.idempotence": True,
    })


async def create_graphql_session():
    """
    Creates and returns async GraphQL client and session.
    """

    transport = AIOHTTPTransport(
        url=DIGITRAFFIC_URL,
        headers={
            "Digitraffic-User": "RailAnomaly/1.0",
            "Accept-Encoding": "gzip",
        },
    )
    client = Client(transport=transport, execute_timeout=10)
    session = await client.connect_async(reconnecting=True)
    return client, session


async def send_all(producer: AIOProducer, msgs: list) -> None:
    """
    Sends message list through given producer to broker.
    """

    futures = [
        await producer.produce(
            topic=KAFKA_TOPIC,
            key=f'key-{msg.get("trainNumber")}'.encode(),
            value=json.dumps(msg).encode("utf-8"),
        )
        for msg in msgs
    ]
    await producer.flush()
    results = await asyncio.gather(*futures, return_exceptions=True)

    failed = sum(1 for r in results if isinstance(r, Exception))
    sent = len(results) - failed

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error("Delivery error", error=str(result), msg_index=i)

    return sent, failed


async def fetch_trains(session) -> list:
    """
    Fetches a batch of train locations data from Digitraffic api.
    """
    
    result = await session.execute(gql(CURRENTLY_RUNNING_TRAINS))
    return result.get("currentlyRunningTrains") or []


async def run_loop(producer: AIOProducer, session, interval: float) -> None:
    """
    Manages event loop.
    """

    next_start = monotonic()

    while True:
        now = monotonic()
        if next_start > now:
            await asyncio.sleep(next_start - now)

        next_start += interval

        started = monotonic()
        fetched, sent, failed = 0, 0, 0
        error = None

        try:
            batch = await fetch_trains(session)
            fetched = len(batch)
            sent, failed = await send_all(producer, batch)
        except Exception as e:  # noqa: BLE001
            error = str(e)

        logger.info(
            "Iteration finished",
            fetched=fetched,
            sent=sent,
            failed=failed,
            duration_ms=round((monotonic() - started) * 1000, 1),
            error=error,
        )

        if monotonic() > next_start:
            next_start = monotonic() + interval


async def main():
    """
    Entrypoint.
    """

    interval = float(os.getenv("POLLING_INTERVAL", "15"))
    producer = create_producer()
    client, session = await create_graphql_session()

    try:
        await run_loop(producer, session, interval)

    finally:
        await producer.close()
        await client.close_async()


if __name__ == "__main__":
    asyncio.run(main())
