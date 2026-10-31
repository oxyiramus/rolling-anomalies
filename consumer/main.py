import json

from confluent_kafka import Consumer, KafkaError
import structlog

logger = structlog.get_logger()


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'work-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe(['train-locations'])

logger.info("Consumer started")

try:
    while True:
        logger.debug("Polling...")
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Ошибка: {msg.error()}")
                break

        try:
            data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Обработано: {data}")
            
            consumer.commit(message=msg)
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}")
finally:
    logger.info("Consumer closed")
    consumer.close()
