import json
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

import structlog

logger = structlog.get_logger()


def test_at_least_once_retry(kafka_broker):
    topic = "test-topic"
    
    # 0. Обязательно создаем топик!
    admin = AdminClient({'bootstrap.servers': kafka_broker})
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    # Ждем, пока топик создастся (с небольшой погрешностью)
    for _, f in fs.items():
        try:
            f.result() 
        except Exception as e:
            logger.error(f"Ошибка создания топика: {e}")

    # 1. Продюсер пишет сообщение
    p = Producer({'bootstrap.servers': kafka_broker})
    p.produce(topic, json.dumps({"id": 1}).encode())
    p.flush() # Ждем отправки

    # 2. Консюмер читает, но НЕ коммитит (имитация падения)
    c1 = Consumer({
        'bootstrap.servers': kafka_broker,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    c1.subscribe([topic])
    
    # Даем время на ребалансировку и чтение
    msg1 = c1.poll(10.0) 
    assert msg1 is not None, "Первый консюмер не получил сообщение"
    assert not msg1.error()
    
    # НЕ вызываем c1.commit() - имитируем сбой
    c1.close()

    # 3. Новый консюмер в той же группе должен получить ЭТО ЖЕ сообщение снова
    c2 = Consumer({
        'bootstrap.servers': kafka_broker,
        'group.id': 'test-group', # та же группа
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    c2.subscribe([topic])
    
    msg2 = c2.poll(10.0) # Увеличили таймаут
    assert msg2 is not None, "Второй консюмер не получил сообщение повторно"
    assert msg2.value() == msg1.value() 
    c2.close()
