import time

import pytest
from testcontainers.community.kafka import KafkaContainer

import structlog

logger = structlog.get_logger()


@pytest.fixture(scope="session")
def kafka_broker():
    # 1. Явно указываем образ, совместимый с KRaft
    kafka = KafkaContainer("confluentinc/cp-kafka:8.4.0")
    
    # 2. Включаем KRaft и передаём обязательный CLUSTER_ID
    kafka = kafka.with_kraft().with_env("CLUSTER_ID", "test-cluster-id-12345")
    
    with kafka as container:
        server = container.get_bootstrap_server()
        # Даём брокеру время на полную инициализацию после появления лога
        time.sleep(5) 
        yield server
