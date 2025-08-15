import asyncio
import threading
import time

import json
import os
import socket
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka import KafkaException

load_dotenv()  # Загружает переменные из .env
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_FOR_AI_HANDLER=os.getenv('KAFKA_TOPIC_FOR_AI_HANDLER')
KAFKA_TOPIC_FOR_NOTIFICATIONS=os.getenv('KAFKA_TOPIC_FOR_NOTIFICATIONS')

# этот импорт необходимо указывать именно тут для корректного импорта .tests.env
import pytest_asyncio
from confluent_kafka.cimpl import NewTopic

from srt.config import logger
from srt.dependencies.kafka_dependencies import admin_client
from srt.dependencies.kafka_dependencies import ConsumerKafkaNotifications

RESPONSE = {
    "callback_url": "https://test_url",
    'processing_id': 1,
    'user_id': 2,
    'resume_id': 3,
    'requirements_id': 4,
    'score': 50,
    'matches': ["Python", "SQL", "опыт с ИИ"],
    'recommendation': "Кандидат имеет частичные совпадения, но отсутствует профильный опыт. Рассматривать не рекомендуется.",
    'verdict': "Не подходит",
}

TOPIC_LIST = [
    KAFKA_TOPIC_FOR_AI_HANDLER,
    KAFKA_TOPIC_FOR_NOTIFICATIONS
]

class ProducerKafka:
    def __init__(self):
        self.conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'client.id': socket.gethostname()
            }
        self.producer = Producer(self.conf)

    def sent_message(self, topic: str, key: str, value: dict):
        try:
            self.producer.produce(topic=topic, key=key, value=json.dumps(value).encode('utf-8'), callback=self._acked)
            self.producer.flush()
            self.producer.poll(1)
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")

    def _acked(self, err, msg):
        logger.info(f"Kafka new message: err: {err}\nmsg: {msg.value().decode('utf-8')}")

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'test-group-' + str(os.getpid()),  # Уникальный group.id для каждого запуска тестов
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',  # Отключаем авто-коммит
    'isolation.level': 'read_committed'
}

producer = ProducerKafka()

@pytest_asyncio.fixture(scope='function', autouse=True)
async def start_kafka_consumer():
    """Фикстура для запуска нового consumer Kafka для каждого теста"""
    consumer_instance = ConsumerKafkaNotifications(KAFKA_TOPIC_FOR_AI_HANDLER)
    consumer_thread = threading.Thread(target=consumer_instance.consumer_run)
    consumer_thread.daemon = True
    consumer_thread.start()
    await asyncio.sleep(2)
    yield

    # Остановить consumer после завершения теста (необязательно, но желательно)
    await consumer_instance.set_running(False)

@pytest_asyncio.fixture(scope='session', autouse=True)
async def check_kafka_connection(_session_scoped_runner):
    try:
        admin_client.list_topics(timeout=10)
    except Exception:
        raise Exception("Не удалось установить соединение с Kafka!")

@pytest_asyncio.fixture(scope='function')
async def clearing_kafka():
    """Очищает топик у kafka с которым работаем, путём его пересоздания"""
    max_retries = 15

    for topic in TOPIC_LIST:
        admin_client.delete_topics([topic])

    # Ждём подтверждения удаления
    for _ in range(max_retries):
        meta = admin_client.list_topics(timeout=5)
        if all(t not in meta.topics for t in TOPIC_LIST):
            break
        time.sleep(1)
    else:
        logger.warning(f"Топик всё ещё существует после попыток удаления.")

    # Создаём топики
    for topic in TOPIC_LIST:
        admin_client.create_topics([NewTopic(topic=topic, num_partitions=1, replication_factor=1)])

    time.sleep(2)

    # Ждём инициализации partition и leader
    for _ in range(max_retries):
        meta_data = admin_client.list_topics(timeout=5)
        ready = True
        for topic in TOPIC_LIST:
            topic_meta = meta_data.topics.get(topic)
            if not topic_meta or topic_meta.error:
                ready = False
                break
            partitions = topic_meta.partitions
            if 0 not in partitions or partitions[0].leader == -1:
                ready = False
                break
        if ready:
            break
        time.sleep(1)
    else:
        raise RuntimeError("Partition или leader не инициализирован после создания топика.")

