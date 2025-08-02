import asyncio
import json
import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

from srt.config import logger, MIN_COMMIT_COUNT_KAFKA, KEY_NEW_NOTIFICATIONS,STORAGE_TIME_PROCESSED_MESSAGES
from srt.dependencies.redis_dependencies import RedisWrapper
from srt.sending_data import sending_code_200, sending_error_notification

load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_FOR_AI_HANDLER = os.getenv('KAFKA_TOPIC_FOR_AI_HANDLER')
KAFKA_TOPIC_FOR_UPLOADING_DATA = os.getenv('KAFKA_TOPIC_FOR_UPLOADING_DATA')
KAFKA_TOPIC_FOR_NOTIFICATIONS = os.getenv('KAFKA_TOPIC_FOR_NOTIFICATIONS')

admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})\

producer = None # ниже будет переопределён
consumer_auth = None # ниже будет переопределён


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Создаёт топик в Kafka.

    :param topic_name: Название топика
    :param num_partitions: Количество партиций
    :param replication_factor: Фактор репликации
    """
    # Создание объекта топика
    new_topic = NewTopic(
        topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    # Запрос на создание топика
    futures = admin_client.create_topics([new_topic])

    # Ожидание результата
    for topic, future in futures.items():
        try:
            future.result()  # Блокирует выполнение, пока топик не создан
            logger.info(f"Топик '{topic}' успешно создан!")
        except Exception as e:
            logger.error(f"Ошибка при создании топика '{topic}': {e}")


def check_exists_topic(topic_names: list):
    """Проверяет, существует ли топик, если нет, то создаст его"""
    for topic in topic_names:
        cluster_metadata = admin_client.list_topics()
        if not topic in cluster_metadata.topics: # если topic не существует
            create_topic(
                topic_name=topic,
                num_partitions=1,
                replication_factor=1
            )

class ConsumerKafka:
    def __init__(self, topic: str):
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'foo',
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': False,  # отключаем auto-commit
            'on_commit': self.commit_completed
        } # тут указали в какую функцию попадёт при сохранении
        self.consumer = Consumer(self.conf)
        self.running = True
        self.topic = topic
        check_exists_topic([self.topic])


    # в эту функцию попадём при вызове метода consumer.commit
    def commit_completed(self, err, partitions):
        if err:
            logger.error(str(err))
        else:
            logger.info("сохранили партию kafka")

    async def set_running(self, running: bool):
        self.running = running

    async def subscribe_topics(self, topics: list):
        self.consumer.subscribe([topics])
        self.consumer.poll(0)  # форсирует загрузку метаданных

    # ЭТУ ФУНКЦИЮ ПЕРЕОБРЕДЕЛЯЕМ В НАСТЛЕДУЕМОМ КЛАССЕ,
    # ОНА БУДЕТ ВЫПОЛНЯТЬ ДЕЙСТВИЯ ПРИ ПОЛУЧЕНИИ СООБЩЕНИЯ
    async def worker_topic(self, data:dict, key: str):
        pass

    async def error_handler(self, e):
        logger.error(f'Произошла ошибка при обработки сообщения c kafka: {str(e)}')

    async def _run_consumer(self):
        self.consumer.subscribe([self.topic])
        msg_count = 0

        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                   (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                key = msg.key().decode('utf-8')

                try:
                    await self.worker_topic(data, key)
                except Exception as e:
                    await self.error_handler(e)

                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT_KAFKA == 0:
                    self.consumer.commit(asynchronous=True)

    def consumer_run(self):
        """Создаёт цикл и запускает асинхронный код для чтения сообщений"""
        try:
            loop = asyncio.new_event_loop() # Создаём новый цикл
            asyncio.set_event_loop(loop)  # Назначаем его для текущего потока
            loop.run_until_complete(self._run_consumer()) # Запускаем асинхронный код
        finally:
            self.consumer.close()

# consumer для получения данные о новых запросах
class ConsumerKafkaNotifications(ConsumerKafka):
    def __init__(self, topic: str):
        super().__init__(topic)

    async def worker_topic(self, data: dict, key: str):
        if key == KEY_NEW_NOTIFICATIONS: # при поступлении нового запроса
            async with RedisWrapper() as redis:
                try:
                    redis_result = await redis.get(f'processed_messages:{data['response']['processing_id']}')
                except Exception as e:
                    logger.error(f'Redis error: {e}')

                if redis_result is not None:
                    logger.info(f"Сообщение от kafka на обработку с processing_id = {data['response']['processing_id']} будет пропущено,"
                                f"т.к. Было ранее обработано")
                    # в этом случае ничего возвращать не надо, ибо уже обработали этот запрос
                    return

                callback_url = data['response']['callback_url']
                del data['response']['callback_url']
                if data['success'] is True: # если удачно обработали
                    await sending_code_200(callback_url, data)
                else:
                    error_type = 'processing_error'
                    error_details = {
                        "code": 500,
                        "message": data['message_error'],
                        "details": {
                            "reason": "Unknown error",
                            "service": "ai_processor"
                        }
                    }

                    if data['wait_seconds']: # если есть задержка, значит, ошибка только в rate limit
                        error_details['code'] = 429
                        error_details['details']["reason"] = 'Too many request'
                        error_type = 'Too many request'

                    await sending_error_notification(
                        callback_url=callback_url,
                        error_type=error_type,
                        error_details=error_details
                    )
                # сохраняем в redis (значение может быть любое)
                await redis.setex(f'processed_messages:{data['response']['processing_id']}', STORAGE_TIME_PROCESSED_MESSAGES, '_')



consumer_notifications = ConsumerKafkaNotifications(KAFKA_TOPIC_FOR_AI_HANDLER)