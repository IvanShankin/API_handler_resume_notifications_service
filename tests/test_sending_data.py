import asyncio
import pytest

from srt.config import KEY_NEW_NOTIFICATIONS
from tests.conftest import producer, RESPONSE
from srt.dependencies.redis_dependencies import RedisWrapper
from tests.conftest import KAFKA_TOPIC_FOR_AI_HANDLER

@pytest.mark.asyncio
@pytest.mark.parametrize(
    'data',
    [
        ({"success": True,"response": RESPONSE,"message_error": None,"wait_seconds": None}), # сценарий когда всё хорошо
        ({"success": False,"response": RESPONSE,"message_error": "Пожалуйста подождите 20 секунд","wait_seconds": 20}), # сценарий когда есть rate limit
        ({"success": False,"response": RESPONSE,"message_error": "Internal Server Error","wait_seconds": None}), # сценарий когда произошла непредвиденная ошибка
    ]
)
async def test_kafka(data, clearing_kafka):
    async with RedisWrapper() as redis: # очистка redis
        await redis.flushdb()

    producer.sent_message(
        topic=KAFKA_TOPIC_FOR_AI_HANDLER,
        key=KEY_NEW_NOTIFICATIONS,
        value=data
    )

    await asyncio.sleep(11) # даём время на обработку сервера (в 10 секунд выставлен таймаут отправки )

    redis_data = await redis.get(f'processed_messages:{data['response']['processing_id']}')
    print(f'В тестах: processed_messages:{data['response']['processing_id']}')
    assert redis_data
