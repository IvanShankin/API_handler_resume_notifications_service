import logging
from datetime import timedelta
from pathlib import Path

MIN_COMMIT_COUNT_KAFKA = 10

# данные для ключей Kafka (CONSUMER)
KEY_NEW_NOTIFICATIONS = 'new_request'

STORAGE_TIME_PROCESSED_MESSAGES = timedelta(days=3) # время хранения обработанного сообщения

LOG_DIR = Path("../logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "notification_service.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)