from datetime import datetime, UTC

import httpx
import requests
import json
from srt.config import logger

async def sending_code_200(callback_url:str, data: dict):
    """Отправит данные на указанный callback_url"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
            callback_url,
            json=json.dumps(data).encode('utf-8'),
            timeout=20
            )
            response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Не удалось отослать сообщение по url: {callback_url}\nОшибка: {str(e)}")
        return False

async def sending_error_notification(callback_url:str,  error_type: str, error_details: dict,):
    """
    Отправляет структурированное сообщение об ошибке на callback_url

    :param callback_url: URL для обратного вызова
    :param error_type: Тип ошибки (например, "processing_error", "rate_limit")
    :param error_details: Детали ошибки {code, message, details}
    """

    error_payload = {
        "status": "error",
        "timestamp": datetime.now(UTC).isoformat(),
        "error": {
            "type": error_type,
            **error_details
        },
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                callback_url,
                json=error_payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Error-Notification": "true"
                }
            )
            response.raise_for_status()

    except httpx.HTTPStatusError as e:
        logger.error(f"Callback server returned error status: {e.response.status_code}")
    except httpx.RequestError as e:
        logger.error(f"Failed to deliver error notification: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    else:
        logger.info(f"Error notification delivered to {callback_url}")