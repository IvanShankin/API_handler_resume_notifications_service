# Микросервис обработки уведомлений

## Описание

Этот микросервис предоставляет функциональность для:
- Асинхронной обработки событий через Kafka
- Отправки уведомлений на callback-URL
- Кэширования обработанных сообщений с использованием Redis
- Интеграции с AI-обработчиком и другими сервисами системы

## Основные функции
- Получение сообщений из Kafka:
  - Обработка результатов работы AI-обработчика
  - Обработка ошибок (rate limit, server errors)
- Отправка уведомлений:
  - Успешные результаты обработки
  - Сообщения об ошибках
- Кеширование обработанных сообщений в Redis для избежания дублирования

## Технологический стек
- **Python 3.13.3**
- **Kafka** + **confluent-kafka** - асинхронная обработка сообщений
- **Redis** - кэширование
- **Pydantic** - валидация данных
- **Pytest** - тестирование
- 
## Запуск сервиса

### Требования
- Docker и Docker Compose
- Python 3.10+

### Установка
1. Клонируйте репозиторий
2. Выполните: `pip install -r requirements.txt`
3. Создайте файл `.env` и `.test.env`(для тестирования) на основе `.env.example`
4. Заполните необходимые переменные окружения

### Запуск
```bash
docker-compose up -d
```

## Конфигурация
Основные настройки сервиса находятся в:
- `srt/config.py` - основные параметры сервиса
- `.env` - переменные окружения

## Тестирование
Для запуска тестов:
```bash
pytest
```

Для запуска тестов через Docker:
```bash
docker-compose --env-file .test.env up -d 
```
```bash
docker-compose --env-file .test.env exec app pytest
```

Тесты покрывают:
- Unit-тесты основных функций
- Интеграционные тесты работы с Kafka
- Тесты работы с Redis
- Тесты отправки уведомлений

## Логирование
Логи сохраняются в файл `logs/notification_service.log` с ротацией по дням.

## Данные получаемые с Kafka:

- Читает по топику `AI_handler`
  - читаемые ключи: `new_request`
  
- Читает по топику `sending`
  - читаемые ключи: `new_sending`
  - структура сообщения:
    ```json
    {
      "success": bool,
      "response": {
        "callback_url": str,
        "processing_id": int,
        "user_id": int,
        "resume_id": int,
        "requirements_id": int,
        "score": int,
        "matches": list[str],
        "recommendation": str,
        "verdict": str
      },
      "message_error": str,
      "wait_seconds": int
    }
    ```

## Хранение в Redis:

### 1. Отметки о обработанных уведомлениях
**Ключ:** `processed_messages:{processing_id}`  
**Значение:** `'_'` (метка)  
**TTL:** `STORAGE_TIME_PROCESSED_MESSAGES` (3 дня)  
**Источник:** `kafka_dependencies.py` (`ConsumerKafkaNotifications`)

### Общий readme: https://github.com/IvanShankin/API_handler_resume_general_readme