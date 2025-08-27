import asyncio

from src.dependencies.kafka_dependencies import consumer_notifications

if __name__ == "__main__":
    asyncio.run(consumer_notifications.consumer_run())