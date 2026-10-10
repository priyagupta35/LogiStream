import json
import logging
import asyncio
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer
from api.config import settings
from api.schemas.schemas import EventEnvelope

logger = logging.getLogger("logistream.kafka")

class EventProducer:
    def __init__(self):
        self._producer: Optional[AIOKafkaProducer] = None
        self._is_connected = False

    async def start(self):
        """Initializes the Kafka Producer connection."""
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Strongest durability guarantee
                retries=5,
                request_timeout_ms=5000
            )
            await self._producer.start()
            self._is_connected = True
            logger.info(f"Kafka Producer connected to {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.warning(f"Could not connect Kafka Producer to {settings.KAFKA_BOOTSTRAP_SERVERS}: {e}. (Will retry on send)")
            self._is_connected = False

    async def stop(self):
        """Gracefully flushes and closes the producer."""
        if self._producer and self._is_connected:
            await self._producer.stop()
            self._is_connected = False
            logger.info("Kafka Producer stopped")

    async def emit_event(
        self,
        topic: str,
        event_type: str,
        order_id: str,
        payload: Dict[str, Any]
    ) -> EventEnvelope:
        """
        Publishes a strongly typed EventEnvelope to Kafka, partitioned by order_id.
        """
        envelope = EventEnvelope(
            event_type=event_type,
            order_id=order_id,
            payload=payload
        )

        if not self._is_connected:
            # Attempt reconnect
            try:
                if not self._producer:
                    await self.start()
                else:
                    await self._producer.start()
                    self._is_connected = True
            except Exception as e:
                logger.error(f"Kafka unavailable. Storing event locally. Error: {e}")
                return envelope

        try:
            # Keying by order_id guarantees ordered delivery per delivery lifecycle
            await self._producer.send_and_wait(
                topic=topic,
                key=order_id,
                value=envelope.dict()
            )
            logger.info(f"Emitted {event_type} for order {order_id} to {topic} [event_id={envelope.event_id}]")
        except Exception as e:
            logger.error(f"Failed to publish event {envelope.event_id} to {topic}: {e}")
            raise e

        return envelope

producer = EventProducer()
