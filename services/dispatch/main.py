import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from api.config import settings
from api.models.entities import Order, Delivery, DeliveryEvent, ProcessedEvent, OrderStatus, Driver
from api.utils.redis_client import RedisClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [DISPATCH] %(levelname)s - %(message)s")
logger = logging.getLogger("logistream.dispatch")

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
redis_client = RedisClient()

async def process_order_created(event_data: dict, db: AsyncSession, producer: AIOKafkaProducer):
    event_id = event_data.get("event_id")
    order_id = event_data.get("order_id")
    payload = event_data.get("payload", {})
    
    # 1. Idempotency Check: verify if event was already processed
    existing = await db.execute(select(ProcessedEvent).filter(ProcessedEvent.event_id == event_id))
    if existing.scalars().first():
        logger.warning(f"Event {event_id} already processed. Skipping duplicate (Idempotency Guard).")
        return

    # 2. Fetch Order
    res = await db.execute(select(Order).filter(Order.id == order_id))
    order = res.scalars().first()
    if not order or order.status != OrderStatus.CREATED:
        logger.info(f"Order {order_id} not eligible for dispatch (Status={order.status if order else 'None'}).")
        return

    # 3. Find Nearest Driver via Redis GEO
    pickup_lat = payload.get("pickup_lat", order.pickup_lat)
    pickup_lng = payload.get("pickup_lng", order.pickup_lng)
    
    nearest_driver_id = await redis_client.find_nearest_driver(pickup_lat, pickup_lng, radius_km=50.0)

    if not nearest_driver_id:
        # Fallback: query any available driver from PostgreSQL if Redis set is being populated
        driver_res = await db.execute(select(Driver).filter(Driver.current_lat.isnot(None)).limit(1))
        fallback_driver = driver_res.scalars().first()
        if fallback_driver:
            nearest_driver_id = fallback_driver.id

    if nearest_driver_id:
        logger.info(f"Dispatched order {order_id} to nearest driver {nearest_driver_id}")
        
        # Mark driver busy in Redis
        await redis_client.set_driver_busy(nearest_driver_id)

        # Update order & create delivery record atomically
        order.status = OrderStatus.ASSIGNED
        delivery = Delivery(
            order_id=order.id,
            driver_id=nearest_driver_id
        )
        db.add(delivery)

        # Audit event
        audit_event = DeliveryEvent(
            order_id=order.id,
            event_type="DriverAssigned",
            payload=json.dumps({"driver_id": nearest_driver_id})
        )
        db.add(audit_event)

        # Record idempotency token in the same atomic transaction
        processed_token = ProcessedEvent(
            event_id=event_id,
            event_type="OrderCreated",
            order_id=order_id
        )
        db.add(processed_token)

        # Commit DB transaction
        await db.commit()

        # Emit DriverAssigned event to delivery.events topic
        assigned_envelope = {
            "event_id": f"assign-{event_id}",
            "event_type": "DriverAssigned",
            "order_id": order_id,
            "payload": {
                "order_id": order_id,
                "driver_id": nearest_driver_id,
                "pickup": [order.pickup_lat, order.pickup_lng],
                "delivery": [order.delivery_lat, order.delivery_lng]
            }
        }
        await producer.send_and_wait(
            settings.KAFKA_DELIVERY_EVENTS_TOPIC,
            key=order_id.encode("utf-8"),
            value=json.dumps(assigned_envelope).encode("utf-8")
        )
    else:
        raise NoDriverAvailable(f"No available driver for order {order_id}")


# ---------------------------------------------------------------------------
# Retry with exponential backoff, then dead-letter queue
# ---------------------------------------------------------------------------
import time as _time

MAX_RETRIES = 3
BASE_DELAY_SECONDS = 2  # retry delays: 2s, 4s, 8s


class NoDriverAvailable(Exception):
    pass


async def route_failure(event_data: dict, producer: AIOKafkaProducer, error: Exception):
    """Send a failed event to the retry topic, or to the DLQ once retries are exhausted."""
    order_id = str(event_data.get("order_id", "unknown"))
    attempt = int(event_data.get("retry_count", 0)) + 1
    event_data["retry_count"] = attempt
    event_data["last_error"] = str(error)

    if attempt > MAX_RETRIES:
        topic = settings.KAFKA_ORDER_DLQ_TOPIC
        logger.error(f"Order {order_id} failed {MAX_RETRIES} retries. Routing to DLQ: {error}")
    else:
        delay = BASE_DELAY_SECONDS ** attempt
        event_data["not_before"] = _time.time() + delay
        topic = settings.KAFKA_ORDER_RETRY_TOPIC
        logger.warning(f"Order {order_id} attempt {attempt}/{MAX_RETRIES} failed ({error}). Retrying in {delay}s.")

    await producer.send_and_wait(
        topic,
        key=order_id.encode("utf-8"),
        value=json.dumps(event_data).encode("utf-8")
    )


async def consume_loop(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer, name: str):
    async for msg in consumer:
        try:
            event_data = json.loads(msg.value.decode("utf-8"))
        except Exception as e:
            # Poison pill: unreadable payload goes straight to the DLQ
            logger.error(f"[{name}] Unreadable message, routing to DLQ: {e}")
            await producer.send_and_wait(settings.KAFKA_ORDER_DLQ_TOPIC, key=msg.key, value=msg.value)
            await consumer.commit()
            continue

        try:
            wait = float(event_data.get("not_before", 0)) - _time.time()
            if wait > 0:
                await asyncio.sleep(min(wait, 60))
            if event_data.get("event_type") == "OrderCreated":
                async with AsyncSessionLocal() as db:
                    await process_order_created(event_data, db, producer)
        except Exception as e:
            await route_failure(event_data, producer, e)

        # Commit the offset only after the event was handled or routed to retry/DLQ
        await consumer.commit()


async def run_dispatch_consumer():
    logger.info("Starting Dispatch Service consumer loops...")
    await redis_client.connect()

    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_EVENTS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP_DISPATCH,
        enable_auto_commit=False,  # Manual offset commit after DB transaction
        auto_offset_reset="earliest"
    )
    retry_consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_RETRY_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{settings.KAFKA_CONSUMER_GROUP_DISPATCH}-retry",
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )

    while True:
        try:
            await consumer.start()
            await retry_consumer.start()
            await producer.start()
            logger.info("Connected to Kafka. Listening for OrderCreated events...")
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka broker ({e}). Retrying in 5 seconds...")
            await asyncio.sleep(5)

    try:
        await asyncio.gather(
            consume_loop(consumer, producer, "orders"),
            consume_loop(retry_consumer, producer, "retry"),
        )
    finally:
        await consumer.stop()
        await retry_consumer.stop()
        await producer.stop()
        await redis_client.disconnect()


if __name__ == "__main__":
    asyncio.run(run_dispatch_consumer())