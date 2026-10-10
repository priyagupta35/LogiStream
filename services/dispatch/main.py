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
        logger.warning(f"No available driver nearby for order {order_id}. Sending to retry topic.")
        await producer.send_and_wait(
            settings.KAFKA_ORDER_RETRY_TOPIC,
            key=order_id.encode("utf-8"),
            value=json.dumps(event_data).encode("utf-8")
        )

async def run_dispatch_consumer():
    logger.info("Starting Dispatch Service consumer loop...")
    await redis_client.connect()

    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_EVENTS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP_DISPATCH,
        enable_auto_commit=False,  # Manual offset commit after DB transaction
        auto_offset_reset="earliest"
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )

    while True:
        try:
            await consumer.start()
            await producer.start()
            logger.info("Connected to Kafka. Listening for OrderCreated events...")
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka broker ({e}). Retrying in 5 seconds...")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            try:
                event_data = json.loads(msg.value.decode("utf-8"))
                event_type = event_data.get("event_type")
                
                if event_type == "OrderCreated":
                    async with AsyncSessionLocal() as db:
                        await process_order_created(event_data, db, producer)
                
                # Commit offset ONLY after successful execution
                await consumer.commit()
            except Exception as e:
                logger.error(f"Error processing dispatch message: {e}")
                # In production, route poison pill messages to DLQ
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.disconnect()

if __name__ == "__main__":
    asyncio.run(run_dispatch_consumer())
