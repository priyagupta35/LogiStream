import asyncio
import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from api.config import settings
from api.models.entities import Order, Delivery, DeliveryEvent, Driver, OrderStatus
from api.utils.redis_client import RedisClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SIMULATOR] %(levelname)s - %(message)s")
logger = logging.getLogger("logistream.simulator")

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
redis_client = RedisClient()

def interpolate(p1: float, p2: float, step: float = 0.25) -> float:
    """Simulates physical progress between coordinates."""
    diff = p2 - p1
    if abs(diff) < 0.001:
        return p2
    return p1 + (diff * step)

async def simulate_active_deliveries(producer: AIOKafkaProducer):
    async with AsyncSessionLocal() as db:
        # Fetch active deliveries
        query = select(Order).options(
            selectinload(Order.delivery).selectinload(Delivery.driver)
        ).filter(Order.status.in_([
            OrderStatus.ASSIGNED,
            OrderStatus.PICKED_UP,
            OrderStatus.IN_TRANSIT,
            OrderStatus.OUT_FOR_DELIVERY
        ]))
        
        res = await db.execute(query)
        active_orders = res.scalars().all()

        for order in active_orders:
            if not order.delivery or not order.delivery.driver:
                continue

            driver = order.delivery.driver

            # Default driver starting position if None
            if driver.current_lat is None or driver.current_lng is None:
                driver.current_lat = order.pickup_lat + 0.015
                driver.current_lng = order.pickup_lng + 0.015

            # Target destination depends on current phase
            if order.status == OrderStatus.ASSIGNED:
                # Heading to Pickup
                target_lat = order.pickup_lat
                target_lng = order.pickup_lng
                
                driver.current_lat = interpolate(driver.current_lat, target_lat, 0.35)
                driver.current_lng = interpolate(driver.current_lng, target_lng, 0.35)
                
                # Check if reached pickup
                if abs(driver.current_lat - target_lat) < 0.002 and abs(driver.current_lng - target_lng) < 0.002:
                    order.status = OrderStatus.PICKED_UP
                    order.delivery.picked_up_at = datetime.utcnow()
                    db.add(DeliveryEvent(order_id=order.id, event_type="OrderPickedUp"))
                    logger.info(f"Order {order.id}: Driver picked up package.")
                    await emit_event(producer, "OrderPickedUp", order.id, driver.id)

            elif order.status == OrderStatus.PICKED_UP:
                order.status = OrderStatus.IN_TRANSIT
                db.add(DeliveryEvent(order_id=order.id, event_type="InTransit"))
                logger.info(f"Order {order.id}: Package in transit.")
                await emit_event(producer, "InTransit", order.id, driver.id)

            elif order.status in [OrderStatus.IN_TRANSIT, OrderStatus.OUT_FOR_DELIVERY]:
                # Heading to Delivery Address
                target_lat = order.delivery_lat
                target_lng = order.delivery_lng

                driver.current_lat = interpolate(driver.current_lat, target_lat, 0.3)
                driver.current_lng = interpolate(driver.current_lng, target_lng, 0.3)

                # Nearing destination
                if order.status == OrderStatus.IN_TRANSIT and abs(driver.current_lat - target_lat) < 0.01:
                    order.status = OrderStatus.OUT_FOR_DELIVERY
                    db.add(DeliveryEvent(order_id=order.id, event_type="OutForDelivery"))
                    logger.info(f"Order {order.id}: Out for delivery.")
                    await emit_event(producer, "OutForDelivery", order.id, driver.id)

                # Reached destination -> DELIVERED!
                if abs(driver.current_lat - target_lat) < 0.002 and abs(driver.current_lng - target_lng) < 0.002:
                    order.status = OrderStatus.DELIVERED
                    order.delivery.delivered_at = datetime.utcnow()
                    db.add(DeliveryEvent(order_id=order.id, event_type="OrderDelivered"))
                    logger.info(f"Order {order.id}: Successfully DELIVERED!")
                    await emit_event(producer, "OrderDelivered", order.id, driver.id)

                    # Free up driver in Redis GEO
                    await redis_client.set_driver_available(driver.id, driver.current_lat, driver.current_lng)

            driver.last_ping = datetime.utcnow()
            await redis_client.update_driver_location(driver.id, driver.current_lat, driver.current_lng)

        await db.commit()

async def emit_event(producer: AIOKafkaProducer, event_type: str, order_id: str, driver_id: str):
    envelope = {
        "event_id": f"sim-{datetime.utcnow().timestamp()}",
        "event_type": event_type,
        "order_id": order_id,
        "occurred_at": datetime.utcnow().isoformat(),
        "payload": {"order_id": order_id, "driver_id": driver_id}
    }
    try:
        await producer.send_and_wait(
            settings.KAFKA_DELIVERY_EVENTS_TOPIC,
            key=order_id.encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8")
        )
    except Exception as e:
        logger.warning(f"Could not emit simulation event: {e}")

async def run_simulator():
    logger.info("Starting Driver Telemetry Simulator loop...")
    await redis_client.connect()

    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    while True:
        try:
            await producer.start()
            logger.info("Simulator connected to Kafka.")
            break
        except Exception:
            await asyncio.sleep(5)

    try:
        while True:
            await simulate_active_deliveries(producer)
            await asyncio.sleep(3)  # Step every 3 seconds
    finally:
        await producer.stop()
        await redis_client.disconnect()

if __name__ == "__main__":
    asyncio.run(run_simulator())
