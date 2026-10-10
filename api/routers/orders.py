import json
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from api.database import get_db
from api.models.entities import Order, Delivery, DeliveryEvent, User, OrderStatus, UserRole
from api.schemas.schemas import OrderCreate, OrderResponse, OrderStatusUpdate
from api.security import get_current_user
from api.config import settings
from api.utils.kafka_producer import producer

router = APIRouter(prefix="/orders", tags=["Orders & Logistics"])

@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    order_in: OrderCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Creates a new delivery order and emits an asynchronous 'OrderCreated' event to Kafka.
    """
    new_order = Order(
        customer_id=current_user.id,
        pickup_address=order_in.pickup_address,
        pickup_lat=order_in.pickup_lat,
        pickup_lng=order_in.pickup_lng,
        delivery_address=order_in.delivery_address,
        delivery_lat=order_in.delivery_lat,
        delivery_lng=order_in.delivery_lng,
        package_weight_kg=order_in.package_weight_kg,
        package_notes=order_in.package_notes,
        status=OrderStatus.CREATED
    )

    db.add(new_order)
    await db.flush()

    # Record initial audit event
    init_event = DeliveryEvent(
        order_id=new_order.id,
        event_type="OrderCreated",
        payload=json.dumps({
            "order_id": new_order.id,
            "customer_id": current_user.id,
            "pickup": [new_order.pickup_lat, new_order.pickup_lng],
            "delivery": [new_order.delivery_lat, new_order.delivery_lng],
            "weight": new_order.package_weight_kg
        })
    )
    db.add(init_event)
    await db.commit()
    await db.refresh(new_order)

    # Emit Kafka event to orders.events (asynchronous backbone)
    try:
        await producer.emit_event(
            topic=settings.KAFKA_ORDER_EVENTS_TOPIC,
            event_type="OrderCreated",
            order_id=new_order.id,
            payload={
                "order_id": new_order.id,
                "customer_id": current_user.id,
                "pickup_lat": new_order.pickup_lat,
                "pickup_lng": new_order.pickup_lng,
                "pickup_address": new_order.pickup_address,
                "delivery_lat": new_order.delivery_lat,
                "delivery_lng": new_order.delivery_lng,
                "package_weight_kg": new_order.package_weight_kg
            }
        )
    except Exception as e:
        # DB is committed; background retry or outbox pattern ensures eventual consistency
        pass

    return new_order

@router.get("", response_model=List[OrderResponse])
async def list_orders(
    status_filter: Optional[OrderStatus] = Query(None, alias="status"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    query = select(Order).options(selectinload(Order.delivery))
    if current_user.role == UserRole.CUSTOMER:
        query = query.filter(Order.customer_id == current_user.id)
    
    if status_filter:
        query = query.filter(Order.status == status_filter)

    query = query.order_by(Order.created_at.desc())
    res = await db.execute(query)
    return res.scalars().all()

@router.get("/{order_id}", response_model=OrderResponse)
async def get_order_by_id(
    order_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    query = select(Order).options(selectinload(Order.delivery)).filter(Order.id == order_id)
    res = await db.execute(query)
    order = res.scalars().first()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    if current_user.role == UserRole.CUSTOMER and order.customer_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied to this order")

    return order

@router.post("/{order_id}/cancel", response_model=OrderResponse)
async def cancel_order(
    order_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    query = select(Order).filter(Order.id == order_id)
    res = await db.execute(query)
    order = res.scalars().first()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    if current_user.role == UserRole.CUSTOMER and order.customer_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    if order.status in [OrderStatus.DELIVERED, OrderStatus.CANCELLED]:
        raise HTTPException(status_code=400, detail=f"Cannot cancel order in state {order.status.value}")

    order.status = OrderStatus.CANCELLED
    
    event = DeliveryEvent(
        order_id=order.id,
        event_type="OrderCancelled",
        payload=json.dumps({"reason": "User requested cancellation"})
    )
    db.add(event)
    await db.commit()
    await db.refresh(order)

    # Emit event to Kafka
    try:
        await producer.emit_event(
            topic=settings.KAFKA_ORDER_EVENTS_TOPIC,
            event_type="OrderCancelled",
            order_id=order.id,
            payload={"order_id": order.id, "cancelled_by": current_user.id}
        )
    except Exception:
        pass

    return order
