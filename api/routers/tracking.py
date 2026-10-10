import json
import asyncio
from typing import Dict, List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from api.database import get_db, AsyncSessionLocal
from api.models.entities import Order, Delivery, DeliveryEvent, Driver
from api.utils.redis_client import redis_client

router = APIRouter(prefix="/tracking", tags=["Live Tracking & Telemetry"])

class ConnectionManager:
    """Manages active WebSocket subscribers per order_id."""
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, order_id: str, websocket: WebSocket):
        await websocket.accept()
        if order_id not in self.active_connections:
            self.active_connections[order_id] = []
        self.active_connections[order_id].append(websocket)

    def disconnect(self, order_id: str, websocket: WebSocket):
        if order_id in self.active_connections:
            if websocket in self.active_connections[order_id]:
                self.active_connections[order_id].remove(websocket)
            if not self.active_connections[order_id]:
                del self.active_connections[order_id]

    async def broadcast_order_update(self, order_id: str, message: dict):
        if order_id in self.active_connections:
            payload = json.dumps(message)
            for connection in self.active_connections[order_id]:
                try:
                    await connection.send_text(payload)
                except Exception:
                    pass

manager = ConnectionManager()

@router.get("/{order_id}")
async def get_tracking_snapshot(order_id: str, db: AsyncSession = Depends(get_db)):
    """Provides current tracking telemetry, assigned driver location, and audit events."""
    query = select(Order).options(
        selectinload(Order.delivery),
        selectinload(Order.events)
    ).filter(Order.id == order_id)
    
    res = await db.execute(query)
    order = res.scalars().first()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    driver_loc = None
    if order.delivery and order.delivery.driver_id:
        driver_res = await db.execute(select(Driver).filter(Driver.id == order.delivery.driver_id))
        driver = driver_res.scalars().first()
        if driver:
            driver_loc = {
                "driver_id": driver.id,
                "lat": driver.current_lat,
                "lng": driver.current_lng,
                "last_ping": driver.last_ping.isoformat() if driver.last_ping else None
            }

    return {
        "order_id": order.id,
        "status": order.status.value,
        "pickup": {"lat": order.pickup_lat, "lng": order.pickup_lng, "address": order.pickup_address},
        "delivery": {"lat": order.delivery_lat, "lng": order.delivery_lng, "address": order.delivery_address},
        "assigned_driver": driver_loc,
        "timeline": [
            {"event_type": e.event_type, "occurred_at": e.occurred_at.isoformat()}
            for e in sorted(order.events, key=lambda x: x.occurred_at)
        ]
    }

@router.websocket("/ws/{order_id}")
async def websocket_tracking(websocket: WebSocket, order_id: str):
    """
    Real-time bi-directional WebSocket streaming live coordinates and status updates for an order.
    """
    await manager.connect(order_id, websocket)
    try:
        # Send initial snapshot
        async with AsyncSessionLocal() as db:
            query = select(Order).options(selectinload(Order.delivery)).filter(Order.id == order_id)
            res = await db.execute(query)
            order = res.scalars().first()
            if order:
                await websocket.send_text(json.dumps({
                    "event": "CONNECTED",
                    "order_id": order.id,
                    "status": order.status.value
                }))

        # Keep socket open and stream periodic driver state if assigned
        while True:
            await asyncio.sleep(2)
            async with AsyncSessionLocal() as db:
                query = select(Order).options(selectinload(Order.delivery)).filter(Order.id == order_id)
                res = await db.execute(query)
                order = res.scalars().first()
                if order and order.delivery and order.delivery.driver_id:
                    d_res = await db.execute(select(Driver).filter(Driver.id == order.delivery.driver_id))
                    driver = d_res.scalars().first()
                    if driver:
                        await websocket.send_text(json.dumps({
                            "event": "TELEMETRY",
                            "status": order.status.value,
                            "driver_id": driver.id,
                            "lat": driver.current_lat,
                            "lng": driver.current_lng
                        }))
    except WebSocketDisconnect:
        manager.disconnect(order_id, websocket)
    except Exception:
        manager.disconnect(order_id, websocket)
