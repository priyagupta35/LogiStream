from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from api.database import get_db
from api.models.entities import Driver, DriverStatus, User, UserRole
from api.schemas.schemas import DriverLocationUpdate, DriverStatusUpdate, DriverResponse
from api.security import get_current_user, require_role
from api.utils.redis_client import redis_client

router = APIRouter(prefix="/drivers", tags=["Driver Fleet & Telemetry"])

@router.post("/location", response_model=DriverResponse)
async def update_driver_location(
    loc_in: DriverLocationUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Ingests driver telemetry, writes coordinates to PostgreSQL, and indexes spatially in Redis GEO.
    """
    res = await db.execute(select(Driver).filter(Driver.id == loc_in.driver_id))
    driver = res.scalars().first()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.current_lat = loc_in.latitude
    driver.current_lng = loc_in.longitude
    driver.last_ping = datetime.utcnow()

    await db.commit()
    await db.refresh(driver)

    # Real-time state update in Redis GEO for spatial discovery
    await redis_client.update_driver_location(
        driver_id=driver.id,
        lat=loc_in.latitude,
        lng=loc_in.longitude
    )

    return driver

@router.put("/{driver_id}/status", response_model=DriverResponse)
async def update_driver_status(
    driver_id: str,
    status_in: DriverStatusUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(Driver).filter(Driver.id == driver_id))
    driver = res.scalars().first()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.status = status_in.status
    await db.commit()
    await db.refresh(driver)

    if status_in.status == DriverStatus.AVAILABLE and driver.current_lat and driver.current_lng:
        await redis_client.set_driver_available(driver.id, driver.current_lat, driver.current_lng)
    elif status_in.status == DriverStatus.BUSY:
        await redis_client.set_driver_busy(driver.id)

    return driver

@router.get("/nearby")
async def find_nearby_drivers(
    lat: float = Query(..., ge=-90.0, le=90.0),
    lng: float = Query(..., ge=-180.0, le=180.0),
    radius_km: float = Query(25.0, ge=1.0, le=100.0)
):
    """
    Sub-millisecond discovery of nearest fleet units via Redis GEOSEARCH.
    """
    nearest_driver_id = await redis_client.find_nearest_driver(
        pickup_lat=lat,
        pickup_lng=lng,
        radius_km=radius_km
    )
    return {
        "searched_point": {"lat": lat, "lng": lng},
        "radius_km": radius_km,
        "nearest_driver_id": nearest_driver_id
    }

@router.get("", response_model=List[DriverResponse])
async def list_drivers(
    status_filter: Optional[DriverStatus] = Query(None, alias="status"),
    db: AsyncSession = Depends(get_db)
):
    query = select(Driver)
    if status_filter:
        query = query.filter(Driver.status == status_filter)
    res = await db.execute(query)
    return res.scalars().all()
