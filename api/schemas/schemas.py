import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, EmailStr, Field
from api.models.entities import UserRole, DriverStatus, OrderStatus

# --- Event Envelope ---
class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    order_id: str
    occurred_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    payload: Dict[str, Any]
    version: str = "1.0"

# --- Auth Schemas ---
class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: str
    role: UserRole = UserRole.CUSTOMER

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: str
    email: str
    full_name: str
    role: UserRole
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str
    user: UserResponse

class TokenPayload(BaseModel):
    sub: Optional[str] = None
    role: Optional[str] = None

# --- Driver Schemas ---
class DriverLocationUpdate(BaseModel):
    driver_id: str
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)

class DriverStatusUpdate(BaseModel):
    status: DriverStatus

class DriverResponse(BaseModel):
    id: str
    user_id: str
    vehicle_type: str
    status: DriverStatus
    current_lat: Optional[float] = None
    current_lng: Optional[float] = None
    last_ping: Optional[datetime] = None

    class Config:
        from_attributes = True

# --- Order Schemas ---
class OrderCreate(BaseModel):
    pickup_address: str
    pickup_lat: float = Field(..., ge=-90.0, le=90.0)
    pickup_lng: float = Field(..., ge=-180.0, le=180.0)
    delivery_address: str
    delivery_lat: float = Field(..., ge=-90.0, le=90.0)
    delivery_lng: float = Field(..., ge=-180.0, le=180.0)
    package_weight_kg: float = 1.0
    package_notes: Optional[str] = None

class DeliveryResponse(BaseModel):
    id: str
    driver_id: str
    assigned_at: datetime
    picked_up_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class OrderResponse(BaseModel):
    id: str
    customer_id: str
    pickup_address: str
    pickup_lat: float
    pickup_lng: float
    delivery_address: str
    delivery_lat: float
    delivery_lng: float
    package_weight_kg: float
    package_notes: Optional[str] = None
    status: OrderStatus
    created_at: datetime
    updated_at: datetime
    delivery: Optional[DeliveryResponse] = None

    class Config:
        from_attributes = True

class OrderStatusUpdate(BaseModel):
    status: OrderStatus
    notes: Optional[str] = None
