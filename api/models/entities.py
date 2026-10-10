import enum
import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, Float, DateTime, Enum, ForeignKey, Text, Boolean, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB
from api.database import Base

class UserRole(str, enum.Enum):
    CUSTOMER = "CUSTOMER"
    DRIVER = "DRIVER"
    ADMIN = "ADMIN"

class DriverStatus(str, enum.Enum):
    AVAILABLE = "AVAILABLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"

class OrderStatus(str, enum.Enum):
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"

class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=False)
    role = Column(Enum(UserRole), default=UserRole.CUSTOMER, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    driver_profile = relationship("Driver", back_populates="user", uselist=False)
    orders = relationship("Order", back_populates="customer")

class Driver(Base):
    __tablename__ = "drivers"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), unique=True, nullable=False)
    vehicle_type = Column(String(50), default="Motorcycle")
    status = Column(Enum(DriverStatus), default=DriverStatus.AVAILABLE, index=True)
    current_lat = Column(Float, nullable=True)
    current_lng = Column(Float, nullable=True)
    last_ping = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="driver_profile")
    deliveries = relationship("Delivery", back_populates="driver")

class Order(Base):
    __tablename__ = "orders"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    
    # Pickup details
    pickup_address = Column(String(500), nullable=False)
    pickup_lat = Column(Float, nullable=False)
    pickup_lng = Column(Float, nullable=False)

    # Destination details
    delivery_address = Column(String(500), nullable=False)
    delivery_lat = Column(Float, nullable=False)
    delivery_lng = Column(Float, nullable=False)

    package_weight_kg = Column(Float, default=1.0)
    package_notes = Column(Text, nullable=True)
    status = Column(Enum(OrderStatus), default=OrderStatus.CREATED, index=True, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    customer = relationship("User", back_populates="orders")
    delivery = relationship("Delivery", back_populates="order", uselist=False)
    events = relationship("DeliveryEvent", back_populates="order", cascade="all, delete-orphan")

class Delivery(Base):
    __tablename__ = "deliveries"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id = Column(String(36), ForeignKey("orders.id"), unique=True, nullable=False)
    driver_id = Column(String(36), ForeignKey("drivers.id"), nullable=False)
    
    assigned_at = Column(DateTime, default=datetime.utcnow)
    picked_up_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)

    order = relationship("Order", back_populates="delivery")
    driver = relationship("Driver", back_populates="deliveries")

class DeliveryEvent(Base):
    __tablename__ = "delivery_events"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id = Column(String(36), ForeignKey("orders.id"), nullable=False, index=True)
    event_type = Column(String(100), nullable=False, index=True)
    payload = Column(Text, nullable=True)  # JSON serialized string
    occurred_at = Column(DateTime, default=datetime.utcnow)

    order = relationship("Order", back_populates="events")

class ProcessedEvent(Base):
    """
    Guarantees Idempotency for consumers.
    Inserting into this table inside the same DB transaction prevents duplicate event processing.
    """
    __tablename__ = "processed_events"

    event_id = Column(String(100), primary_key=True)
    event_type = Column(String(100), nullable=False)
    order_id = Column(String(100), nullable=False, index=True)
    processed_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_processed_event_id"),
    )
