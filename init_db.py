import asyncio
import logging
from api.database import engine, Base, AsyncSessionLocal
from api.models.entities import User, Driver, UserRole, DriverStatus
from api.security import get_password_hash
from api.utils.redis_client import redis_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("logistream.init")

INITIAL_DRIVERS = [
    {"name": "Fleet Driver Rahul", "email": "rahul@logistream.com", "lat": 22.5726, "lng": 88.3639, "vehicle": "Electric Van"},
    {"name": "Fleet Driver Amit", "email": "amit@logistream.com", "lat": 22.5850, "lng": 88.3750, "vehicle": "Motorcycle"},
    {"name": "Fleet Driver Sneha", "email": "sneha@logistream.com", "lat": 22.5600, "lng": 88.3500, "vehicle": "Cargo Bike"},
    {"name": "Fleet Driver Vikram", "email": "vikram@logistream.com", "lat": 22.5900, "lng": 88.3900, "vehicle": "Van"},
]

async def seed():
    logger.info("Connecting to Database and ensuring tables exist...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await redis_client.connect()

    async with AsyncSessionLocal() as db:
        # Create default Admin
        admin_email = "admin@logistream.com"
        admin = User(
            email=admin_email,
            full_name="Operations Admin",
            hashed_password=get_password_hash("AdminSecret2026!"),
            role=UserRole.ADMIN
        )
        db.add(admin)

        # Create demo Customer
        cust_email = "customer@logistream.com"
        customer = User(
            email=cust_email,
            full_name="Enterprise Customer",
            hashed_password=get_password_hash("CustomerPass123!"),
            role=UserRole.CUSTOMER
        )
        db.add(customer)

        await db.flush()

        # Create Drivers
        for d in INITIAL_DRIVERS:
            driver_user = User(
                email=d["email"],
                full_name=d["name"],
                hashed_password=get_password_hash("DriverPass123!"),
                role=UserRole.DRIVER
            )
            db.add(driver_user)
            await db.flush()

            driver_profile = Driver(
                user_id=driver_user.id,
                vehicle_type=d["vehicle"],
                status=DriverStatus.AVAILABLE,
                current_lat=d["lat"],
                current_lng=d["lng"]
            )
            db.add(driver_profile)
            await db.flush()

            # Index in Redis GEO
            await redis_client.update_driver_location(driver_profile.id, d["lat"], d["lng"])
            logger.info(f"Seeded Driver: {d['name']} at ({d['lat']}, {d['lng']})")

        await db.commit()

    await redis_client.disconnect()
    logger.info("LogiStream database initialized & initial fleet seeded successfully!")

if __name__ == "__main__":
    asyncio.run(seed())
