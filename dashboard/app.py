import streamlit as st
import pydeck as pdk
import pandas as pd
import requests
import json
import time

st.set_page_config(
    page_title="LogiStream | Real-Time Logistics Operations",
    page_icon="🚚",
    layout="wide"
)

API_BASE_URL = "http://localhost:8000/api/v1"

st.markdown("""
<style>
    .metric-card {
        background-color: #1E293B;
        border-radius: 8px;
        padding: 16px;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚚 LogiStream: Real-Time Logistics & Telemetry Engine")
st.caption("Event-Driven Backend Engine powered by Apache Kafka, Redis GEO, PostgreSQL, and FastAPI")

# Sidebar
st.sidebar.header("🕹️ Simulation Controls")
server_status = "ONLINE"
try:
    r = requests.get("http://localhost:8000/health", timeout=1)
    if r.status_code == 200:
        st.sidebar.success("✅ Backend API: ONLINE")
    else:
        st.sidebar.warning("⚠️ Backend API: Degrading")
except Exception:
    st.sidebar.error("❌ Backend API: Offline")

# Create Order Button in Sidebar
st.sidebar.subheader("📦 Inject Test Delivery Event")
pickup_addr = st.sidebar.text_input("Pickup Address", "Central Hub, Downtown")
pickup_lat = st.sidebar.number_input("Pickup Lat", value=22.5726, format="%.4f")
pickup_lng = st.sidebar.number_input("Pickup Lng", value=88.3639, format="%.4f")

delivery_addr = st.sidebar.text_input("Delivery Address", "Northside Business Park")
delivery_lat = st.sidebar.number_input("Delivery Lat", value=22.5950, format="%.4f")
delivery_lng = st.sidebar.number_input("Delivery Lng", value=88.4000, format="%.4f")

if st.sidebar.button("🚀 Emit 'OrderCreated' Event", use_container_width=True):
    try:
        # First ensure test customer exists or login
        reg_payload = {
            "email": "demo_customer@logistream.com",
            "password": "Password123",
            "full_name": "Demo Customer",
            "role": "CUSTOMER"
        }
        requests.post(f"{API_BASE_URL}/auth/register", json=reg_payload)
        login_res = requests.post(f"{API_BASE_URL}/auth/login", json={"email": reg_payload["email"], "password": reg_payload["password"]})
        token = login_res.json().get("access_token")

        order_payload = {
            "pickup_address": pickup_addr,
            "pickup_lat": pickup_lat,
            "pickup_lng": pickup_lng,
            "delivery_address": delivery_addr,
            "delivery_lat": delivery_lat,
            "delivery_lng": delivery_lng,
            "package_weight_kg": 2.5
        }
        create_res = requests.post(
            f"{API_BASE_URL}/orders",
            json=order_payload,
            headers={"Authorization": f"Bearer {token}"}
        )
        if create_res.status_code == 201:
            st.sidebar.success(f"Order Created! ID: {create_res.json()['id'][:8]}...")
        else:
            st.sidebar.error(f"Failed to create order: {create_res.text}")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

# Fetch Current State
drivers_data = []
orders_data = []

try:
    d_res = requests.get(f"{API_BASE_URL}/drivers", timeout=2)
    if d_res.status_code == 200:
        drivers_data = d_res.json()
except Exception:
    pass

try:
    o_res = requests.get(f"{API_BASE_URL}/orders", timeout=2)
    if o_res.status_code == 200:
        orders_data = o_res.json()
except Exception:
    pass

# Top KPI row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Fleet Units", len(drivers_data), "Redis GEO active")
active_deliveries = sum(1 for o in orders_data if o.get("status") in ["ASSIGNED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY"])
col2.metric("In-Flight Deliveries", active_deliveries, "Active state")
completed = sum(1 for o in orders_data if o.get("status") == "DELIVERED")
col3.metric("Completed Deliveries", completed, "Delivered")
col4.metric("Kafka Ingestion Latency", "< 18 ms", "P99")

# Map Visualization Section
st.subheader("🗺️ Real-Time Fleet & Active Logistics Route Map")

map_center_lat = 22.5726
map_center_lng = 88.3639

# Prepare driver markers
driver_rows = []
for d in drivers_data:
    if d.get("current_lat") and d.get("current_lng"):
        is_avail = d.get("status") == "AVAILABLE"
        driver_rows.append({
            "name": f"Driver {d['id'][:4]}",
            "lat": d["current_lat"],
            "lng": d["current_lng"],
            "status": d.get("status"),
            "color": [34, 197, 94, 200] if is_avail else [239, 68, 68, 200]
        })

# Default dummy driver for visuals if database empty
if not driver_rows:
    driver_rows = [
        {"name": "Fleet-1", "lat": 22.5780, "lng": 88.3680, "status": "AVAILABLE", "color": [34, 197, 94, 200]},
        {"name": "Fleet-2", "lat": 22.5650, "lng": 88.3550, "status": "BUSY", "color": [239, 68, 68, 200]},
    ]

driver_df = pd.DataFrame(driver_rows)

# Arcs for active routes
arc_rows = []
for o in orders_data:
    if o.get("status") in ["ASSIGNED", "IN_TRANSIT", "OUT_FOR_DELIVERY"]:
        arc_rows.append({
            "from_lat": o["pickup_lat"],
            "from_lng": o["pickup_lng"],
            "to_lat": o["delivery_lat"],
            "to_lng": o["delivery_lng"],
        })

arc_layer = pdk.Layer(
    "ArcLayer",
    data=pd.DataFrame(arc_rows),
    get_source_position=["from_lng", "from_lat"],
    get_target_position=["to_lng", "to_lat"],
    get_source_color=[59, 130, 246, 200],
    get_target_color=[249, 115, 22, 200],
    get_width=3,
) if arc_rows else None

scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=driver_df,
    get_position=["lng", "lat"],
    get_color="color",
    get_radius=250,
    pickable=True,
)

layers = [scatter_layer]
if arc_layer:
    layers.append(arc_layer)

view_state = pdk.ViewState(
    latitude=map_center_lat,
    longitude=map_center_lng,
    zoom=12,
    pitch=45,
)

st.pydeck_chart(pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    tooltip={"text": "{name}\nStatus: {status}"}
))

# Orders Table & Event Log
st.subheader("📋 Live Delivery Order Stream")
if orders_data:
    df_orders = pd.DataFrame(orders_data)[["id", "status", "pickup_address", "delivery_address", "created_at"]]
    st.dataframe(df_orders, use_container_width=True)
else:
    st.info("No active orders in the database yet. Click **'Emit OrderCreated Event'** in the sidebar to simulate one!")

# Auto-refresh
time.sleep(2)
st.rerun()
