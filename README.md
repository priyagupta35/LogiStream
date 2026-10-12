<div align="center">

# LogiStream 🚚
### Event-Driven Logistics & Real-Time Delivery Backend Engine

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.7_KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-7.0_GEO-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-PyDeck_3D-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)

<p align="center">
  A high-throughput, event-driven logistics infrastructure simulating enterprise real-time dispatch systems.<br/>
  Engineered to showcase <b>distributed systems</b>, <b>asynchronous stream processing</b>, <b>spatial fleet indexing</b>, and <b>transactional idempotency</b>.
</p>

> *"I built an event-driven logistics backend that processes asynchronous delivery events, manages real-time driver state, connects independent services, and provides live tracking with monitoring and fault tolerance."*
>
> 

---


https://github.com/user-attachments/assets/62710820-aaaa-4f95-8bf8-4c8f8a38c8c8


</div>

## 📌 Architectural Overview

LogiStream decouples client write operations from spatial dispatch and telemetric delivery pipelines using an asynchronous event mesh:

```
                            ┌───────────────────────────────────┐
                            │    Client / Dispatch Operator     │
                            └─────────────────┬─────────────────┘
                                              │ POST /orders
                                              ▼
                            ┌───────────────────────────────────┐
                            │         FastAPI Engine            │
                            │   • Order Entity Persistence      │
                            │   • Audit Event Record            │
                            └─────────────────┬─────────────────┘
                                              │ OrderCreated
                                              ▼
                            ┌───────────────────────────────────┐
                            │       Apache Kafka Broker         │
                            │    Topic: orders.events           │
                            │    (Keyed by order_id)            │
                            └─────────────────┬─────────────────┘
                                              │
                                              ▼
                            ┌───────────────────────────────────┐
                            │     Dispatch Consumer Service     │
                            │   • Idempotency Guard Table       │
                            │   • Redis GEO Nearest Search      │
                            └─────────┬───────────────┬─────────┘
              Nearest Driver Found    │               │ No Driver / Timeout
                                      ▼               ▼
                ┌───────────────────────────┐   ┌───────────────────────────┐
                │ DriverAssigned Event      │   │ Kafka orders.retry / dlq  │
                │ Topic: delivery.events    │   └───────────────────────────┘
                └─────────────┬─────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│ Active Fleet Simulator    │   │ Live WebSocket Streaming  │
│ • GPS interpolation       │   │ • Real-time coordinate push│
│ • OrderPickedUp           │   │ • Streamlit 3D Dashboard  │
│ • InTransit / Delivered   │   └───────────────────────────┘
└───────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Technologies | Architectural Responsibility |
| :--- | :--- | :--- |
| **API & Gateway** | Python 3.11+, FastAPI, Pydantic v2 | High-performance asynchronous REST & WebSocket endpoints |
| **Event Backbone** | Apache Kafka (KRaft mode) | Partitioned event topics (`orders.events`, `delivery.events`, `orders.retry`, `orders.dlq`) |
| **Real-Time Spatial State**| Redis 7 (GEO commands) | Sub-millisecond spatial fleet indexing via `GEOADD` and `GEOSEARCH` |
| **Relational Persistence**| PostgreSQL 16, SQLAlchemy 2.0 | ACID transactions, index optimization, and `processed_events` deduplication |
| **Telemetry & Visuals** | Streamlit + PyDeck 3D | Real-time operations dashboard with interactive route arcs and live driver pins |
| **Observability** | Prometheus | HTTP request duration, consumer group offsets, and error rate tracking |
| **Containerization** | Docker, Docker Compose | 1-command reproducible cluster environment |
| **Cloud Deployment** | Render Cloud Blueprint (`render.yaml`)| Production-grade cloud deployment ready |

---

## 🛡️ Reliability & Fault Tolerance Guarantees

1. **Transactional Idempotency:**
   - Every consumer queries the `processed_events` table before applying business changes.
   - Inserting the idempotency token and updating the business entity share a **single atomic database transaction**.
   - The Kafka consumer commits the message offset **only after** the database transaction commits successfully, guaranteeing at-least-once with zero duplicate executions.
2. **Keyed Topic Partitioning:**
   - All Kafka messages are keyed by `order_id`, ensuring strict sequential delivery per order lifecycle.
3. **Dead-Letter Queue (DLQ) & Exponential Retries:**
   - Unassignable orders or transient network failures publish to `orders.retry` with exponential backoff before routing poison-pill events to `orders.dlq`.
4. **Redis Ephemeral Spatial Cache:**
   - Inactive driver heartbeats automatically expire via Redis TTL, preventing phantom dispatch to offline units.

---

## 📦 Lifecycle Order Flow

```
Create order ──> OrderCreated ──> Kafka ──> Dispatcher (Redis GEO) ──> DriverAssigned
       │
       └──> OrderPickedUp ──> InTransit ──> OutForDelivery ──> OrderDelivered
```

---

## 🔌 API Specification

### 1. Authentication (`/api/v1/auth`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/auth/register` | Register customer or driver account | Public |
| `POST` | `/api/v1/auth/login` | Authenticate and obtain JWT token | Public |
| `GET` | `/api/v1/auth/me` | Fetch active user profile | Authenticated |

### 2. Orders & Logistics (`/api/v1/orders`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/orders` | Create delivery order & emit `OrderCreated` to Kafka | Customer |
| `GET` | `/api/v1/orders` | List delivery orders with status filters | Authenticated |
| `GET` | `/api/v1/orders/{order_id}` | Retrieve order details & delivery timeline | Authenticated |
| `POST` | `/api/v1/orders/{order_id}/cancel` | Cancel order & emit `OrderCancelled` | Customer |

### 3. Fleet Telemetry (`/api/v1/drivers`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/drivers/location` | Ingest GPS coordinates & index in Redis GEO | Driver |
| `PUT` | `/api/v1/drivers/{id}/status` | Update driver availability (`AVAILABLE`, `BUSY`) | Driver |
| `GET` | `/api/v1/drivers/nearby` | Sub-millisecond nearby driver discovery via Redis GEO | Public / Internal |

### 4. Real-Time Tracking (`/api/v1/tracking`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/v1/tracking/{order_id}` | Current snapshot of assigned driver coordinates | Public |
| `WS` | `/api/v1/tracking/ws/{order_id}` | Bi-directional WebSocket stream of live driver motion | Public |

---

## 🚀 Quickstart & Single-Command Launch

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
- Git.

### 1. Clone the Repository
```bash
git clone https://github.com/priyagupta35/LogiStream.git
cd LogiStream
```

### 2. Start the Entire Cluster
```bash
docker compose up --build
```

This spins up:
- **PostgreSQL 16:** `localhost:5432`
- **Redis 7:** `localhost:6379`
- **Apache Kafka (KRaft):** `localhost:9092`
- **FastAPI Core Engine:** `http://localhost:8000/docs` *(Interactive Swagger UI)*
- **Dispatch Worker Service:** Internal container
- **Driver Telemetry Simulator:** Internal container
- **Streamlit 3D Operations Dashboard:** `http://localhost:8501`
- **Prometheus Scraper:** `http://localhost:9090`

---

## 🖥️ Live Streamlit 3D Dashboard

Access the real-time PyDeck operations map at **`http://localhost:8501`**:
- View live fleet vehicles moving in 3D across city coordinates.
- Dynamic arc layers connecting pickup hubs to delivery drop-off destinations.
- In-dashboard controls to trigger `OrderCreated` events and observe live Kafka dispatching.

---

## 🧪 Running the Test Suite

```bash
# Run unit & idempotency tests
python -m unittest discover tests/
```

---

## 👩‍💻 Author

**Priya Gupta**  
*B.Tech in Computer Science & Business Systems (CSBS)*  
*Netaji Subhash Engineering College, Kolkata*

- **GitHub:** [@priyagupta35](https://github.com/priyagupta35)
- **LinkedIn:** [Priya Gupta](https://www.linkedin.com/in/priya-gupta-b4a79528a/)
- **Email:** [priyagupta.csbs@gmail.com](mailto:priyagupta.csbs@gmail.com)
