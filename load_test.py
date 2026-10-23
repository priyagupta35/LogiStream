import asyncio, time, httpx

BASE = "http://localhost:8000/api/v1"
N, CONC = 500, 20
payload = {"pickup_address": "Central Hub", "pickup_lat": 22.5726, "pickup_lng": 88.3639,
           "delivery_address": "Northside", "delivery_lat": 22.5950, "delivery_lng": 88.4000,
           "package_weight_kg": 2.5}

async def main():
    async with httpx.AsyncClient(timeout=30) as c:
        creds = {"email": "load@logistream.com", "password": "Password123"}
        await c.post(f"{BASE}/auth/register", json={**creds, "full_name": "Load Test", "role": "CUSTOMER"})
        tok = (await c.post(f"{BASE}/auth/login", json=creds)).json()["access_token"]
        h = {"Authorization": f"Bearer {tok}"}
        sem = asyncio.Semaphore(CONC)
        lat, bad = [], 0

        async def one():
            nonlocal bad
            async with sem:
                t = time.perf_counter()
                r = await c.post(f"{BASE}/orders", json=payload, headers=h)
                lat.append((time.perf_counter() - t) * 1000)
                if r.status_code != 201:
                    bad += 1

        t0 = time.perf_counter()
        await asyncio.gather(*[one() for _ in range(N)])
        dur = time.perf_counter() - t0
        lat.sort()
        q = lambda p: lat[int(len(lat) * p) - 1]
        print(f"requests={N} concurrency={CONC} errors={bad}")
        print(f"duration={dur:.1f}s throughput={N / dur:.1f} req/s")
        print(f"latency ms: p50={q(.5):.0f} p95={q(.95):.0f} p99={q(.99):.0f}")

asyncio.run(main())