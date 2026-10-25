"""Simulateur temps reel de taxis et clients (style Uber / Bolt) sur Tunis."""
import argparse, json, math, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

ZONES = {
    "Centre-ville": (36.8008, 10.1800),
    "Lac": (36.8380, 10.2400),
    "La Marsa": (36.8780, 10.3250),
    "Ariana": (36.8625, 10.1956),
    "Bardo": (36.8092, 10.1340),
    "Ben Arous": (36.7530, 10.2280),
}
FIRST_NAMES = ["Ali", "Sami", "Mehdi", "Yassine", "Karim", "Omar", "Amine", "Nizar", "Hedi", "Walid"]
MAX_WAIT_S = 90


def now_iso():
    return datetime.now(timezone.utc).isoformat()

def random_point(zone=None):
    zone = zone or random.choice(list(ZONES))
    lat, lon = ZONES[zone]
    return lat + random.gauss(0, 0.008), lon + random.gauss(0, 0.008)

def zone_of(lat, lon):
    return min(ZONES, key=lambda z: (ZONES[z][0] - lat) ** 2 + (ZONES[z][1] - lon) ** 2)

def distance_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))

def move_towards(obj, target, speed_kmh, tick_s):
    step = speed_kmh * tick_s / 3600
    dist = distance_km(obj["lat"], obj["lon"], *target)
    if dist <= step:
        obj["lat"], obj["lon"] = target
        return True
    f = step / dist
    obj["lat"] += (target[0] - obj["lat"]) * f
    obj["lon"] += (target[1] - obj["lon"]) * f
    return False

def make_taxi(i):
    lat, lon = random_point()
    return {"taxi_id": f"TX-{i:03d}", "driver": random.choice(FIRST_NAMES),
            "lat": lat, "lon": lon, "status": "available",
            "speed_kmh": random.uniform(25, 45), "target": random_point(), "request_id": None}

def make_client():
    pickup, dropoff = random_point(), random_point()
    return {"request_id": str(uuid.uuid4())[:8], "client_id": f"CL-{random.randint(1, 9999):04d}",
            "pickup": pickup, "dropoff": dropoff, "lat": pickup[0], "lon": pickup[1],
            "status": "waiting", "taxi_id": None, "created": time.time()}

def taxi_event(t):
    return {"taxi_id": t["taxi_id"], "driver": t["driver"],
            "lat": round(t["lat"], 6), "lon": round(t["lon"], 6),
            "status": t["status"], "speed_kmh": round(t["speed_kmh"], 1),
            "zone": zone_of(t["lat"], t["lon"]), "request_id": t["request_id"],
            "timestamp": now_iso()}

def client_event(c):
    return {"request_id": c["request_id"], "client_id": c["client_id"],
            "lat": round(c["lat"], 6), "lon": round(c["lon"], 6),
            "pickup_lat": round(c["pickup"][0], 6), "pickup_lon": round(c["pickup"][1], 6),
            "dropoff_lat": round(c["dropoff"][0], 6), "dropoff_lon": round(c["dropoff"][1], 6),
            "status": c["status"], "taxi_id": c["taxi_id"], "zone": zone_of(*c["pickup"]),
            "trip_km": round(distance_km(*c["pickup"], *c["dropoff"]), 2),
            "timestamp": now_iso()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--taxis", type=int, default=40)
    ap.add_argument("--spawn", type=float, default=0.4)
    ap.add_argument("--tick", type=float, default=1.0)
    ap.add_argument("--speedup", type=float, default=10.0)
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode(),
                             key_serializer=lambda k: k.encode())
    taxis = [make_taxi(i + 1) for i in range(args.taxis)]
    clients = {}
    sim_tick = args.tick * args.speedup
    print(f"Simulation : {args.taxis} taxis -> Kafka {args.bootstrap} (Ctrl+C pour arreter)")

    while True:
        # 1. Nouveaux clients
        if random.random() < args.spawn:
            c = make_client()
            clients[c["request_id"]] = c

        # 2. Dispatch : taxi libre le plus proche
        free = [t for t in taxis if t["status"] == "available"]
        for c in clients.values():
            if c["status"] != "waiting" or not free:
                continue
            t = min(free, key=lambda x: distance_km(x["lat"], x["lon"], *c["pickup"]))
            free.remove(t)
            t.update(status="to_pickup", target=c["pickup"], request_id=c["request_id"])
            c.update(status="assigned", taxi_id=t["taxi_id"])

        # 3. Deplacement des taxis
        for t in taxis:
            arrived = move_towards(t, t["target"], t["speed_kmh"], sim_tick)
            c = clients.get(t["request_id"])
            if t["status"] == "available" and arrived:
                t["target"] = random_point()
            elif t["status"] == "to_pickup" and arrived and c:
                t.update(status="on_trip", target=c["dropoff"])
                c["status"] = "picked_up"
            elif t["status"] == "on_trip":
                if c:
                    c["lat"], c["lon"] = t["lat"], t["lon"]
                if arrived:
                    if c:
                        c["status"] = "completed"
                    t.update(status="available", request_id=None, target=random_point())

        # 4. Annulations
        for c in clients.values():
            if c["status"] == "waiting" and time.time() - c["created"] > MAX_WAIT_S:
                c["status"] = "cancelled"

        # 5. Envoi vers Kafka
        for t in taxis:
            producer.send("taxi-locations", key=t["taxi_id"], value=taxi_event(t))
        for rid, c in list(clients.items()):
            producer.send("client-requests", key=rid, value=client_event(c))
            if c["status"] in ("completed", "cancelled"):
                del clients[rid]
        producer.flush()

        counts = {s: sum(t["status"] == s for t in taxis) for s in ("available", "to_pickup", "on_trip")}
        waiting = sum(c["status"] == "waiting" for c in clients.values())
        print(f"{datetime.now():%H:%M:%S} taxis={counts} clients_en_attente={waiting}")
        time.sleep(args.tick)


if __name__ == "__main__":
    main()
