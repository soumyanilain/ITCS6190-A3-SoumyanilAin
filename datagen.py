"""Generate the input data for Assignment 3 (plain Python, no extra packages).

    python3 datagen.py <seed> [<output directory>]

Writes trips.csv and zones.csv to the output directory, by default shared-folder/input next
to this file. Use your student ID as the seed: the data is then yours, and it can be
regenerated exactly from that number.

The trips file is deliberately not clean. A small share of the rows have a dropoff before
the pickup, and a small share have a fare that is empty or not a number. Part 1 of the
assignment is about finding and removing them.
"""
import csv
import os
import random
import sys
from datetime import datetime, timedelta

if len(sys.argv) not in (2, 3) or not sys.argv[1].isdigit():
    print(__doc__)
    sys.exit(2)

seed = int(sys.argv[1])
random.seed(seed)

NUM_TRIPS, NUM_DRIVERS = 100_000, 250
START, DAYS = datetime(2026, 9, 1), 30
PAYMENT = ["card", "card", "card", "app", "app", "cash"]

ZONES = [  # id, name, borough
    (1, "Uptown Center", "Central"), (2, "First Ward", "Central"), (3, "Fourth Ward", "Central"),
    (4, "South End", "Central"), (5, "Dilworth", "Central"), (6, "Plaza Midwood", "East"),
    (7, "NoDa", "North"), (8, "University City", "North"), (9, "Ballantyne", "South"),
    (10, "SouthPark", "South"), (11, "Myers Park", "South"), (12, "Elizabeth", "East"),
    (13, "Eastland", "East"), (14, "Steele Creek", "West"), (15, "Airport", "West"),
    (16, "Wesley Heights", "West"), (17, "Cotswold", "East"), (18, "Matthews", "East"),
    (19, "Huntersville", "North"), (20, "Derita", "North"), (21, "Pineville", "South"),
    (22, "Montford", "South"), (23, "Camp North End", "Central"), (24, "Freedom Park", "South"),
    (25, "Mint Hill", "East"),
]
# Popularity weights: a few zones dominate, as in real cities.
WEIGHTS = [12, 9, 8, 10, 6, 5, 6, 7, 5, 6, 4, 3, 2, 3, 9, 3, 2, 3, 3, 2, 2, 2, 4, 3, 1]
HOUR_WEIGHTS = [2, 1, 1, 1, 1, 2, 4, 7, 9, 7, 5, 6, 7, 6, 6, 7, 8, 10, 9, 7, 6, 5, 4, 3]

out_dir = (sys.argv[2] if len(sys.argv) == 3 else
           os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared-folder", "input"))
os.makedirs(out_dir, exist_ok=True)

with open(os.path.join(out_dir, "zones.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["zone_id", "zone_name", "borough"])
    w.writerows(ZONES)

zone_ids = [z[0] for z in ZONES]
drivers = [f"D{i:03d}" for i in range(1, NUM_DRIVERS + 1)]
# Each driver has a home zone they pick up from more often than the others.
home = {d: random.choices(zone_ids, WEIGHTS)[0] for d in drivers}

with open(os.path.join(out_dir, "trips.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["trip_id", "driver_id", "pickup_zone", "dropoff_zone", "pickup_ts",
                "dropoff_ts", "distance_km", "fare", "payment_type"])
    for i in range(1, NUM_TRIPS + 1):
        d = random.choice(drivers)
        pz = home[d] if random.random() < 0.4 else random.choices(zone_ids, WEIGHTS)[0]
        dz = random.choices(zone_ids, WEIGHTS)[0]
        day = random.randrange(DAYS)
        hour = random.choices(range(24), HOUR_WEIGHTS)[0]
        pickup = START + timedelta(days=day, hours=hour, minutes=random.randrange(60),
                                   seconds=random.randrange(60))
        distance = round(random.lognormvariate(1.5, 0.6), 2)          # km, median about 4.5
        minutes = max(3, int(distance * random.uniform(2.0, 4.0)) + random.randrange(0, 8))
        dropoff = pickup + timedelta(minutes=minutes, seconds=random.randrange(60))
        fare = f"{2.50 + 1.80 * distance + 0.40 * minutes:.2f}"
        r = random.random()
        if r < 0.015:                                                    # dropoff before pickup
            pickup, dropoff = dropoff, pickup
        elif r < 0.025:                                                  # fare missing
            fare = ""
        elif r < 0.030:                                                  # fare not a number
            fare = random.choice(["N/A", "unknown", "12,40"])
        w.writerow([f"T{i:06d}", d, pz, dz, pickup.strftime("%Y-%m-%d %H:%M:%S"),
                    dropoff.strftime("%Y-%m-%d %H:%M:%S"), distance, fare,
                    random.choice(PAYMENT)])

print(f"seed {seed}: wrote {NUM_TRIPS} trips and {len(ZONES)} zones to {out_dir}")
