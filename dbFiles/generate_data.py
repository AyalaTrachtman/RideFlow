"""
generate_data.py  –  RideFlow PostgreSQL Idempotent Seeder
──────────────────────────────────────────────────────────────────────────────
Generates realistic synthetic data and inserts it directly into the RideFlow
PostgreSQL database.  Safe to run multiple times on a non-empty database:

  • Fetches MAX(pk) from the DB before assigning new IDs   → no PK collision
  • Pre-loads existing unique values (emails, phones, plates) from the DB
  • Checks natural-key existence before every INSERT       → no duplicate data
  • Wraps everything in one transaction (commit / rollback)

Insertion order (FK-safe):
    STOP → ROUTE → DRIVER → VEHICLE → INCLUDES → TRIP → PASSENGER → REGISTRATION

Usage:
    python generate_data.py                        # incremental seed
    python generate_data.py --reset                # wipe + full re-seed
    python generate_data.py --counts stop=30 trip=60 passenger=100
    python generate_data.py --tables trip passenger
    python generate_data.py --seed 42              # reproducible

Requirements:
    pip install psycopg2-binary python-dotenv faker
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import random
import sys
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ── Optional Faker ────────────────────────────────────────────────────────────
try:
    from faker import Faker
    _FAKER_AVAILABLE = True
except ImportError:
    _FAKER_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── .env  (two levels up: dbFiles → RideFlow root) ───────────────────────────
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# ── Insertion order ───────────────────────────────────────────────────────────
TABLE_ORDER = [
    "stop", "route", "driver", "vehicle",
    "includes", "trip", "passenger", "registration",
]

# Delete order for --reset (reverse FK dependency)
RESET_ORDER = list(reversed(TABLE_ORDER))

# Default *additional* row counts to insert per run
DEFAULT_COUNTS: dict[str, int] = {
    "stop":         20,
    "route":         8,
    "driver":       10,
    "vehicle":      10,
    "includes":      0,   # derived: 3-6 stops per new route
    "trip":         30,
    "passenger":    50,
    "registration":  0,   # derived: 0-8 per new trip
}

# ── Domain data ───────────────────────────────────────────────────────────────
ISRAELI_CITIES = [
    "Tel Aviv", "Jerusalem", "Haifa", "Rishon LeZion", "Petah Tikva",
    "Ashdod", "Netanya", "Beer Sheva", "Bnei Brak", "Holon",
    "Ramat Gan", "Ashkelon", "Rehovot", "Bat Yam", "Herzliya",
    "Kfar Saba", "Modi'in", "Ra'anana", "Lod", "Eilat",
    "Nahariya", "Hadera", "Nazareth", "Acre", "Afula",
    "Tiberias", "Nof HaGalil", "Kiryat Gat", "Dimona", "Arad",
]
SECTORS        = ["North", "South", "Center", "Jerusalem", "Haifa District", "Sharon", "Negev"]
VEHICLE_TYPES  = ["Bus", "Minibus", "Van", "Shuttle", "Electric Bus"]
CAPACITIES     = {"Bus": 80, "Minibus": 25, "Van": 15, "Shuttle": 20, "Electric Bus": 75}
LICENSE_TYPES  = ["B", "C", "D", "D1", "D+E"]
REG_STATUSES   = ["confirmed", "waitlisted", "cancelled"]
DEPARTURE_TIMES = [
    "06:00", "06:30", "07:00", "07:15", "07:30", "07:45",
    "08:00", "08:30", "09:00", "10:00", "12:00", "13:00",
    "14:00", "15:00", "16:00", "17:00", "18:00", "19:00",
]
ROUTE_PREFIXES = [
    "Northern Express", "Southern Line", "Central Loop", "Express", "Coastal",
    "Mountain Route", "Valley Line", "Metro Connect", "Ring Road", "Direct",
]
STOP_SUFFIXES = [
    "Central Station", "North", "South", "Mall", "University",
    "Bus Terminal", "Market", "Park", "Hospital", "Junction",
]


# ── Shared state (FK registry + uniqueness guards) ────────────────────────────
class _State:
    # IDs known to exist in the DB (pre-loaded + newly inserted)
    stop_ids:      list[int] = []
    route_ids:     list[int] = []
    driver_ids:    list[int] = []
    plate_numbers: list[str] = []
    trip_ids:      list[int] = []
    passenger_ids: list[int] = []

    # Route → stops mapping (built from INCLUDES rows)
    route_stops:  dict[int, list[int]] = {}
    # Trip → route mapping
    trip_route:   dict[int, int]       = {}

    # Uniqueness sets (pre-loaded from DB + newly inserted values)
    existing_stop_names:   set[str]         = set()
    existing_route_names:  set[str]         = set()
    existing_driver_keys:  set[tuple]       = set()  # (fullname, licenseType)
    existing_plates:       set[str]         = set()
    existing_emails:       set[str]         = set()
    existing_phones:       set[str]         = set()
    existing_trip_keys:    set[tuple]       = set()  # (trip_date, route_id, driver_id)
    existing_includes:     set[tuple]       = set()  # (route_id, stop_id)
    existing_reg_pairs:    set[tuple[int,int]] = set()  # (pass_id, trip_id)

_S = _State()

# ── Faker / fallback ──────────────────────────────────────────────────────────
_fake: "Faker | None" = None

def _init_faker(seed: int | None) -> None:
    global _fake
    if _FAKER_AVAILABLE:
        _fake = Faker("he_IL")
        if seed is not None:
            Faker.seed(seed)
    if seed is not None:
        random.seed(seed)


def _full_name() -> str:
    if _fake:
        return _fake.name()
    first = random.choice(["Avi","Maya","Yosef","Sara","David","Tamar",
                            "Moshe","Noa","Eitan","Shira","Ron","Lior"])
    last  = random.choice(["Cohen","Levi","Mizrahi","Peretz","Biton",
                            "Azulay","Friedman","Katz","Shapiro","Ben-David"])
    return f"{first} {last}"


def _unique_email(name: str, uid: int) -> str:
    parts = name.lower().split()
    local = "".join(c for c in "_".join(parts) if c.isalnum() or c == "_")
    domains = ["gmail.com","yahoo.com","walla.co.il","hotmail.com","outlook.com"]
    base  = f"{local}{uid}"
    email = f"{base}@{random.choice(domains)}"
    counter = 0
    while email in _S.existing_emails:
        counter += 1
        email = f"{base}_{counter}@{random.choice(domains)}"
    _S.existing_emails.add(email)
    return email


def _unique_phone(uid: int) -> str:
    prefixes = ["050","052","053","054","055","058"]
    phone = f"{random.choice(prefixes)}{str(uid % 10_000_000).zfill(7)}"
    counter = 0
    while phone in _S.existing_phones:
        counter += 1
        phone = f"{random.choice(prefixes)}{str((uid + counter * 97) % 10_000_000).zfill(7)}"
    _S.existing_phones.add(phone)
    return phone


def _unique_plate() -> str:
    letters = "ABCDEFGHIJKLMNPQRSTUVWXYZ"
    for _ in range(5000):
        plate = "".join(random.choices(letters, k=3)) + "-" + str(random.randint(100, 999))
        if plate not in _S.existing_plates:
            _S.existing_plates.add(plate)
            return plate
    raise RuntimeError("Could not generate a unique plate number after 5000 attempts.")


# ── Database connection ───────────────────────────────────────────────────────

def get_connection() -> psycopg2.extensions.connection:
    """Return a psycopg2 connection from .env credentials."""
  
    host     = os.getenv("DB_HOST", "localhost")
    port     = int(os.getenv("DB_PORT", 5432))
    dbname   = os.getenv("DB_NAME_SECRET", "").strip()
    user     = os.getenv("DB_USER_SECRET", "").strip()
    password = os.getenv("DB_PASSWORD_SECRET", "").strip()

    print("USER:", user)
    print("DB:", dbname)
    print("PASSWORD:", repr(password))


    if not all([dbname, user, password]):
        logger.error("Missing DB credentials in .env (%s).", ENV_PATH)
        sys.exit(1)

    logger.info("Connecting to '%s' at %s:%s as '%s'…", dbname, host, port, user)
    try:
        conn = psycopg2.connect(
            host=host, port=port,
            dbname=dbname, user=user, password=password,
        )
        conn.autocommit = False
        logger.info("Connection established.")
        return conn
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect: %s", exc)
        sys.exit(1)


# ── Pre-load existing DB state ────────────────────────────────────────────────

def _scalar(cursor, sql: str, params=()) -> Any:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return row[0] if row else None


def preload_existing_state(cursor) -> None:
    """
    Read the current DB contents into _S so that:
      - New IDs start above existing max PKs.
      - Uniqueness sets include already-present natural keys.
      - FK registries include all existing IDs.
    """
    logger.info("Pre-loading existing database state…")

    # ── STOP ──────────────────────────────────────────────────────────────────
    cursor.execute('SELECT stop_id, stop_name FROM "STOP"')
    for sid, sname in cursor.fetchall():
        _S.stop_ids.append(sid)
        _S.existing_stop_names.add(sname)

    # ── ROUTE ─────────────────────────────────────────────────────────────────
    cursor.execute('SELECT route_id, route_name FROM "ROUTE"')
    for rid, rname in cursor.fetchall():
        _S.route_ids.append(rid)
        _S.existing_route_names.add(rname)

    # ── DRIVER ────────────────────────────────────────────────────────────────
    cursor.execute('SELECT driver_id, driver_fullname, "licenseType" FROM "DRIVER"')
    for did, fname, ltype in cursor.fetchall():
        _S.driver_ids.append(did)
        _S.existing_driver_keys.add((fname, ltype))

    # ── VEHICLE ───────────────────────────────────────────────────────────────
    cursor.execute('SELECT plate_number FROM "VEHICLE"')
    for (plate,) in cursor.fetchall():
        _S.plate_numbers.append(plate)
        _S.existing_plates.add(plate)

    # ── INCLUDES ──────────────────────────────────────────────────────────────
    cursor.execute('SELECT route_id, stop_id FROM "INCLUDES"')
    for rid, sid in cursor.fetchall():
        _S.existing_includes.add((rid, sid))
        _S.route_stops.setdefault(rid, []).append(sid)

    # ── TRIP ──────────────────────────────────────────────────────────────────
    cursor.execute('SELECT trip_id, trip_date, route_id, driver_id FROM "TRIP"')
    for tid, tdate, rid, did in cursor.fetchall():
        _S.trip_ids.append(tid)
        _S.trip_route[tid] = rid
        _S.existing_trip_keys.add((tdate, rid, did))

    # ── PASSENGER ─────────────────────────────────────────────────────────────
    cursor.execute('SELECT pass_id, email, phone FROM "PASSENGER"')
    for pid, email, phone in cursor.fetchall():
        _S.passenger_ids.append(pid)
        if email:
            _S.existing_emails.add(email)
        if phone:
            _S.existing_phones.add(phone)

    # ── REGISTRATION ──────────────────────────────────────────────────────────
    cursor.execute('SELECT pass_id, trip_id FROM "REGISTRATION"')
    for pid, tid in cursor.fetchall():
        _S.existing_reg_pairs.add((pid, tid))

    logger.info(
        "Pre-load complete — stops:%d routes:%d drivers:%d vehicles:%d "
        "trips:%d passengers:%d registrations:%d",
        len(_S.stop_ids), len(_S.route_ids), len(_S.driver_ids),
        len(_S.plate_numbers), len(_S.trip_ids),
        len(_S.passenger_ids), len(_S.existing_reg_pairs),
    )


def _next_id(cursor, table: str, pk_col: str) -> int:
    """Return MAX(pk_col)+1 for *table*, or 1 if the table is empty."""
    val = _scalar(cursor, f'SELECT COALESCE(MAX("{pk_col}"), 0) FROM "{table}"')
    return int(val) + 1


# ── INSERT helper ─────────────────────────────────────────────────────────────

def _insert_row(cursor, table: str, row: dict[str, Any]) -> None:
    cols  = list(row.keys())
    query = (
        f'INSERT INTO "{table.upper()}" '
        f'({", ".join(f"{chr(34)}{c}{chr(34)}" for c in cols)}) '
        f'VALUES ({", ".join(["%s"] * len(cols))})'
    )
    cursor.execute(query, tuple(row.values()))


# ── Row generators ────────────────────────────────────────────────────────────

def _gen_stop_name() -> str:
    """Generate a stop name not already in the DB."""
    for _ in range(2000):
        city = random.choice(ISRAELI_CITIES)
        name = f"{city} – {random.choice(STOP_SUFFIXES)}"
        if name not in _S.existing_stop_names:
            return name
    # Fallback: add a random suffix to guarantee uniqueness
    return f"{random.choice(ISRAELI_CITIES)} – Stop-{random.randint(1000, 9999)}"


def _gen_route_name() -> str:
    for _ in range(2000):
        prefix = random.choice(ROUTE_PREFIXES)
        origin = random.choice(ISRAELI_CITIES)
        dest   = random.choice([c for c in ISRAELI_CITIES if c != origin])
        name   = f"{prefix}: {origin} → {dest}"
        if name not in _S.existing_route_names:
            return name
    return f"Route-{random.randint(1000, 9999)}: {random.choice(ISRAELI_CITIES)} → {random.choice(ISRAELI_CITIES)}"


def _gen_driver_fields() -> tuple[str, str]:
    """Return (fullname, licenseType) not already in the DB."""
    for _ in range(2000):
        name  = _full_name()
        ltype = random.choice(LICENSE_TYPES)
        if (name, ltype) not in _S.existing_driver_keys:
            return name, ltype
    return f"Driver-{random.randint(1000, 9999)}", random.choice(LICENSE_TYPES)


def _gen_trip_fields(trip_id: int) -> dict[str, Any] | None:
    """Return trip field dict with a unique (date, route, driver) combo, or None."""
    anchor = datetime.date(2024, 1, 1)
    for _ in range(500):
        route_id  = random.choice(_S.route_ids)
        driver_id = random.choice(_S.driver_ids)
        trip_date = anchor + datetime.timedelta(days=random.randint(0, 365))
        key = (trip_date, route_id, driver_id)
        if key not in _S.existing_trip_keys:
            _S.existing_trip_keys.add(key)
            _S.trip_route[trip_id] = route_id
            return {
                "trip_id":         trip_id,
                "trip_date":       trip_date,
                "departure_Time":  random.choice(DEPARTURE_TIMES),
                "available_Seats": random.randint(1, 80),
                "route_id":        route_id,
                "driver_id":       driver_id,
                "plate_number":    random.choice(_S.plate_numbers),
            }
    return None


# ── Table seeders ─────────────────────────────────────────────────────────────

def seed_table(cursor, table: str, count: int) -> tuple[int, int]:
    """
    Seed *count* new rows into *table*.
    Returns (inserted, skipped).
    """
    t        = table.lower()
    inserted = 0
    skipped  = 0

    # ── STOP ──────────────────────────────────────────────────────────────────
    if t == "stop":
        next_id = _next_id(cursor, "STOP", "stop_id")
        for i in range(count):
            sid  = next_id + i
            name = _gen_stop_name()
            if name in _S.existing_stop_names:
                logger.debug("[STOP] SKIPPED existing stop_name '%s'.", name)
                skipped += 1
                continue
            try:
                _insert_row(cursor, "stop", {"stop_id": sid, "stop_name": name})
                _S.stop_ids.append(sid)
                _S.existing_stop_names.add(name)
                logger.debug("[STOP] INSERTED stop_id=%d '%s'.", sid, name)
                inserted += 1
            except Exception as exc:
                logger.error("[STOP] stop_id=%d skipped – %s", sid, exc)
                skipped += 1

    # ── ROUTE ─────────────────────────────────────────────────────────────────
    elif t == "route":
        next_id = _next_id(cursor, "ROUTE", "route_id")
        for i in range(count):
            rid  = next_id + i
            name = _gen_route_name()
            if name in _S.existing_route_names:
                logger.debug("[ROUTE] SKIPPED existing route_name '%s'.", name)
                skipped += 1
                continue
            try:
                _insert_row(cursor, "route", {"route_name": name, "route_id": rid})
                _S.route_ids.append(rid)
                _S.existing_route_names.add(name)
                logger.debug("[ROUTE] INSERTED route_id=%d '%s'.", rid, name)
                inserted += 1
            except Exception as exc:
                logger.error("[ROUTE] route_id=%d skipped – %s", rid, exc)
                skipped += 1

    # ── DRIVER ────────────────────────────────────────────────────────────────
    elif t == "driver":
        next_id = _next_id(cursor, "DRIVER", "driver_id")
        for i in range(count):
            did   = next_id + i
            name, ltype = _gen_driver_fields()
            key   = (name, ltype)
            if key in _S.existing_driver_keys:
                logger.debug("[DRIVER] SKIPPED existing driver '%s'.", name)
                skipped += 1
                continue
            try:
                _insert_row(cursor, "driver", {
                    "licenseType": ltype, "driver_fullname": name, "driver_id": did,
                })
                _S.driver_ids.append(did)
                _S.existing_driver_keys.add(key)
                logger.debug("[DRIVER] INSERTED driver_id=%d '%s'.", did, name)
                inserted += 1
            except Exception as exc:
                logger.error("[DRIVER] driver_id=%d skipped – %s", did, exc)
                skipped += 1

    # ── VEHICLE ───────────────────────────────────────────────────────────────
    elif t == "vehicle":
        for _ in range(count):
            try:
                vtype = random.choice(VEHICLE_TYPES)
                plate = _unique_plate()      # already guards against duplicates
                _insert_row(cursor, "vehicle", {
                    "capacity": CAPACITIES[vtype], "vehicle_type": vtype, "plate_number": plate,
                })
                _S.plate_numbers.append(plate)
                logger.debug("[VEHICLE] INSERTED plate=%s.", plate)
                inserted += 1
            except Exception as exc:
                logger.error("[VEHICLE] row skipped – %s", exc)
                skipped += 1

    # ── INCLUDES ──────────────────────────────────────────────────────────────
    elif t == "includes":
        if not _S.stop_ids or not _S.route_ids:
            logger.warning("[INCLUDES] Requires STOP and ROUTE — skipping.")
            return 0, 0
        # Only assign stops to routes that are new this run (not already in route_stops)
        new_routes = [r for r in _S.route_ids if r not in _S.route_stops]
        for route_id in new_routes:
            n      = random.randint(3, min(6, len(_S.stop_ids)))
            chosen = random.sample(_S.stop_ids, n)
            _S.route_stops[route_id] = chosen
            for stop_id in chosen:
                pair = (route_id, stop_id)
                if pair in _S.existing_includes:
                    logger.debug("[INCLUDES] SKIPPED existing (%d, %d).", route_id, stop_id)
                    skipped += 1
                    continue
                try:
                    _insert_row(cursor, "includes", {"route_id": route_id, "stop_id": stop_id})
                    _S.existing_includes.add(pair)
                    logger.debug("[INCLUDES] INSERTED route=%d stop=%d.", route_id, stop_id)
                    inserted += 1
                except Exception as exc:
                    logger.error("[INCLUDES] (%d,%d) skipped – %s", route_id, stop_id, exc)
                    skipped += 1

    # ── TRIP ──────────────────────────────────────────────────────────────────
    elif t == "trip":
        if not (_S.route_ids and _S.driver_ids and _S.plate_numbers):
            logger.warning("[TRIP] Requires ROUTE, DRIVER, VEHICLE — skipping.")
            return 0, 0
        next_id = _next_id(cursor, "TRIP", "trip_id")
        attempts = 0
        i = 0
        while i < count and attempts < count * 10:
            attempts += 1
            tid = next_id + i
            fields = _gen_trip_fields(tid)
            if fields is None:
                logger.warning("[TRIP] Could not find unique (date,route,driver) after retries.")
                skipped += 1
                i += 1
                continue
            try:
                _insert_row(cursor, "trip", fields)
                _S.trip_ids.append(tid)
                logger.debug("[TRIP] INSERTED trip_id=%d.", tid)
                inserted += 1
                i += 1
            except Exception as exc:
                logger.error("[TRIP] trip_id=%d skipped – %s", tid, exc)
                skipped += 1
                i += 1

    # ── PASSENGER ─────────────────────────────────────────────────────────────
    elif t == "passenger":
        next_id = _next_id(cursor, "PASSENGER", "pass_id")
        for i in range(count):
            pid  = next_id + i
            name = _full_name()
            email = _unique_email(name, pid)
            phone = _unique_phone(pid)
            try:
                _insert_row(cursor, "passenger", {
                    "email": email, "phone": phone,
                    "pass_fullname": name, "pass_id": pid,
                    "sector": random.choice(SECTORS + [None]),
                })
                _S.passenger_ids.append(pid)
                logger.debug("[PASSENGER] INSERTED pass_id=%d.", pid)
                inserted += 1
            except Exception as exc:
                logger.error("[PASSENGER] pass_id=%d skipped – %s", pid, exc)
                skipped += 1

    # ── REGISTRATION ──────────────────────────────────────────────────────────
    elif t == "registration":
        if not (_S.trip_ids and _S.passenger_ids):
            logger.warning("[REGISTRATION] Requires TRIP and PASSENGER — skipping.")
            return 0, 0
        next_id = _next_id(cursor, "REGISTRATION", "reg_id")
        reg_id  = next_id
        # Only register passengers onto trips that were inserted this run
        new_trips = [tid for tid in _S.trip_ids
                     if tid >= (_next_id(cursor, "TRIP", "trip_id") - len(_S.trip_ids))]
        # Simpler: iterate all trips and register a few random passengers each
        for trip_id in _S.trip_ids:
            route_id    = _S.trip_route.get(trip_id)
            route_stops = _S.route_stops.get(route_id, _S.stop_ids[:2] or [1])
            max_regs    = random.randint(0, min(8, len(_S.passenger_ids)))
            candidates  = random.sample(_S.passenger_ids, min(max_regs, len(_S.passenger_ids)))
            for pass_id in candidates:
                pair = (pass_id, trip_id)
                if pair in _S.existing_reg_pairs:
                    logger.debug("[REGISTRATION] SKIPPED existing (pass=%d, trip=%d).",
                                 pass_id, trip_id)
                    skipped += 1
                    continue
                stops    = route_stops if route_stops else _S.stop_ids[:1]
                boarding, dropoff = (random.sample(stops, 2) if len(stops) >= 2
                                     else (stops[0], stops[0]))
                try:
                    _insert_row(cursor, "registration", {
                        "reg_id":           reg_id,
                        "status":           random.choice(REG_STATUSES),
                        "pass_id":          pass_id,
                        "trip_id":          trip_id,
                        "boarding_stop_id": boarding,
                        "dropoff_stop_id":  dropoff,
                    })
                    _S.existing_reg_pairs.add(pair)
                    reg_id += 1
                    inserted += 1
                except Exception as exc:
                    logger.error("[REGISTRATION] reg_id=%d skipped – %s", reg_id, exc)
                    skipped += 1

    return inserted, skipped


# ── Reset helper ──────────────────────────────────────────────────────────────

def reset_database(cursor) -> None:
    """Delete all rows from every table in FK-safe reverse order."""
    logger.warning("──── RESET: deleting all rows from all tables ────")
    for table in RESET_ORDER:
        cursor.execute(f'DELETE FROM "{table.upper()}"')
        logger.info("[RESET] Cleared table %s.", table.upper())


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "RideFlow idempotent PostgreSQL seeder.\n"
            "Safe to run on empty OR populated databases."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--counts", nargs="+", metavar="TABLE=N", default=[],
        help="Additional rows to insert per table, e.g. --counts stop=30 trip=50",
    )
    parser.add_argument(
        "--tables", nargs="+", metavar="TABLE", default=TABLE_ORDER,
        help=f"Tables to seed. Choices: {', '.join(TABLE_ORDER)}",
    )
    parser.add_argument(
        "--seed", type=int, default=None, metavar="INT",
        help="Random seed for reproducible output.",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete ALL existing data then insert a fresh dataset.",
    )
    args = parser.parse_args()

    _init_faker(args.seed)
    if not _FAKER_AVAILABLE:
        logger.warning("Faker not installed – using built-in lists. "
                       "Install with:  pip install faker")

    # Parse --counts
    counts = dict(DEFAULT_COUNTS)
    for item in args.counts:
        if "=" not in item:
            logger.error("Invalid --counts entry %r (expected TABLE=N)", item)
            sys.exit(1)
        tbl, _, n = item.partition("=")
        if tbl.lower() not in counts:
            logger.error("Unknown table %r. Choices: %s", tbl, list(counts))
            sys.exit(1)
        counts[tbl.lower()] = int(n)

    # Expand with required parent tables
    deps: dict[str, set[str]] = {
        "stop":         set(),
        "route":        set(),
        "driver":       set(),
        "vehicle":      set(),
        "includes":     {"stop", "route"},
        "trip":         {"route", "driver", "vehicle"},
        "passenger":    set(),
        "registration": {"trip", "passenger", "stop", "route", "includes"},
    }
    to_seed: set[str] = {t.lower() for t in args.tables}
    changed = True
    while changed:
        changed = False
        for tbl in list(to_seed):
            for parent in deps.get(tbl, set()):
                if parent not in to_seed:
                    logger.info("Auto-adding required parent table '%s'.", parent)
                    to_seed.add(parent)
                    changed = True

    # ── Connect ───────────────────────────────────────────────────────────────
    conn   = get_connection()
    cursor = conn.cursor()

    try:
        # Optional wipe
        if args.reset:
            reset_database(cursor)

        # Pre-load existing state (skipped when --reset clears everything)
        if not args.reset:
            preload_existing_state(cursor)

        # ── Seed ──────────────────────────────────────────────────────────────
        grand_inserted = 0
        grand_skipped  = 0

        for table in TABLE_ORDER:
            if table not in to_seed:
                continue
            n = counts.get(table, 0)
            logger.info("── Seeding %s (target: %s new rows) ──",
                        table.upper(), n if n else "auto")
            ins, skp = seed_table(cursor, table, n)
            logger.info("   ✓ %s — INSERTED: %d  SKIPPED: %d", table.upper(), ins, skp)
            grand_inserted += ins
            grand_skipped  += skp

        conn.commit()
        logger.info(
            "══ Done. TOTAL INSERTED: %d  TOTAL SKIPPED: %d ══",
            grand_inserted, grand_skipped,
        )

    except Exception as exc:
        conn.rollback()
        logger.error("Fatal error – transaction rolled back: %s", exc)
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
