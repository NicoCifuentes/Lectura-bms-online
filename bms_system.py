import asyncio
import json
import os
import sqlite3
import time
from urllib.parse import urlsplit

import requests
from bleak import BleakClient

# ================= CONFIG =================

ADDRESS = "A5:C2:37:58:BF:EA"

NOTIFY_CHAR = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_CHAR = "0000ff02-0000-1000-8000-00805f9b34fb"

READ_INTERVAL = 60
REQUEST_TIMEOUT = 8
PENDING_BATCH_SIZE = 50

API_URL = "http://api.oceandev.cl/api/data"
TOKEN = "bms-rulltnrmv3uinuzkwmrnjnhk"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bms.db")
TABLE_NAME = "bms_data"

print(f"[DB] Usando SQLite en: {DB_PATH}")

# ================= HTTP =================

session = requests.Session()
session.headers.update(
    {
        "Content-Type": "application/json",
        "User-Agent": "rpi-bms-uploader/3.0",
    }
)

# ================= DB =================

conn = sqlite3.connect(DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")


def print_schema_debug():
    try:
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        print("[DB] Tablas disponibles:")
        if not tables:
            print(" - (ninguna)")
        else:
            for row in tables:
                print(f" - {row['name']}")

        columns = cursor.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
        if columns:
            print(f"[DB] Columnas de {TABLE_NAME}:")
            for col in columns:
                print(f" - {col['name']} ({col['type']})")
        else:
            print(f"[DB] La tabla {TABLE_NAME} aun no existe.")
    except Exception as exc:
        print(f"[DB] Error leyendo esquema: {exc}")


def ensure_schema():
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            voltage REAL,
            current REAL,
            power REAL,
            soc INTEGER,
            cap_rem REAL,
            cap_nom REAL,
            cycles INTEGER,
            delta_v REAL,
            temperature REAL,
            status TEXT,
            uploaded INTEGER DEFAULT 0,
            uploaded_at INTEGER DEFAULT NULL
        )
        """
    )

    existing_columns = {
        row["name"]
        for row in cursor.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
    }

    if "uploaded_at" not in existing_columns:
        cursor.execute(
            f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMN uploaded_at INTEGER DEFAULT NULL
            """
        )
        print("[DB] Added missing column: uploaded_at")

    if "temperature" not in existing_columns:
        cursor.execute(
            f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMN temperature REAL DEFAULT NULL
            """
        )
        print("[DB] Added missing column: temperature")

    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_uploaded_id
        ON {TABLE_NAME} (uploaded, id)
        """
    )

    conn.commit()
    print("[DB] Esquema SQLite verificado.")


ensure_schema()
print_schema_debug()

# ================= STATUS =================


def get_status(data):
    current = data["current"]
    soc = data["soc"]
    delta_v = data["delta_v"]

    if soc is not None and soc < 20:
        return "critica"

    if delta_v is not None and delta_v > 0.05:
        return "desbalance"

    if current > 0.2:
        return "cargando"

    if current < -0.2:
        return "descargando"

    return "reposo"


# ================= CONNECTIVITY =================


def api_reachable():
    target = urlsplit(API_URL)
    probe_url = f"{target.scheme}://{target.netloc}/"

    try:
        response = session.get(probe_url, timeout=3)
        print(f"[NET] API reachable: {probe_url} -> {response.status_code}")
        return True
    except Exception as exc:
        print(f"[NET] API unreachable: {exc}")
        return False


# ================= API =================


def upload_to_server(data):
    payload = {
        "token": TOKEN,
        "ts": data["ts"],
        "data": {
            "v": data["voltage"],
            "c": data["current"],
            "p": data["power"],
            "soc": data["soc"],
            "cr": data["capacity_rem_ah"],
            "cn": data["capacity_nom_ah"],
            "cy": data["cycles"],
            "dv": data["delta_v"],
            "t": data.get("temperature"),
            "st": data["status"],
        },
    }

    try:
        response = session.post(API_URL, data=json.dumps(payload), timeout=REQUEST_TIMEOUT)
        print(f"[API] POST {response.status_code} -> {response.text[:500]}")
        return response.status_code in (200, 201)
    except Exception as exc:
        print(f"[API] Upload error: {exc}")
        return False


# ================= BUFFER =================


def save_local(data):
    cursor.execute(
        f"""
        INSERT INTO {TABLE_NAME}
        (ts, voltage, current, power, soc, cap_rem, cap_nom, cycles, delta_v, temperature, status, uploaded)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (
            data["ts"],
            data["voltage"],
            data["current"],
            data["power"],
            data["soc"],
            data["capacity_rem_ah"],
            data["capacity_nom_ah"],
            data["cycles"],
            data["delta_v"],
            data.get("temperature"),
            data["status"],
        ),
    )

    conn.commit()


def pending_count():
    row = cursor.execute(
        f"SELECT COUNT(*) AS total FROM {TABLE_NAME} WHERE uploaded = 0"
    ).fetchone()
    return row["total"] if row else 0


def upload_pending():
    total_uploaded = 0

    while True:
        rows = cursor.execute(
            f"""
            SELECT *
            FROM {TABLE_NAME}
            WHERE uploaded = 0
            ORDER BY id ASC
            LIMIT ?
            """,
            (PENDING_BATCH_SIZE,),
        ).fetchall()

        if not rows:
            if total_uploaded:
                print(f"[SYNC] Backlog drained: {total_uploaded} rows uploaded.")
            return total_uploaded

        uploaded_this_batch = 0

        for row in rows:
            data = {
                "ts": row["ts"],
                "voltage": row["voltage"],
                "current": row["current"],
                "power": row["power"],
                "soc": row["soc"],
                "capacity_rem_ah": row["cap_rem"],
                "capacity_nom_ah": row["cap_nom"],
                "cycles": row["cycles"],
                "delta_v": row["delta_v"],
                "temperature": row["temperature"],
                "status": row["status"],
            }

            if upload_to_server(data):
                cursor.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET uploaded = 1, uploaded_at = ?
                    WHERE id = ?
                    """,
                    (int(time.time()), row["id"]),
                )
                conn.commit()
                uploaded_this_batch += 1
                total_uploaded += 1
                print(f"[SYNC] Uploaded local row id={row['id']} ts={row['ts']}")
            else:
                print("[SYNC] Upload stopped due to API failure.")
                return total_uploaded

        if uploaded_this_batch < len(rows):
            return total_uploaded


# ================= BLE =================


def cmd(frame):
    tail = {0x03: (0xFF, 0xFD), 0x04: (0xFF, 0xFC)}
    return bytearray([0xDD, 0xA5, frame, 0x00, tail[frame][0], tail[frame][1], 0x77])


def extract_payload(packet):
    if len(packet) < 6:
        return None
    return packet[4:-1]


def parse_temperature(payload):
    if len(payload) <= 22:
        return None

    sensor_count = payload[22]
    if sensor_count <= 0:
        return None

    temperatures = []
    base_offset = 23

    for index in range(sensor_count):
        start = base_offset + (index * 2)
        end = start + 2

        if end > len(payload):
            break

        raw_value = int.from_bytes(payload[start:end], "big")
        if raw_value <= 0:
            continue

        celsius = (raw_value - 2731) / 10
        temperatures.append(celsius)

    if not temperatures:
        return None

    return round(sum(temperatures) / len(temperatures), 2)


def parse_basic(payload):
    return {
        "voltage": int.from_bytes(payload[0:2], "big") / 100,
        "current": int.from_bytes(payload[2:4], "big", signed=True) / 100,
        "cap_rem": int.from_bytes(payload[4:6], "big") / 100,
        "cap_nom": int.from_bytes(payload[6:8], "big") / 100,
        "cycles": int.from_bytes(payload[8:10], "big"),
        "soc": payload[19] if len(payload) > 19 else None,
        "temperature": parse_temperature(payload),
    }


def parse_cells(payload):
    cells = []
    for index in range(0, len(payload), 2):
        millivolts = int.from_bytes(payload[index:index + 2], "big")
        if 1000 <= millivolts <= 5000:
            cells.append(millivolts / 1000.0)
    return cells


async def request_packet(client, frame):
    buffer = bytearray()
    event = asyncio.Event()

    def notify(_sender, data):
        nonlocal buffer
        buffer.extend(data)
        if 0x77 in data:
            event.set()

    await client.start_notify(NOTIFY_CHAR, notify)
    await client.write_gatt_char(WRITE_CHAR, cmd(frame))

    try:
        await asyncio.wait_for(event.wait(), timeout=5)
    except Exception:
        await client.stop_notify(NOTIFY_CHAR)
        return None

    await client.stop_notify(NOTIFY_CHAR)

    try:
        start = buffer.index(0xDD)
        end = buffer.index(0x77, start)
        return buffer[start:end + 1]
    except Exception:
        return None


async def get_sample(client):
    packet_03 = await request_packet(client, 0x03)
    packet_04 = await request_packet(client, 0x04)

    if not packet_03 or not packet_04:
        return None

    payload_03 = extract_payload(packet_03)
    payload_04 = extract_payload(packet_04)

    if not payload_03 or not payload_04:
        return None

    info = parse_basic(payload_03)
    cells = parse_cells(payload_04)

    if not cells:
        return None

    vmin = min(cells)
    vmax = max(cells)
    timestamp = int(time.time())

    data = {
        "ts": timestamp,
        "voltage": info["voltage"],
        "current": info["current"],
        "power": round(info["voltage"] * info["current"], 3),
        "soc": info["soc"],
        "capacity_rem_ah": info["cap_rem"],
        "capacity_nom_ah": info["cap_nom"],
        "cycles": info["cycles"],
        "delta_v": round(vmax - vmin, 4),
        "temperature": info["temperature"],
    }

    data["status"] = get_status(data)
    return data


# ================= MAIN =================


async def main_loop():
    while True:
        try:
            async with BleakClient(ADDRESS) as client:
                print(f"[BLE] Connected to BMS {ADDRESS}")

                while True:
                    data = await get_sample(client)

                    if data:
                        print(f"[BMS] Sample: {data}")
                        save_local(data)
                        print(f"[DB] Local sample stored. Pending rows: {pending_count()}")

                        if api_reachable():
                            upload_pending()

                    await asyncio.sleep(READ_INTERVAL)

        except Exception as exc:
            print(f"[BLE] Connection error: {exc}")
            await asyncio.sleep(10)


# ================= RUN =================

if __name__ == "__main__":
    asyncio.run(main_loop())
