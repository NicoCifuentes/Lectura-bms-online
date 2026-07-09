import asyncio
import time
import sqlite3
import requests
import json
from bleak import BleakClient

# ================= CONFIG =================

ADDRESS = "A5:C2:37:27:3B:5F" #Cambiar segun baterias

NOTIFY_CHAR = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_CHAR  = "0000ff02-0000-1000-8000-00805f9b34fb"

READ_INTERVAL = 60

API_URL = "http://api.oceandev.cl/api/data"
TOKEN = "bms-dxn8pgultc0ig4rhai5uqgpc" #Cambiar con el token que te entrega la plataforma

DB_PATH = "bms.db"

# ================= DB =================

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

conn.execute("PRAGMA journal_mode=WAL;")

cursor.execute("""
CREATE TABLE IF NOT EXISTS bms_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER,
    voltage REAL,
    current REAL,
    power REAL,
    soc INTEGER,
    cap_rem REAL,
    cap_nom REAL,
    cycles INTEGER,
    delta_v REAL,
    status TEXT,
    uploaded INTEGER DEFAULT 0
)
""")
conn.commit()

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

    elif current < -0.2:
        return "descargando"

    else:
        return "reposo"

# ================= INTERNET =================

def has_internet():
    try:
        requests.get("https://www.google.com", timeout=3)
        return True
    except:
        return False

# ================= API =================

def upload_to_server(data):
    payload = {
        "token": TOKEN,
        "data": {
            "v": data["voltage"],
            "c": data["current"],
            "p": data["power"],
            "soc": data["soc"],
            "cr": data["capacity_rem_ah"],
            "cn": data["capacity_nom_ah"],
            "cy": data["cycles"],
            "dv": data["delta_v"],
            "st": data["status"]
        }
    }

    try:
        r = requests.post(API_URL, json=payload, timeout=5)
        print("STATUS:", r.status_code, r.text)
        return r.status_code in (200, 201)
    except Exception as e:
        print("Error conexion:", e)
        return False
# ================= BUFFER =================

def save_local(data):

    cursor.execute("""
        INSERT INTO bms_data 
        (ts, voltage, current, power, soc, cap_rem, cap_nom, cycles, delta_v, status, uploaded)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (
        data["ts"],
        data["voltage"],
        data["current"],
        data["power"],
        data["soc"],
        data["capacity_rem_ah"],
        data["capacity_nom_ah"],
        data["cycles"],
        data["delta_v"],
        data["status"]
    ))

    conn.commit()

def upload_pending():

    rows = cursor.execute("""
        SELECT * FROM bms_data 
        WHERE uploaded = 0 
        ORDER BY id ASC 
        LIMIT 20
    """).fetchall()

    for row in rows:

        data = {
            "ts": row[1],
            "voltage": row[2],
            "current": row[3],
            "power": row[4],
            "soc": row[5],
            "capacity_rem_ah": row[6],
            "capacity_nom_ah": row[7],
            "cycles": row[8],
            "delta_v": row[9],
            "status": row[10]
        }

        if upload_to_server(data):
            cursor.execute("UPDATE bms_data SET uploaded = 1 WHERE id = ?", (row[0],))
            conn.commit()
            print("? Enviado ID:", row[0])
        else:
            break

# ================= BLE =================

def cmd(frame):
    tail = {0x03: (0xFF, 0xFD), 0x04: (0xFF, 0xFC)}
    return bytearray([0xDD, 0xA5, frame, 0x00, tail[frame][0], tail[frame][1], 0x77])

def extract_payload(pkt):
    if len(pkt) < 6:
        return None
    return pkt[4:-1]

def parse_basic(p):
    return {
        "voltage": int.from_bytes(p[0:2], "big") / 100,
        "current": int.from_bytes(p[2:4], "big", signed=True) / 100,
        "cap_rem": int.from_bytes(p[4:6], "big") / 100,
        "cap_nom": int.from_bytes(p[6:8], "big") / 100,
        "cycles": int.from_bytes(p[8:10], "big"),
        "soc": p[19] if len(p) > 19 else None
    }

def parse_cells(p):
    cells = []
    for i in range(0, len(p), 2):
        mv = int.from_bytes(p[i:i+2], "big")
        if 1000 <= mv <= 5000:
            cells.append(mv / 1000.0)
    return cells

async def request_packet(client, frame):

    buffer = bytearray()
    event = asyncio.Event()

    def notify(sender, data):
        nonlocal buffer
        buffer.extend(data)
        if 0x77 in data:
            event.set()

    await client.start_notify(NOTIFY_CHAR, notify)
    await client.write_gatt_char(WRITE_CHAR, cmd(frame))

    try:
        await asyncio.wait_for(event.wait(), timeout=5)
    except:
        await client.stop_notify(NOTIFY_CHAR)
        return None

    await client.stop_notify(NOTIFY_CHAR)

    try:
        s = buffer.index(0xDD)
        e = buffer.index(0x77, s)
        return buffer[s:e+1]
    except:
        return None

async def get_sample(client):

    pkt03 = await request_packet(client, 0x03)
    pkt04 = await request_packet(client, 0x04)

    if not pkt03 or not pkt04:
        return None

    p03 = extract_payload(pkt03)
    p04 = extract_payload(pkt04)

    if not p03 or not p04:
        return None

    info = parse_basic(p03)
    cells = parse_cells(p04)

    if not cells:
        return None

    vmin = min(cells)
    vmax = max(cells)

    data = {
        "ts": int(time.time()),
        "voltage": info["voltage"],
        "current": info["current"],
        "power": info["voltage"] * info["current"],
        "soc": info["soc"],
        "capacity_rem_ah": info["cap_rem"],
        "capacity_nom_ah": info["cap_nom"],
        "cycles": info["cycles"],
        "delta_v": vmax - vmin
    }

    data["status"] = get_status(data)

    return data

# ================= MAIN =================

async def main_loop():

    while True:
        try:
            async with BleakClient(ADDRESS) as client:
                print("?? Conectado al BMS")

                while True:

                    data = await get_sample(client)

                    if data:
                        print("??", data)

                        save_local(data)

                        if has_internet():
                            upload_pending()

                    await asyncio.sleep(READ_INTERVAL)

        except Exception as e:
            print("Error BLE:", e)
            await asyncio.sleep(10)

# ================= RUN =================

if __name__ == "__main__":
    asyncio.run(main_loop())