import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict

import websockets
from supabase import Client, create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── 환경 변수 ────────────────────────────────────────────────
API_KEY = os.environ["AISSTREAM_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SVC_KEY = os.environ["SUPABASE_SERVICE_KEY"]

# 배치형 스냅샷 설정
COLLECT_SECONDS = int(os.getenv("COLLECT_SECONDS", "900"))      # 기본 15분
MAX_STALE_SECONDS = int(os.getenv("MAX_STALE_SECONDS", "300"))  # 5분 이상 안 보이면 제외
SCORE_ANCHORED = float(os.getenv("SCORE_ANCHORED", "6.0"))
SCORE_BERTHED = float(os.getenv("SCORE_BERTHED", "2.0"))

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SVC_KEY)
AISSTREAM_WS = "wss://stream.aisstream.io/v0/stream"

# ─── 50개 항만 정의 ───────────────────────────────────────────
# box: [[min_lat, min_lon], [max_lat, max_lon]]
PORTS: Dict[str, dict] = {
    # 한국·일본
    "KRPUS": {"name": "Busan", "country": "KR", "region": "kr-jp", "lat": 35.10, "lon": 129.04, "box": [[34.9, 128.7], [35.3, 129.3]]},
    "KRICN": {"name": "Incheon", "country": "KR", "region": "kr-jp", "lat": 37.45, "lon": 126.55, "box": [[37.3, 126.3], [37.6, 126.8]]},
    "JPNGO": {"name": "Nagoya", "country": "JP", "region": "kr-jp", "lat": 35.07, "lon": 136.88, "box": [[34.8, 136.5], [35.4, 137.2]]},
    "JPYOK": {"name": "Yokohama", "country": "JP", "region": "kr-jp", "lat": 35.45, "lon": 139.65, "box": [[35.2, 139.4], [35.7, 139.9]]},
    "JPTYO": {"name": "Tokyo", "country": "JP", "region": "kr-jp", "lat": 35.62, "lon": 139.77, "box": [[35.4, 139.5], [35.8, 140.1]]},
    "JPUKB": {"name": "Kobe", "country": "JP", "region": "kr-jp", "lat": 34.68, "lon": 135.20, "box": [[34.5, 134.9], [34.9, 135.5]]},

    # 중국
    "CNSHA": {"name": "Shanghai", "country": "CN", "region": "china", "lat": 31.23, "lon": 121.47, "box": [[30.8, 121.0], [31.6, 122.0]]},
    "CNQIN": {"name": "Qingdao", "country": "CN", "region": "china", "lat": 36.07, "lon": 120.38, "box": [[35.8, 120.0], [36.3, 120.7]]},
    "CNNGB": {"name": "Ningbo", "country": "CN", "region": "china", "lat": 29.87, "lon": 121.55, "box": [[29.6, 121.2], [30.1, 122.0]]},
    "CNTXG": {"name": "Tianjin", "country": "CN", "region": "china", "lat": 38.98, "lon": 117.72, "box": [[38.7, 117.3], [39.2, 118.1]]},
    "CNYTN": {"name": "Yantian", "country": "CN", "region": "china", "lat": 22.57, "lon": 114.27, "box": [[22.3, 114.0], [22.8, 114.6]]},
    "CNNSA": {"name": "Nansha", "country": "CN", "region": "china", "lat": 22.77, "lon": 113.57, "box": [[22.5, 113.2], [23.0, 113.9]]},
    "CNDLC": {"name": "Dalian", "country": "CN", "region": "china", "lat": 38.91, "lon": 121.60, "box": [[38.6, 121.2], [39.1, 122.0]]},

    # 동남아시아
    "VNTOT": {"name": "Cai Mep", "country": "VN", "region": "sea", "lat": 10.52, "lon": 107.03, "box": [[10.2, 106.7], [10.8, 107.4]]},
    "VNHPH": {"name": "Haiphong", "country": "VN", "region": "sea", "lat": 20.87, "lon": 106.68, "box": [[20.6, 106.4], [21.1, 107.0]]},
    "THLCH": {"name": "Laem Chabang", "country": "TH", "region": "sea", "lat": 13.08, "lon": 100.88, "box": [[12.8, 100.6], [13.4, 101.2]]},
    "SGSIN": {"name": "Singapore", "country": "SG", "region": "sea", "lat": 1.26, "lon": 103.82, "box": [[0.9, 103.5], [1.6, 104.2]]},
    "MYLPK": {"name": "Port Klang", "country": "MY", "region": "sea", "lat": 3.00, "lon": 101.38, "box": [[2.7, 101.0], [3.3, 101.7]]},
    "IDJKT": {"name": "Jakarta", "country": "ID", "region": "sea", "lat": -6.10, "lon": 106.88, "box": [[-6.4, 106.5], [-5.7, 107.2]]},
    "IDSUB": {"name": "Surabaya", "country": "ID", "region": "sea", "lat": -7.20, "lon": 112.73, "box": [[-7.5, 112.4], [-6.9, 113.1]]},
    "PHMNL": {"name": "Manila", "country": "PH", "region": "sea", "lat": 14.59, "lon": 120.97, "box": [[14.2, 120.6], [14.9, 121.3]]},

    # 남아시아·중동
    "LKCMB": {"name": "Colombo", "country": "LK", "region": "sa-me", "lat": 6.93, "lon": 79.85, "box": [[6.6, 79.5], [7.2, 80.2]]},
    "AEJEA": {"name": "Jebel Ali", "country": "AE", "region": "sa-me", "lat": 24.98, "lon": 55.06, "box": [[24.6, 54.7], [25.3, 55.4]]},
    "INBOM": {"name": "Mumbai", "country": "IN", "region": "sa-me", "lat": 18.94, "lon": 72.84, "box": [[18.6, 72.5], [19.2, 73.1]]},
    "JOAQJ": {"name": "Aqaba", "country": "JO", "region": "sa-me", "lat": 29.52, "lon": 35.00, "box": [[29.2, 34.7], [29.8, 35.3]]},
    "ILASH": {"name": "Ashdod", "country": "IL", "region": "sa-me", "lat": 31.82, "lon": 34.65, "box": [[31.5, 34.3], [32.1, 34.9]]},

    # 유럽
    "NLRTM": {"name": "Rotterdam", "country": "NL", "region": "europe", "lat": 51.92, "lon": 4.48, "box": [[51.6, 4.0], [52.2, 5.0]]},
    "DEHAM": {"name": "Hamburg", "country": "DE", "region": "europe", "lat": 53.55, "lon": 9.99, "box": [[53.3, 9.5], [53.8, 10.4]]},
    "BEANR": {"name": "Antwerp", "country": "BE", "region": "europe", "lat": 51.22, "lon": 4.40, "box": [[50.9, 4.0], [51.5, 4.8]]},
    "GBFXT": {"name": "Felixstowe", "country": "GB", "region": "europe", "lat": 51.95, "lon": 1.33, "box": [[51.7, 1.0], [52.2, 1.7]]},
    "FRLEH": {"name": "Le Havre", "country": "FR", "region": "europe", "lat": 49.49, "lon": 0.11, "box": [[49.2, -0.2], [49.8, 0.5]]},
    "GRPIR": {"name": "Piraeus", "country": "GR", "region": "europe", "lat": 37.94, "lon": 23.64, "box": [[37.7, 23.3], [38.2, 24.0]]},
    "ESVLC": {"name": "Valencia", "country": "ES", "region": "europe", "lat": 39.46, "lon": -0.31, "box": [[39.1, -0.6], [39.8, 0.0]]},
    "ITGOA": {"name": "Genoa", "country": "IT", "region": "europe", "lat": 44.41, "lon": 8.93, "box": [[44.1, 8.6], [44.7, 9.3]]},
    "SIKOP": {"name": "Koper", "country": "SI", "region": "europe", "lat": 45.55, "lon": 13.73, "box": [[45.3, 13.4], [45.8, 14.1]]},
    "ESALG": {"name": "Algeciras", "country": "ES", "region": "europe", "lat": 36.13, "lon": -5.45, "box": [[35.9, -5.7], [36.4, -5.1]]},

    # 북미
    "USLAX": {"name": "Los Angeles", "country": "US", "region": "namerica", "lat": 33.73, "lon": -118.25, "box": [[33.4, -118.6], [34.0, -117.9]]},
    "USLGB": {"name": "Long Beach", "country": "US", "region": "namerica", "lat": 33.77, "lon": -118.22, "box": [[33.5, -118.5], [34.0, -117.9]]},
    "USNYC": {"name": "New York", "country": "US", "region": "namerica", "lat": 40.67, "lon": -74.01, "box": [[40.3, -74.4], [40.9, -73.6]]},
    "USSAV": {"name": "Savannah", "country": "US", "region": "namerica", "lat": 32.08, "lon": -81.09, "box": [[31.8, -81.4], [32.4, -80.8]]},
    "CAVAN": {"name": "Vancouver", "country": "CA", "region": "namerica", "lat": 49.29, "lon": -123.11, "box": [[49.0, -123.5], [49.6, -122.7]]},
    "USMSY": {"name": "New Orleans", "country": "US", "region": "namerica", "lat": 29.95, "lon": -90.07, "box": [[29.6, -90.4], [30.2, -89.7]]},

    # 러시아·CIS
    "RUVVO": {"name": "Vladivostok", "country": "RU", "region": "ru-cis", "lat": 43.12, "lon": 131.89, "box": [[42.8, 131.5], [43.5, 132.3]]},
    "RUNVS": {"name": "Novorossiysk", "country": "RU", "region": "ru-cis", "lat": 44.72, "lon": 37.78, "box": [[44.4, 37.4], [44.9, 38.2]]},
    "KZAKT": {"name": "Aktau", "country": "KZ", "region": "ru-cis", "lat": 43.65, "lon": 51.18, "box": [[43.3, 50.8], [44.0, 51.6]]},

    # 아프리카·지중해
    "MACAS": {"name": "Casablanca", "country": "MA", "region": "africa", "lat": 33.59, "lon": -7.62, "box": [[33.3, -7.9], [33.9, -7.3]]},
    "KEMBA": {"name": "Mombasa", "country": "KE", "region": "africa", "lat": -4.05, "lon": 39.67, "box": [[-4.4, 39.3], [-3.7, 40.0]]},
    "ZADUR": {"name": "Durban", "country": "ZA", "region": "africa", "lat": -29.87, "lon": 31.03, "box": [[-30.2, 30.7], [-29.5, 31.4]]},
    "TZDAR": {"name": "Dar es Salaam", "country": "TZ", "region": "africa", "lat": -6.82, "lon": 39.29, "box": [[-7.1, 38.9], [-6.5, 39.6]]},
    "EGPSD": {"name": "Port Said", "country": "EG", "region": "africa", "lat": 31.25, "lon": 32.30, "box": [[30.9, 31.9], [31.5, 32.7]]},
}

# key: "{mmsi}_{port_code}" → latest observation within the collection window
vessel_snapshot: Dict[str, dict] = {}
PORT_INDEX = [(code, p["box"]) for code, p in PORTS.items()]


def find_port(lat: float, lon: float) -> str | None:
    for code, box in PORT_INDEX:
        min_lat, min_lon = box[0]
        max_lat, max_lon = box[1]
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return code
    return None


def classify_status(nav_status: int, speed: float) -> str | None:
    """
    배치형 스냅샷 분류.
    - berthed: 접안/정지 상태
    - anchored: 묘박/대기 상태
    """
    if nav_status == 5 or speed < 0.1:
        return "berthed"
    if nav_status == 1 or speed < 0.5:
        return "anchored"
    return None


def calc_snapshot_tpfs(anchored_count: int, berthed_count: int) -> float:
    """
    스냅샷용 간이 혼잡 점수 (0~100).
    상시 대기시간 추적 대신, 현재 대기 선박 수에 더 큰 가중치를 부여.
    """
    raw = anchored_count * SCORE_ANCHORED + berthed_count * SCORE_BERTHED
    return round(min(raw, 100.0), 1)


def tpfs_to_level(tpfs: float) -> str:
    if tpfs >= 75:
        return "CONGESTED"
    if tpfs >= 50:
        return "BUSY"
    if tpfs >= 25:
        return "STABLE"
    return "LOW"


def process_message(msg: dict):
    if msg.get("MessageType") != "PositionReport":
        return

    meta = msg.get("MetaData", {})
    pos = msg.get("Message", {}).get("PositionReport", {})

    mmsi = str(meta.get("MMSI", "")).strip()
    if not mmsi:
        return

    try:
        lat = float(meta.get("latitude", pos.get("Latitude", 0)))
        lon = float(meta.get("longitude", pos.get("Longitude", 0)))
        nav_status = int(pos.get("NavigationalStatus", -1))
        speed = float(pos.get("SpeedOverGround", 99))
    except (TypeError, ValueError):
        return

    if lat == 0 or lon == 0:
        return

    port_code = find_port(lat, lon)
    if not port_code:
        return

    state = classify_status(nav_status, speed)
    if state is None:
        return

    now = datetime.now(timezone.utc)
    key = f"{mmsi}_{port_code}"
    vessel_snapshot[key] = {
        "mmsi": mmsi,
        "port_code": port_code,
        "nav_status": nav_status,
        "speed": speed,
        "state": state,
        "last_seen": now,
    }


async def collect_snapshot():
    """COLLECT_SECONDS 동안 AIS 메시지를 수집하고 종료."""
    bounding_boxes = [p["box"] for p in PORTS.values()]
    subscribe_msg = {
        "APIKey": API_KEY,
        "BoundingBoxes": bounding_boxes,
        "FilterMessageTypes": ["PositionReport"],
    }

    deadline = asyncio.get_running_loop().time() + COLLECT_SECONDS
    backoff = 5

    while asyncio.get_running_loop().time() < deadline:
        try:
            remaining = max(1, int(deadline - asyncio.get_running_loop().time()))
            log.info("Connecting to aisstream.io... remaining collection window: %ss", remaining)

            async with websockets.connect(
                AISSTREAM_WS,
                ping_interval=30,
                ping_timeout=10,
                max_size=2**20,
            ) as ws:
                await ws.send(json.dumps(subscribe_msg))
                log.info("Subscribed to %s port zones", len(bounding_boxes))
                backoff = 5

                while asyncio.get_running_loop().time() < deadline:
                    timeout = max(1, deadline - asyncio.get_running_loop().time())
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        break

                    try:
                        msg = json.loads(raw)
                        process_message(msg)
                    except json.JSONDecodeError:
                        continue

        except websockets.exceptions.ConnectionClosed as e:
            log.warning("Connection closed: %s. Retrying in %ss", e, backoff)
        except Exception as e:
            log.error("Stream error: %s. Retrying in %ss", e, backoff)

        if asyncio.get_running_loop().time() < deadline:
            sleep_for = min(backoff, max(1, int(deadline - asyncio.get_running_loop().time())))
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 2, 60)


def build_rows(now: datetime) -> list[dict]:
    port_agg: Dict[str, dict] = {
        code: {"anchored": 0, "berthed": 0}
        for code in PORTS
    }

    fresh_cutoff = now.timestamp() - MAX_STALE_SECONDS

    for v in vessel_snapshot.values():
        if v["last_seen"].timestamp() < fresh_cutoff:
            continue

        code = v["port_code"]
        if v["state"] == "berthed":
            port_agg[code]["berthed"] += 1
        elif v["state"] == "anchored":
            port_agg[code]["anchored"] += 1

    rows = []
    for code in PORTS:
        anchored = port_agg[code]["anchored"]
        berthed = port_agg[code]["berthed"]
        tpfs = calc_snapshot_tpfs(anchored, berthed)

        rows.append(
            {
                "port_code": code,
                "updated_at": now.isoformat(),
                "vessels_anchored": anchored,
                "vessels_berthed": berthed,
                "avg_wait_hours": 0.0,
                "max_wait_hours": 0.0,
                "tpfs": tpfs,
                "level": tpfs_to_level(tpfs),
            }
        )

    return rows


def save_metrics(rows, now):
    # 최신 테이블 업데이트
    supabase.table("port_current").upsert(
        rows,
        on_conflict="port_code"
    ).execute()

    # 이력 테이블 저장
    history_rows = [
        {
            "port_code": r["port_code"],
            "snapshot_at": now.isoformat(),
            "vessels_anchored": r["vessels_anchored"],
            "vessels_berthed": r["vessels_berthed"],
            "avg_wait_hours": r["avg_wait_hours"],
            "max_wait_hours": r["max_wait_hours"],
            "tpfs": r["tpfs"],
            "level": r["level"],
        }
        for r in rows
    ]

    supabase.table("port_history").insert(history_rows).execute()

    congested = sum(1 for r in rows if r["level"] == "CONGESTED")
    busy = sum(1 for r in rows if r["level"] == "BUSY")
    stable = sum(1 for r in rows if r["level"] == "STABLE")
    low = sum(1 for r in rows if r["level"] == "LOW")

    log.info(
        "Saved %s ports. levels => congested=%s busy=%s stable=%s low=%s",
        len(rows), congested, busy, stable, low,
    )


async def main():
    log.info("MTL Port Congestion Snapshot Collector starting...")
    log.info(
        "Monitoring %s ports | collection window=%ss | stale cutoff=%ss",
        len(PORTS), COLLECT_SECONDS, MAX_STALE_SECONDS,
    )

    await collect_snapshot()

    now = datetime.now(timezone.utc)
    rows = build_rows(now)
    active_signals = sum(r["vessels_anchored"] + r["vessels_berthed"] for r in rows)

    log.info("Collection finished. Active vessel signals in snapshot: %s", active_signals)
    save_metrics(rows, now)


if __name__ == "__main__":
    asyncio.run(main())
