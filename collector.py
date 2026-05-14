"""
MTL Vessel Activity Index Collector
- aisstream.io WebSocket으로 글로벌 컨테이너 항만의 AIS PositionReport 수집
- 컨테이너선/잡화선 필터링하여 항만별 묘박/접안 선박 수 집계
- MVAI (MTL Vessel Activity Index, 내부 산식) 산출 후 Supabase 저장
- GitHub Actions cron으로 주기 실행

NOTE: DB 컬럼명은 기존 호환을 위해 'tpfs'로 유지. 표시상 명칭은 'MVAI'.
      이 지표는 항만 혼잡도(congestion)가 아닌 선박 활동도(activity)를 측정함.
      실제 접안 대기시간은 측정하지 않음.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import websockets
from supabase import Client, create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── 환경 변수 ────────────────────────────────────────────────
def _require_env(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise SystemExit(f"환경변수 {key}가 비어 있습니다. GitHub Secrets를 확인하세요.")
    return v

API_KEY = _require_env("AISSTREAM_API_KEY")
SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_SVC_KEY = _require_env("SUPABASE_SERVICE_KEY")

# 배치형 스냅샷 설정
COLLECT_SECONDS = int(os.getenv("COLLECT_SECONDS", "900"))      # 기본 15분
MAX_STALE_SECONDS = int(os.getenv("MAX_STALE_SECONDS", "300"))  # 5분 이상 안 보이면 제외
SCORE_ANCHORED = float(os.getenv("SCORE_ANCHORED", "6.0"))
SCORE_BERTHED = float(os.getenv("SCORE_BERTHED", "2.0"))

# 데이터 무결성 가드: 수집된 신호가 이 값보다 적으면 Supabase 저장 생략
MIN_SIGNALS_TO_SAVE = int(os.getenv("MIN_SIGNALS_TO_SAVE", "20"))

# Supabase 저장 재시도
SAVE_RETRY_MAX = 3

# AIS ShipType 필터: 컨테이너선(70~79), 잡화·탱커(80~89)
ALLOWED_SHIP_TYPES = set(range(70, 90))

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SVC_KEY)
AISSTREAM_WS = "wss://stream.aisstream.io/v0/stream"

# ─── 항만 정의 ────────────────────────────────────────────────
# box: [[min_lat, min_lon], [max_lat, max_lon]]
# 검증 기준: 2024년 컨테이너 처리량(TEU) + 권역별 대표성
# 인접 항만 박스 겹침은 find_port()에서 중심점 거리로 우선순위 처리
PORTS: Dict[str, dict] = {
    # ─── 한국·일본 (6개) ─────────────────────────────────────
    "KRPUS": {"name": "Busan", "country": "KR", "region": "kr-jp", "lat": 35.10, "lon": 129.04, "box": [[34.9, 128.7], [35.3, 129.3]]},
    "KRICN": {"name": "Incheon", "country": "KR", "region": "kr-jp", "lat": 37.45, "lon": 126.55, "box": [[37.3, 126.3], [37.6, 126.8]]},
    "JPNGO": {"name": "Nagoya", "country": "JP", "region": "kr-jp", "lat": 35.07, "lon": 136.88, "box": [[34.8, 136.5], [35.4, 137.2]]},
    "JPYOK": {"name": "Yokohama", "country": "JP", "region": "kr-jp", "lat": 35.45, "lon": 139.65, "box": [[35.2, 139.4], [35.7, 139.9]]},
    "JPTYO": {"name": "Tokyo", "country": "JP", "region": "kr-jp", "lat": 35.62, "lon": 139.77, "box": [[35.4, 139.5], [35.8, 140.1]]},
    "JPUKB": {"name": "Kobe", "country": "JP", "region": "kr-jp", "lat": 34.68, "lon": 135.20, "box": [[34.50, 134.95], [34.85, 135.45]]},

    # ─── 중국·홍콩·대만 (10개) ───────────────────────────────
    "CNSHA": {"name": "Shanghai", "country": "CN", "region": "china", "lat": 31.23, "lon": 121.47, "box": [[30.8, 121.0], [31.6, 122.0]]},
    "CNQIN": {"name": "Qingdao", "country": "CN", "region": "china", "lat": 36.07, "lon": 120.38, "box": [[35.8, 120.0], [36.3, 120.7]]},
    "CNNGB": {"name": "Ningbo", "country": "CN", "region": "china", "lat": 29.87, "lon": 121.55, "box": [[29.6, 121.2], [30.1, 122.0]]},
    "CNTXG": {"name": "Tianjin", "country": "CN", "region": "china", "lat": 38.98, "lon": 117.72, "box": [[38.7, 117.3], [39.2, 118.1]]},
    "CNYTN": {"name": "Yantian", "country": "CN", "region": "china", "lat": 22.57, "lon": 114.27, "box": [[22.3, 114.0], [22.8, 114.6]]},
    "CNNSA": {"name": "Nansha", "country": "CN", "region": "china", "lat": 22.77, "lon": 113.57, "box": [[22.5, 113.2], [23.0, 113.9]]},
    "CNDLC": {"name": "Dalian", "country": "CN", "region": "china", "lat": 38.91, "lon": 121.60, "box": [[38.6, 121.2], [39.1, 122.0]]},
    "HKHKG": {"name": "Hong Kong", "country": "HK", "region": "china", "lat": 22.31, "lon": 114.17, "box": [[22.15, 113.95], [22.50, 114.35]]},
    "CNXMN": {"name": "Xiamen", "country": "CN", "region": "china", "lat": 24.48, "lon": 118.08, "box": [[24.20, 117.80], [24.75, 118.35]]},
    "TWKHH": {"name": "Kaohsiung", "country": "TW", "region": "china", "lat": 22.62, "lon": 120.28, "box": [[22.40, 120.10], [22.80, 120.45]]},

    # ─── 동남아시아 (9개) ────────────────────────────────────
    "VNTOT": {"name": "Cai Mep", "country": "VN", "region": "sea", "lat": 10.52, "lon": 107.03, "box": [[10.2, 106.7], [10.8, 107.4]]},
    "VNHPH": {"name": "Haiphong", "country": "VN", "region": "sea", "lat": 20.87, "lon": 106.68, "box": [[20.6, 106.4], [21.1, 107.0]]},
    "THLCH": {"name": "Laem Chabang", "country": "TH", "region": "sea", "lat": 13.08, "lon": 100.88, "box": [[12.8, 100.6], [13.4, 101.2]]},
    "SGSIN": {"name": "Singapore", "country": "SG", "region": "sea", "lat": 1.26, "lon": 103.82, "box": [[0.9, 103.5], [1.6, 104.2]]},
    "MYLPK": {"name": "Port Klang", "country": "MY", "region": "sea", "lat": 3.00, "lon": 101.38, "box": [[2.7, 101.0], [3.3, 101.7]]},
    "MYTPP": {"name": "Tanjung Pelepas", "country": "MY", "region": "sea", "lat": 1.36, "lon": 103.53, "box": [[1.15, 103.35], [1.55, 103.70]]},
    "IDJKT": {"name": "Jakarta", "country": "ID", "region": "sea", "lat": -6.10, "lon": 106.88, "box": [[-6.4, 106.5], [-5.7, 107.2]]},
    "IDSUB": {"name": "Surabaya", "country": "ID", "region": "sea", "lat": -7.20, "lon": 112.73, "box": [[-7.5, 112.4], [-6.9, 113.1]]},
    "PHMNL": {"name": "Manila", "country": "PH", "region": "sea", "lat": 14.59, "lon": 120.97, "box": [[14.2, 120.6], [14.9, 121.3]]},

    # ─── 남아시아·중동 (9개) ─────────────────────────────────
    # NOTE: INBOM(Mumbai) 제거 - INNSA(Nhava Sheva/JNPT)와 동일 항만권. INNSA 박스를 확대해 Mumbai 본항도 커버.
    "LKCMB": {"name": "Colombo", "country": "LK", "region": "sa-me", "lat": 6.93, "lon": 79.85, "box": [[6.6, 79.5], [7.2, 80.2]]},
    "AEJEA": {"name": "Jebel Ali", "country": "AE", "region": "sa-me", "lat": 24.98, "lon": 55.06, "box": [[24.6, 54.7], [25.3, 55.4]]},
    "INNSA": {"name": "Nhava Sheva (JNPT/Mumbai)", "country": "IN", "region": "sa-me", "lat": 18.95, "lon": 72.95, "box": [[18.60, 72.50], [19.15, 73.15]]},
    "INMUN": {"name": "Mundra", "country": "IN", "region": "sa-me", "lat": 22.74, "lon": 69.70, "box": [[22.45, 69.40], [23.00, 69.95]]},
    "OMSLL": {"name": "Salalah", "country": "OM", "region": "sa-me", "lat": 16.95, "lon": 54.01, "box": [[16.75, 53.80], [17.15, 54.25]]},
    "SADMM": {"name": "Dammam", "country": "SA", "region": "sa-me", "lat": 26.43, "lon": 50.10, "box": [[26.15, 49.85], [26.70, 50.35]]},
    "QAHMD": {"name": "Hamad Port", "country": "QA", "region": "sa-me", "lat": 24.79, "lon": 51.61, "box": [[24.55, 51.35], [25.00, 51.85]]},
    "JOAQJ": {"name": "Aqaba", "country": "JO", "region": "sa-me", "lat": 29.52, "lon": 35.00, "box": [[29.2, 34.7], [29.8, 35.3]]},
    "ILASH": {"name": "Ashdod", "country": "IL", "region": "sa-me", "lat": 31.82, "lon": 34.65, "box": [[31.65, 34.45], [31.95, 34.85]]},

    # ─── 유럽 (10개) ─────────────────────────────────────────
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

    # ─── 북미 (8개) ─────────────────────────────────────────
    # NOTE: USLAX와 USLGB는 같은 San Pedro Bay에 위치해 박스를 동/서로 분리. find_port()의 거리 기반 선택으로 한번 더 보정됨.
    "USLAX": {"name": "Los Angeles", "country": "US", "region": "namerica", "lat": 33.73, "lon": -118.27, "box": [[33.60, -118.40], [33.85, -118.23]]},
    "USLGB": {"name": "Long Beach", "country": "US", "region": "namerica", "lat": 33.77, "lon": -118.20, "box": [[33.60, -118.23], [33.85, -118.05]]},
    "USNYC": {"name": "New York/NJ", "country": "US", "region": "namerica", "lat": 40.67, "lon": -74.01, "box": [[40.3, -74.4], [40.9, -73.6]]},
    "USSAV": {"name": "Savannah", "country": "US", "region": "namerica", "lat": 32.08, "lon": -81.09, "box": [[31.8, -81.4], [32.4, -80.8]]},
    "USHOU": {"name": "Houston", "country": "US", "region": "namerica", "lat": 29.73, "lon": -95.02, "box": [[29.40, -95.30], [29.85, -94.65]]},
    "USORF": {"name": "Norfolk/Virginia", "country": "US", "region": "namerica", "lat": 36.92, "lon": -76.32, "box": [[36.70, -76.55], [37.10, -76.05]]},
    "USMSY": {"name": "New Orleans", "country": "US", "region": "namerica", "lat": 29.95, "lon": -90.07, "box": [[29.75, -90.30], [30.10, -89.85]]},
    "CAVAN": {"name": "Vancouver", "country": "CA", "region": "namerica", "lat": 49.29, "lon": -123.11, "box": [[49.0, -123.5], [49.6, -122.7]]},

    # ─── 중남미 (8개) ───────────────────────────────────────
    "MXZLO": {"name": "Manzanillo", "country": "MX", "region": "latam", "lat": 19.05, "lon": -104.31, "box": [[18.85, -104.50], [19.20, -104.10]]},
    "BRSSZ": {"name": "Santos", "country": "BR", "region": "latam", "lat": -23.95, "lon": -46.33, "box": [[-24.20, -46.60], [-23.70, -46.00]]},
    "COCTG": {"name": "Cartagena", "country": "CO", "region": "latam", "lat": 10.39, "lon": -75.53, "box": [[10.15, -75.75], [10.60, -75.30]]},
    "PECLL": {"name": "Callao", "country": "PE", "region": "latam", "lat": -12.05, "lon": -77.15, "box": [[-12.30, -77.35], [-11.85, -76.95]]},
    "ECGYE": {"name": "Guayaquil", "country": "EC", "region": "latam", "lat": -2.28, "lon": -79.90, "box": [[-2.55, -80.10], [-2.05, -79.70]]},
    "COBUN": {"name": "Buenaventura", "country": "CO", "region": "latam", "lat": 3.88, "lon": -77.08, "box": [[3.65, -77.25], [4.05, -76.90]]},
    "PABLB": {"name": "Balboa", "country": "PA", "region": "latam", "lat": 8.95, "lon": -79.57, "box": [[8.75, -79.75], [9.10, -79.40]]},
    "PACLN": {"name": "Colon", "country": "PA", "region": "latam", "lat": 9.36, "lon": -79.90, "box": [[9.15, -80.10], [9.55, -79.70]]},

    # ─── 러시아·CIS (4개) ────────────────────────────────────
    "RULED": {"name": "Saint Petersburg", "country": "RU", "region": "ru-cis", "lat": 59.90, "lon": 30.25, "box": [[59.75, 29.95], [60.05, 30.45]]},
    "RUVVO": {"name": "Vladivostok", "country": "RU", "region": "ru-cis", "lat": 43.12, "lon": 131.89, "box": [[42.8, 131.5], [43.5, 132.3]]},
    "RUNVS": {"name": "Novorossiysk", "country": "RU", "region": "ru-cis", "lat": 44.72, "lon": 37.78, "box": [[44.4, 37.4], [44.9, 38.2]]},
    "KZAKT": {"name": "Aktau", "country": "KZ", "region": "ru-cis", "lat": 43.65, "lon": 51.18, "box": [[43.45, 50.95], [43.85, 51.40]]},

    # ─── 아프리카·지중해 (9개) ───────────────────────────────
    "MATNG": {"name": "Tanger Med", "country": "MA", "region": "africa", "lat": 35.89, "lon": -5.50, "box": [[35.70, -5.75], [36.05, -5.25]]},
    "MACAS": {"name": "Casablanca", "country": "MA", "region": "africa", "lat": 33.59, "lon": -7.62, "box": [[33.3, -7.9], [33.9, -7.3]]},
    "NGAPP": {"name": "Lagos/Apapa", "country": "NG", "region": "africa", "lat": 6.45, "lon": 3.38, "box": [[6.25, 3.15], [6.65, 3.60]]},
    "KEMBA": {"name": "Mombasa", "country": "KE", "region": "africa", "lat": -4.05, "lon": 39.67, "box": [[-4.4, 39.3], [-3.7, 40.0]]},
    "ZADUR": {"name": "Durban", "country": "ZA", "region": "africa", "lat": -29.87, "lon": 31.03, "box": [[-30.2, 30.7], [-29.5, 31.4]]},
    "TZDAR": {"name": "Dar es Salaam", "country": "TZ", "region": "africa", "lat": -6.82, "lon": 39.29, "box": [[-7.1, 38.9], [-6.5, 39.6]]},
    "EGPSD": {"name": "Port Said (West)", "country": "EG", "region": "africa", "lat": 31.25, "lon": 32.30, "box": [[31.10, 32.10], [31.40, 32.35]]},
    "EGEDK": {"name": "East Port Said (SCZONE)", "country": "EG", "region": "africa", "lat": 31.16, "lon": 32.35, "box": [[31.00, 32.35], [31.30, 32.65]]},
    "DJJIB": {"name": "Djibouti", "country": "DJ", "region": "africa", "lat": 11.60, "lon": 43.15, "box": [[11.40, 42.95], [11.85, 43.40]]},
}

# key: "{mmsi}_{port_code}" → latest observation within the collection window
vessel_snapshot: Dict[str, dict] = {}
# 데이터 품질 모니터링용
stats = {
    "messages_received": 0,
    "filtered_by_ship_type": 0,
    "filtered_outside_ports": 0,
    "filtered_by_status": 0,
    "accepted": 0,
}

PORT_INDEX = [(code, p["box"], p["lat"], p["lon"]) for code, p in PORTS.items()]


def _haversine_sq(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """근사 거리(제곱). 정확한 km는 필요 없고 비교용으로만 쓰므로 단순 유클리드 제곱."""
    return (lat1 - lat2) ** 2 + (lon1 - lon2) ** 2


def find_port(lat: float, lon: float) -> Optional[str]:
    """
    위경도가 어느 항만 박스에 들어가는지 판정.
    겹치는 박스가 여러 개면 항만 중심점까지의 거리가 가장 가까운 항만 선택.
    (특히 LA/Long Beach, Port Said West/East 같은 인접 항만 보정)
    """
    matched = []
    for code, box, plat, plon in PORT_INDEX:
        min_lat, min_lon = box[0]
        max_lat, max_lon = box[1]
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            matched.append((code, plat, plon))

    if not matched:
        return None
    if len(matched) == 1:
        return matched[0][0]

    matched.sort(key=lambda m: _haversine_sq(lat, lon, m[1], m[2]))
    return matched[0][0]


def classify_status(nav_status: int, speed: float) -> Optional[str]:
    """
    배치형 스냅샷 분류.
    - berthed: 접안/정지 (nav_status=5 Moored, 또는 속도 거의 0)
    - anchored: 묘박/대기 (nav_status=1 At Anchor, 또는 저속)
    """
    if nav_status == 5 or speed < 0.1:
        return "berthed"
    if nav_status == 1 or speed < 0.5:
        return "anchored"
    return None


def is_relevant_ship(ship_type: int) -> bool:
    """컨테이너선(70~79), 잡화·탱커(80~89)만 카운트."""
    return ship_type in ALLOWED_SHIP_TYPES


def calc_snapshot_tpfs(anchored_count: int, berthed_count: int) -> float:
    """
    MVAI 점수 산출 (0~100). DB 컬럼명 호환을 위해 함수명은 tpfs 유지.
    묘박 선박에 더 큰 가중치를 부여 (대기 상태가 활동도 높음 신호).
    
    한계: 항만 규모 정규화 없음. 대형항일수록 항상 높게 나타남.
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

    stats["messages_received"] += 1

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
        ship_type_raw = (
            pos.get("ShipType")
            or meta.get("ShipType")
            or msg.get("Message", {}).get("ShipStaticData", {}).get("Type")
        )
        ship_type = int(ship_type_raw) if ship_type_raw is not None else -1
    except (TypeError, ValueError):
        return

    if lat == 0 or lon == 0:
        return

    if ship_type != -1 and not is_relevant_ship(ship_type):
        stats["filtered_by_ship_type"] += 1
        return

    port_code = find_port(lat, lon)
    if not port_code:
        stats["filtered_outside_ports"] += 1
        return

    state = classify_status(nav_status, speed)
    if state is None:
        stats["filtered_by_status"] += 1
        return

    now = datetime.now(timezone.utc)
    key = f"{mmsi}_{port_code}"
    vessel_snapshot[key] = {
        "mmsi": mmsi,
        "port_code": port_code,
        "nav_status": nav_status,
        "speed": speed,
        "ship_type": ship_type,
        "state": state,
        "last_seen": now,
    }
    stats["accepted"] += 1


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


def save_metrics(rows: list[dict], now: datetime):
    """Supabase 저장. 실패 시 지수 백오프로 재시도."""
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

    last_err: Optional[Exception] = None
    for attempt in range(1, SAVE_RETRY_MAX + 1):
        try:
            supabase.table("port_current").upsert(
                rows,
                on_conflict="port_code",
            ).execute()
            supabase.table("port_history").insert(history_rows).execute()
            break
        except Exception as e:
            last_err = e
            log.warning("Supabase save attempt %s/%s failed: %s", attempt, SAVE_RETRY_MAX, e)
            if attempt < SAVE_RETRY_MAX:
                time.sleep(5 * attempt)
    else:
        log.error("Supabase save permanently failed: %s", last_err)
        raise last_err

    congested = sum(1 for r in rows if r["level"] == "CONGESTED")
    busy = sum(1 for r in rows if r["level"] == "BUSY")
    stable = sum(1 for r in rows if r["level"] == "STABLE")
    low = sum(1 for r in rows if r["level"] == "LOW")

    log.info(
        "Saved %s ports. levels => congested=%s busy=%s stable=%s low=%s",
        len(rows), congested, busy, stable, low,
    )


def log_data_quality(active_signals: int):
    total = stats["messages_received"]
    if total == 0:
        log.warning("No AIS messages received during the collection window.")
        return

    pct_ship = stats["filtered_by_ship_type"] / total * 100
    pct_outside = stats["filtered_outside_ports"] / total * 100
    pct_status = stats["filtered_by_status"] / total * 100
    pct_accept = stats["accepted"] / total * 100

    log.info(
        "Data quality | total=%s | filtered: ship_type=%s(%.1f%%) outside=%s(%.1f%%) status=%s(%.1f%%) | accepted=%s(%.1f%%) | active_signals=%s",
        total,
        stats["filtered_by_ship_type"], pct_ship,
        stats["filtered_outside_ports"], pct_outside,
        stats["filtered_by_status"], pct_status,
        stats["accepted"], pct_accept,
        active_signals,
    )


async def main():
    log.info("MTL Port Congestion Snapshot Collector starting...")
    log.info(
        "Monitoring %s ports | collection window=%ss | stale cutoff=%ss | min signals to save=%s",
        len(PORTS), COLLECT_SECONDS, MAX_STALE_SECONDS, MIN_SIGNALS_TO_SAVE,
    )

    await collect_snapshot()

    now = datetime.now(timezone.utc)
    rows = build_rows(now)
    active_signals = sum(r["vessels_anchored"] + r["vessels_berthed"] for r in rows)

    log.info("Collection finished. Active vessel signals in snapshot: %s", active_signals)
    log_data_quality(active_signals)

    if active_signals < MIN_SIGNALS_TO_SAVE:
        log.error(
            "Active signals (%s) below threshold (%s). Skipping save to preserve previous data.",
            active_signals, MIN_SIGNALS_TO_SAVE,
        )
        return

    save_metrics(rows, now)


if __name__ == "__main__":
    asyncio.run(main())
