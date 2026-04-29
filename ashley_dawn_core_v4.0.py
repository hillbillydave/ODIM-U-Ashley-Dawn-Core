#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ashley Dawn Core v4.0
ODIM-U Informational-Geometry Weather Engine

Author: David E. Blackwell
Hillbilly Storm Chasers Research Division

Concept:
    - Treat the atmosphere as an informational manifold.
    - Compute entropic stress, processing flow, and dilation proxies.
    - Fuse classical weather, geomagnetic stress, and ODIM-U metrics.
    - Provide early warnings and a continental-scale informational map.

This file is designed as a clean, extensible backbone:
    - Safe to run as-is (with an API key if you enable OpenWeather).
    - Easy to extend with ODIM-U v4.0, QSTF, warp-drive coupling, etc.
"""

import os
import math
import time
import json
import sqlite3
import datetime
from datetime import timezone

import requests

# ============================================================
# CONFIGURATION
# ============================================================

DB_PATH = "ashley_memory.db"
FORECAST_DIR = "ashley_forecasts"
os.makedirs(FORECAST_DIR, exist_ok=True)

USE_OPEN_METEO = False          # If you want to wire Open-Meteo later
OPENWEATHER_API_KEY = ""        # Set if you want live OpenWeather data

REQUEST_HEADERS = {
    "User-Agent": "ODIM-U_Ashley-Dawn-Core_v4.0 (contact: your_email@example.com)"
}

# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def init_memory():
    """
    Initialize or upgrade the Ashley memory database.
    Adds columns if they don't exist yet.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS weather_logs (
            dt              DATETIME,
            location        TEXT,
            pressure        REAL,
            temp            REAL,
            humidity        REAL,
            wind_speed      REAL,
            wind_deg        REAL,
            precip_prob     REAL,
            snowfall        REAL,
            snow_depth      REAL,
            cape            REAL,
            risk            REAL,
            kp              REAL,
            bz              REAL,
            entropic_proxy  REAL,
            dilation_proxy  REAL
        )
    """)

    # Ensure all columns exist (for older DBs)
    c.execute("PRAGMA table_info(weather_logs)")
    columns = [row[1] for row in c.fetchall()]
    needed = [
        "wind_speed", "wind_deg", "precip_prob", "snowfall", "snow_depth",
        "cape", "risk", "kp", "bz", "entropic_proxy", "dilation_proxy"
    ]
    for col in needed:
        if col not in columns:
            c.execute(f"ALTER TABLE weather_logs ADD COLUMN {col} REAL")

    conn.commit()
    return conn

# ============================================================
# GEOPHYSICAL DATA HANDLERS
# ============================================================

def get_recent_earthquakes(min_magnitude=2.5, time_period="hour"):
    """
    Pull recent USGS earthquakes as a proxy for crustal stress.
    """
    feeds = {
        "hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson",
        "day":  "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson",
    }
    url = feeds.get(time_period, feeds["hour"])

    try:
        resp = requests.get(url, timeout=15, headers=REQUEST_HEADERS)
        data = resp.json()
        quakes = []
        for feature in data.get("features", [])[:10]:
            props = feature["properties"]
            coords = feature["geometry"]["coordinates"]
            quakes.append({
                "mag":   props.get("mag", 0),
                "place": props.get("place", "Unknown"),
                "time":  datetime.datetime.fromtimestamp(
                    props.get("time", 0) / 1000, tz=timezone.utc
                ),
                "lat":   coords[1],
                "lon":   coords[0],
                "depth_km": coords[2],
            })
        return quakes
    except Exception as e:
        print(f"[USGS_EQ_ERROR] {e}")
        return []

def get_geomagnetic_data():
    """
    Pull Kp and Bz as proxies for magnetospheric stress.
    """
    try:
        kp_url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
        kp_resp = requests.get(kp_url, timeout=10, headers=REQUEST_HEADERS)
        kp_data = kp_resp.json()[-1] if kp_resp.ok else {}
        kp = float(kp_data[1]) if len(kp_data) > 1 and kp_data[1] is not None else 0.0

        bz_url = "https://services.swpc.noaa.gov/text/ace-swepam.txt"
        bz_resp = requests.get(bz_url, timeout=10, headers=REQUEST_HEADERS)
        bz = 0.0
        if bz_resp.ok:
            lines = bz_resp.text.splitlines()
            for line in reversed(lines):
                if line.strip() and not line.startswith(("#", ":")):
                    parts = line.split()
                    if len(parts) >= 8 and parts[7] != "999.9":
                        bz = float(parts[7])
                        break

        return {"kp": kp, "bz": bz}
    except Exception as e:
        print(f"[GEO_ERROR] {e}")
        return {"kp": 0.0, "bz": 0.0}

# ============================================================
# ODIM-U / ASHLEY INTELLIGENCE CORE
# ============================================================

class AshleyIntelligence:
    """
    Core intelligence layer:
        - CAPE-like proxy (Ashley CAPE)
        - Entropic proxy (ODIM-U v4.0 hook)
        - Dilation proxy (time-stretch estimate)
        - Risk classification
    """

    def __init__(self, conn):
        self.conn = conn
        self.outlooks = {
            "A": "Settled_Fine",
            "B": "Fine_Weather",
            "C": "Becoming_Fine",
            "D": "Fine_Becoming_Less_Settled",
            "E": "Showers_Likely",
            "Z": "Stormy_High_Volatility_Watch_Closely",
            "T": "Tornado_Risk_Elevated",
            "S": "Significant_Snow_Expected",
        }

    # -------------------------------
    # Ashley CAPE (effective CAPE)
    # -------------------------------
    def calculate_ashley_cape(
        self,
        temp_c,
        humidity,
        wind_speed,
        precip_prob,
        pressure_delta=0.0,
        kp=0.0,
        bz=0.0,
        nearby_quakes=0
    ):
        """
        Effective CAPE proxy that folds in:
            - thermal buoyancy
            - moisture
            - shear
            - trigger (precip probability)
            - pressure tendency
            - geomagnetic stress
            - seismic agitation
        """
        if temp_c < 5:
            return 0.0

        thermal_base = max(0.0, (temp_c - 5.0) / 30.0)
        thermal = thermal_base ** 3.0

        if temp_c > 15:
            moisture = min(1.0, humidity / 90.0)
        else:
            moisture = min(0.7, humidity / 100.0)

        shear = min(0.8, wind_speed / 40.0)
        trigger = min(0.8, precip_prob / 150.0)
        pressure_boost = max(0.0, -pressure_delta / 5.0)

        geo_factor = 0.0
        if kp >= 7:
            geo_factor += (kp - 6) * 0.3
        if bz < -15:
            geo_factor += (abs(bz) - 10) / 100.0

        quake_factor = min(0.3, nearby_quakes * 0.05)

        base = (moisture + thermal) * 0.8
        multipliers = 1 + shear + trigger + pressure_boost + geo_factor + quake_factor
        raw = base * multipliers

        ecape = min(1800.0, math.log1p(raw * 5.0) * 600.0)
        if temp_c < 10:
            ecape *= 0.5

        return round(ecape, 1)

    # -------------------------------
    # ODIM-U v4.0: Entropic Proxy
    # -------------------------------
    def compute_entropic_proxy(self, cape, kp, bz, humidity, temp_c):
        """
        Entropic stress proxy:
            - Higher CAPE -> higher informational complexity
            - Strong geomagnetic disturbance -> extra manifold stress
            - Humidity + warm temps -> microstate explosion (mist, storms)
        This is a scalar stand-in for S_rel density.
        """
        # Normalize inputs
        cape_norm = min(1.0, cape / 2000.0)
        kp_norm = min(1.0, kp / 9.0)
        bz_norm = max(0.0, min(1.0, abs(min(bz, 0.0)) / 20.0))
        hum_norm = min(1.0, humidity / 100.0)
        temp_norm = min(1.0, max(0.0, (temp_c + 10.0) / 40.0))

        # Weighted combination (tunable)
        entropic = (
            0.45 * cape_norm +
            0.20 * kp_norm +
            0.15 * bz_norm +
            0.10 * hum_norm +
            0.10 * temp_norm
        )

        # Scale to a more interpretable range (e.g., 0–100)
        return round(entropic * 100.0, 2)

    # -------------------------------
    # ODIM-U v4.0: Dilation Proxy
    # -------------------------------
    def compute_dilation_proxy(self, entropic_proxy):
        """
        Dilation proxy (ps/day or arbitrary units):
            - Higher entropic stress -> slower effective "rendering speed"
            - We model dilation as a nonlinear function of entropic load.
        """
        # Map entropic_proxy (0–100) to a fractional slowdown
        x = entropic_proxy / 100.0
        # Nonlinear amplification near high stress
        slowdown_factor = 1.0 + 4.0 * (x ** 3)

        # Base dilation scale (tunable). For now, treat as ps/day.
        base_ps_per_day = 20.0
        dilation = base_ps_per_day * slowdown_factor
        return round(dilation, 3)

    # -------------------------------
    # Risk Classification
    # -------------------------------
    def classify_risk(self, cape, entropic_proxy, temp_c, precip_prob, snowfall):
        """
        Map CAPE + entropic stress + precip/snow into a discrete risk code.
        """
        # Snow risk
        if snowfall > 0.5 and temp_c <= 0:
            return "S"

        # Tornado / severe convective risk
        if cape > 1200 and entropic_proxy > 60 and precip_prob > 60:
            return "T"

        # High volatility storms
        if cape > 800 and entropic_proxy > 50:
            return "Z"

        # Showers likely
        if precip_prob > 50 or cape > 300:
            return "E"

        # Fine / improving
        if cape < 100 and entropic_proxy < 30 and precip_prob < 20:
            return "A"

        # Default mild unsettled
        return "D"

    # -------------------------------
    # Logging
    # -------------------------------
    def log_weather(
        self,
        dt,
        location,
        pressure,
        temp,
        humidity,
        wind_speed,
        wind_deg,
        precip_prob,
        snowfall,
        snow_depth,
        cape,
        risk,
        kp,
        bz,
        entropic_proxy,
        dilation_proxy
    ):
        c = self.conn.cursor()
        c.execute("""
            INSERT INTO weather_logs (
                dt, location, pressure, temp, humidity,
                wind_speed, wind_deg, precip_prob,
                snowfall, snow_depth, cape, risk,
                kp, bz, entropic_proxy, dilation_proxy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            dt, location, pressure, temp, humidity,
            wind_speed, wind_deg, precip_prob,
            snowfall, snow_depth, cape, risk,
            kp, bz, entropic_proxy, dilation_proxy
        ))
        self.conn.commit()

# ============================================================
# STORM PROJECTION HELPERS
# ============================================================

def project_path(lat, lon, bearing, speed_mph, hours=1.0):
    """
    Simple great-circle projection for storm motion.
    """
    R = 3958.8  # Earth radius in miles
    distance_miles = speed_mph * hours

    lat1, lon1, brng = map(math.radians, [lat, lon, bearing])
    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_miles / R) +
        math.cos(lat1) * math.sin(distance_miles / R) * math.cos(brng)
    )
    lon2 = lon1 + math.atan2(
        math.sin(brng) * math.sin(distance_miles / R) * math.cos(lat1),
        math.cos(distance_miles / R) - math.sin(lat1) * math.sin(lat2)
    )
    return math.degrees(lat2), (math.degrees(lon2) + 180) % 360 - 180

def haversine(lat1, lon1, lat2, lon2):
    """
    Distance between two lat/lon points in miles.
    """
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 1)

# ============================================================
# CITY LIST PLACEHOLDER
# ============================================================

# NOTE: Insert your full sentinel city list here.
# For now, we leave a placeholder so you can paste the existing list
# from your v3.0 paper or GitHub repo.

CITY_LIST = [
    # ("City_Name, ST", lat, lon),
    # e.g. ("Omaha, NE", 41.26, -96.01),
    # TODO: PASTE FULL CITY LIST HERE
]

# ============================================================
# WEATHER FETCHING (SIMPLE OPENWEATHER WRAPPER)
# ============================================================

def fetch_openweather(lat, lon):
    """
    Minimal OpenWeather fetch for a single point.
    You can replace or augment this with Open-Meteo or your own feeds.
    """
    if not OPENWEATHER_API_KEY:
        raise RuntimeError("OPENWEATHER_API_KEY not set.")

    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    resp = requests.get(url, timeout=10, headers=REQUEST_HEADERS)
    data = resp.json()

    main = data.get("main", {})
    wind = data.get("wind", {})
    rain = data.get("rain", {})
    snow = data.get("snow", {})

    pressure = main.get("pressure", 1013.0)
    temp = main.get("temp", 15.0)
    humidity = main.get("humidity", 50.0)
    wind_speed = wind.get("speed", 5.0)
    wind_deg = wind.get("deg", 0.0)

    precip_prob = 0.0
    if "1h" in rain:
        precip_prob = min(100.0, rain["1h"] * 50.0)
    snowfall = snow.get("1h", 0.0)
    snow_depth = snowfall  # placeholder

    return {
        "pressure": pressure,
        "temp": temp,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "wind_deg": wind_deg,
        "precip_prob": precip_prob,
        "snowfall": snowfall,
        "snow_depth": snow_depth,
    }

# ============================================================
# CORE SCAN LOOP
# ============================================================

def run_scan():
    """
    Single full-network scan:
        - Pull geomagnetic + quake context
        - For each city:
            - Fetch weather
            - Compute CAPE, entropic proxy, dilation proxy, risk
            - Log to DB
            - Save a simple JSON forecast snapshot
    """
    conn = init_memory()
    ashley = AshleyIntelligence(conn)

    geo = get_geomagnetic_data()
    kp, bz = geo["kp"], geo["bz"]

    quakes = get_recent_earthquakes(time_period="day")
    quake_count = len(quakes)

    now = datetime.datetime.now(timezone.utc)

    for city_name, lat, lon in CITY_LIST:
        try:
            wx = fetch_openweather(lat, lon)

            pressure = wx["pressure"]
            temp = wx["temp"]
            humidity = wx["humidity"]
            wind_speed = wx["wind_speed"]
            wind_deg = wx["wind_deg"]
            precip_prob = wx["precip_prob"]
            snowfall = wx["snowfall"]
            snow_depth = wx["snow_depth"]

            # For now, we don't track pressure tendency; set 0
            pressure_delta = 0.0

            cape = ashley.calculate_ashley_cape(
                temp_c=temp,
                humidity=humidity,
                wind_speed=wind_speed,
                precip_prob=precip_prob,
                pressure_delta=pressure_delta,
                kp=kp,
                bz=bz,
                nearby_quakes=quake_count
            )

            entropic_proxy = ashley.compute_entropic_proxy(
                cape=cape,
                kp=kp,
                bz=bz,
                humidity=humidity,
                temp_c=temp
            )

            dilation_proxy = ashley.compute_dilation_proxy(entropic_proxy)

            risk_code = ashley.classify_risk(
                cape=cape,
                entropic_proxy=entropic_proxy,
                temp_c=temp,
                precip_prob=precip_prob,
                snowfall=snowfall
            )

            ashley.log_weather(
                dt=now.isoformat(),
                location=city_name,
                pressure=pressure,
                temp=temp,
                humidity=humidity,
                wind_speed=wind_speed,
                wind_deg=wind_deg,
                precip_prob=precip_prob,
                snowfall=snowfall,
                snow_depth=snow_depth,
                cape=cape,
                risk=risk_code,
                kp=kp,
                bz=bz,
                entropic_proxy=entropic_proxy,
                dilation_proxy=dilation_proxy
            )

            # Save a simple forecast snapshot
            snapshot = {
                "timestamp": now.isoformat(),
                "city": city_name,
                "lat": lat,
                "lon": lon,
                "pressure": pressure,
                "temp": temp,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "wind_deg": wind_deg,
                "precip_prob": precip_prob,
                "snowfall": snowfall,
                "snow_depth": snow_depth,
                "cape": cape,
                "risk_code": risk_code,
                "kp": kp,
                "bz": bz,
                "entropic_proxy": entropic_proxy,
                "dilation_proxy": dilation_proxy,
            }

            safe_name = city_name.replace(" ", "_").replace(",", "")
            out_path = os.path.join(
                FORECAST_DIR,
                f"{safe_name}_{now.strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2)

            print(f"[OK] {city_name}: CAPE={cape}, Entropic={entropic_proxy}, "
                  f"Dilation={dilation_proxy}, Risk={risk_code}")

        except Exception as e:
            print(f"[CITY_ERROR] {city_name}: {e}")

    conn.close()

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Ashley Dawn Core v4.0 — ODIM-U Informational Weather Engine")
    print("Starting single scan of sentinel network...")
    run_scan()
    print("Scan complete.")
