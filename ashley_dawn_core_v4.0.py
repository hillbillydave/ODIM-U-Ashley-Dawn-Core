#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ashley Dawn Core – ODIM-U v3.x
Informational-geometry-based atmospheric engine for:
- national weather scanning
- CAPE-style instability diagnostics
- storm-path projection
- basic entropic / dilation proxies

This file is designed as a single, self-contained core module.
"""

import os
import math
import time
import json
import sqlite3
import datetime
from datetime import timezone

import requests

# =========================
# CONFIGURATION
# =========================

DB_PATH = "ashley_memory.db"
FORECAST_DIR = "ashley_forecasts"
os.makedirs(FORECAST_DIR, exist_ok=True)

# If True, use Open-Meteo; if False, use OpenWeather
USE_OPEN_METEO = False
OPENWEATHER_API_KEY = ""  # fill if using OpenWeather

REQUEST_HEADERS = {
    "User-Agent": "ODIM-U_Ashley_Dawn_Core_v3.x (contact: your_email@example.com)"
}

# =========================
# DATABASE INITIALIZATION
# =========================

def init_memory():
    """Initialize or upgrade the SQLite database schema."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            location TEXT,
            pressure REAL,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            wind_deg REAL,
            precip_prob REAL,
            snowfall REAL,
            snow_depth REAL,
            cape REAL,
            risk REAL,
            kp REAL,
            bz REAL,
            entropic_proxy REAL,
            dilation_proxy REAL
        )
        """
    )

    # Ensure all expected columns exist (for older DBs)
    c.execute("PRAGMA table_info(weather_logs)")
    columns = [row[1] for row in c.fetchall()]
    missing = [
        "wind_speed",
        "wind_deg",
        "precip_prob",
        "snowfall",
        "snow_depth",
        "cape",
        "risk",
        "kp",
        "bz",
        "entropic_proxy",
        "dilation_proxy",
    ]
    for col in missing:
        if col not in columns:
            c.execute(f"ALTER TABLE weather_logs ADD COLUMN {col} REAL")

    conn.commit()
    return conn

# =========================
# GEOPHYSICAL DATA HANDLERS
# =========================

def get_recent_earthquakes(min_magnitude=2.5, time_period="hour"):
    """Pull recent USGS earthquakes as a crude proxy for crustal stress."""
    feeds = {
        "hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson",
        "day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson",
    }
    url = feeds.get(time_period, feeds["hour"])

    try:
        resp = requests.get(url, timeout=15, headers=REQUEST_HEADERS)
        data = resp.json()
        quakes = []
        for feature in data.get("features", [])[:10]:
            props = feature["properties"]
            coords = feature["geometry"]["coordinates"]
            quakes.append(
                {
                    "mag": props.get("mag", 0),
                    "place": props.get("place", "Unknown"),
                    "time": datetime.datetime.fromtimestamp(
                        props.get("time", 0) / 1000, tz=timezone.utc
                    ),
                    "lat": coords[1],
                    "lon": coords[0],
                    "depth_km": coords[2],
                }
            )
        return quakes
    except Exception as e:
        print(f"[USGS_EQU_ERROR] {e}")
        return []


def get_geomagnetic_data():
    """
    Pull simple geomagnetic proxies:
    - Kp index (planetary)
    - Bz (IMF z-component)
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

# =========================
# ODIM-U ENTROPIC ENGINE
# =========================

class ODIMUEngine:
    """
    Minimal ODIM-U-style engine for entropic and dilation proxies.
    This is not the full field theory—just a diagnostic layer that
    echoes the monograph’s structure.
    """

    def __init__(self):
        # Physical constants (SI)
        self.kb = 1.380649e-23
        self.hbar = 1.0545718e-34
        self.c = 299_792_458.0

        # Blackwell Limit (rough order-of-magnitude placeholder)
        self.SB = 1.0e43  # bits/m^3 at Planck scale (conceptual)

    def compute_entropic_proxy(self, cape, kp, bz):
        """
        Very simplified 'entropy proxy':
        - higher CAPE, higher S_rel
        - strong geomagnetic disturbance nudges it upward
        """
        cape_term = max(0.0, cape) / 2000.0  # normalize ~0–1
        kp_term = max(0.0, kp) / 9.0
        bz_term = max(0.0, -bz) / 20.0  # southward Bz increases stress

        s_rel = (cape_term * 0.7) + (kp_term * 0.2) + (bz_term * 0.1)
        # scale to something like 0–50 for logging
        return s_rel * 50.0

    def compute_dilation_proxy(self, entropic_proxy):
        """
        Map entropic proxy to an effective 'dilation' in ps/day.
        This is a toy mapping, just to keep the conceptual link.
        """
        # treat entropic_proxy as fraction of SB in a tiny volume
        frac = min(1.0, entropic_proxy / 50.0)
        # simple nonlinear mapping
        dilation_ps_per_day = 10.0 + 40.0 * (frac**2)
        return dilation_ps_per_day


# =========================
# ASHLEY INTELLIGENCE CORE
# =========================

class AshleyIntelligence:
    """
    Core diagnostic engine:
    - computes Ashley-style CAPE
    - classifies risk
    - logs to SQLite
    - ties in ODIM-U entropic/dilation proxies
    """

    def __init__(self, conn):
        self.conn = conn
        self.odim = ODIMUEngine()

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

    # ---------- CAPE / Instability ----------

    def calculate_ashley_cape(
        self,
        temp_c,
        humidity_pct,
        wind_speed_mps,
        precip_prob_pct,
        pressure_delta_hpa=0.0,
        kp=0.0,
        bz=0.0,
        nearby_quakes=0,
    ):
        """
        Ashley-style CAPE proxy:
        - thermal + moisture base
        - shear, trigger, pressure, geomagnetic, and quake multipliers
        """
        # crude temperature gate
        if temp_c < 5.0:
            return 0.0

        thermal_base = max(0.0, (temp_c - 5.0) / 30.0)
        thermal = thermal_base**3.0

        if temp_c > 15.0:
            moisture = min(1.0, humidity_pct / 90.0)
        else:
            moisture = min(0.7, humidity_pct / 100.0)

        shear = min(0.8, wind_speed_mps / 18.0)  # ~40 mph
        trigger = min(0.8, precip_prob_pct / 150.0)
        pressure_boost = max(0.0, -pressure_delta_hpa / 5.0)

        geo_factor = 0.0
        if kp >= 7:
            geo_factor += (kp - 6) * 0.3
        if bz < -15:
            geo_factor += (abs(bz) - 10) / 100.0

        quake_factor = min(0.3, nearby_quakes * 0.05)

        base = (moisture + thermal) * 0.8
        multipliers = 1.0 + shear + trigger + pressure_boost + geo_factor + quake_factor
        raw = base * multipliers

        ecape = min(1800.0, math.log1p(raw * 5.0) * 600.0)
        if temp_c < 10.0:
            ecape *= 0.5

        return round(ecape, 1)

    def classify_risk(self, cape, temp_c, precip_prob_pct, snowfall_cm):
        """
        Map CAPE + context to a coarse risk code.
        """
        if snowfall_cm >= 5.0 and temp_c <= 0.0:
            return "S"  # significant snow

        if cape >= 1500.0 and precip_prob_pct >= 60.0:
            return "T"  # tornado / severe

        if cape >= 800.0 and precip_prob_pct >= 40.0:
            return "Z"  # stormy / high volatility

        if cape >= 400.0:
            return "E"  # showers likely / convective

        if cape >= 150.0:
            return "D"  # becoming less settled

        if cape >= 50.0:
            return "C"  # becoming fine

        if cape > 0.0:
            return "B"  # fine weather

        return "A"  # settled fine

    # ---------- Logging ----------

    def log_weather(
        self,
        location,
        pressure_hpa,
        temp_c,
        humidity_pct,
        wind_speed_mps,
        wind_deg,
        precip_prob_pct,
        snowfall_cm,
        snow_depth_cm,
        cape,
        risk_code,
        kp,
        bz,
        entropic_proxy,
        dilation_proxy,
    ):
        c = self.conn.cursor()
        c.execute(
            """
            INSERT INTO weather_logs (
                timestamp, location, pressure, temp, humidity,
                wind_speed, wind_deg, precip_prob, snowfall, snow_depth,
                cape, risk, kp, bz, entropic_proxy, dilation_proxy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.datetime.now(tz=timezone.utc).isoformat(),
                location,
                pressure_hpa,
                temp_c,
                humidity_pct,
                wind_speed_mps,
                wind_deg,
                precip_prob_pct,
                snowfall_cm,
                snow_depth_cm,
                cape,
                self.outlooks.get(risk_code, "Unknown"),
                kp,
                bz,
                entropic_proxy,
                dilation_proxy,
            ),
        )
        self.conn.commit()

    # ---------- Forecast / Scan Helpers ----------

    def fetch_weather_for_city(self, name, lat, lon):
        """
        Fetch basic weather for a city using either Open-Meteo or OpenWeather.
        Returns a dict with the fields we care about.
        """
        if USE_OPEN_METEO:
            url = (
                "https://api.open-meteo.com/v1/forecast"
                f"?latitude={lat}&longitude={lon}"
                "&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,"
                "snowfall,pressure_msl,windspeed_10m,winddirection_10m,snow_depth"
                "&forecast_days=1&timezone=UTC"
            )
            resp = requests.get(url, timeout=15, headers=REQUEST_HEADERS)
            data = resp.json()
            hourly = data.get("hourly", {})
            if not hourly:
                raise RuntimeError("No hourly data from Open-Meteo")

            # take the first hour as "now" proxy
            temp_c = float(hourly["temperature_2m"][0])
            humidity = float(hourly["relativehumidity_2m"][0])
            precip_prob = float(hourly.get("precipitation_probability", [0])[0])
            snowfall = float(hourly.get("snowfall", [0])[0])
            pressure = float(hourly.get("pressure_msl", [1013])[0])
            wind_speed = float(hourly.get("windspeed_10m", [0])[0])
            wind_deg = float(hourly.get("winddirection_10m", [0])[0])
            snow_depth = float(hourly.get("snow_depth", [0])[0])

        else:
            if not OPENWEATHER_API_KEY:
                raise RuntimeError("OPENWEATHER_API_KEY not set and USE_OPEN_METEO=False")

            url = (
                "https://api.openweathermap.org/data/2.5/weather"
                f"?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
            )
            resp = requests.get(url, timeout=15, headers=REQUEST_HEADERS)
            data = resp.json()

            main = data.get("main", {})
            wind = data.get("wind", {})
            weather = data.get("weather", [{}])[0]

            temp_c = float(main.get("temp", 15.0))
            humidity = float(main.get("humidity", 60.0))
            pressure = float(main.get("pressure", 1013.0))
            wind_speed = float(wind.get("speed", 0.0))
            wind_deg = float(wind.get("deg", 0.0))

            # crude proxies
            precip_prob = 60.0 if "rain" in weather.get("main", "").lower() else 10.0
            snowfall = 0.0
            snow_depth = 0.0

        return {
            "name": name,
            "lat": lat,
            "lon": lon,
            "temp_c": temp_c,
            "humidity": humidity,
            "pressure": pressure,
            "wind_speed": wind_speed,
            "wind_deg": wind_deg,
            "precip_prob": precip_prob,
            "snowfall": snowfall,
            "snow_depth": snow_depth,
        }

    def build_outlook_for_city(self, city, kp, bz, nearby_quakes_count=0):
        """
        Full pipeline for a single city:
        - fetch weather
        - compute CAPE
        - classify risk
        - compute entropic + dilation proxies
        - log to DB
        - return a compact summary dict
        """
        name, lat, lon = city
        wx = self.fetch_weather_for_city(name, lat, lon)

        # simple pressure delta placeholder (could be from history)
        pressure_delta = 0.0

        cape = self.calculate_ashley_cape(
            temp_c=wx["temp_c"],
            humidity_pct=wx["humidity"],
            wind_speed_mps=wx["wind_speed"],
            precip_prob_pct=wx["precip_prob"],
            pressure_delta_hpa=pressure_delta,
            kp=kp,
            bz=bz,
            nearby_quakes=nearby_quakes_count,
        )

        risk_code = self.classify_risk(
            cape=cape,
            temp_c=wx["temp_c"],
            precip_prob_pct=wx["precip_prob"],
            snowfall_cm=wx["snowfall"],
        )

        entropic_proxy = self.odim.compute_entropic_proxy(cape=cape, kp=kp, bz=bz)
        dilation_proxy = self.odim.compute_dilation_proxy(entropic_proxy)

        self.log_weather(
            location=name,
            pressure_hpa=wx["pressure"],
            temp_c=wx["temp_c"],
            humidity_pct=wx["humidity"],
            wind_speed_mps=wx["wind_speed"],
            wind_deg=wx["wind_deg"],
            precip_prob_pct=wx["precip_prob"],
            snowfall_cm=wx["snowfall"],
            snow_depth_cm=wx["snow_depth"],
            cape=cape,
            risk_code=risk_code,
            kp=kp,
            bz=bz,
            entropic_proxy=entropic_proxy,
            dilation_proxy=dilation_proxy,
        )

        return {
            "city": name,
            "lat": lat,
            "lon": lon,
            "temp_c": wx["temp_c"],
            "humidity": wx["humidity"],
            "pressure": wx["pressure"],
            "wind_speed": wx["wind_speed"],
            "wind_deg": wx["wind_deg"],
            "precip_prob": wx["precip_prob"],
            "snowfall": wx["snowfall"],
            "snow_depth": wx["snow_depth"],
            "cape": cape,
            "risk_code": risk_code,
            "risk_label": self.outlooks.get(risk_code, "Unknown"),
            "kp": kp,
            "bz": bz,
            "entropic_proxy": entropic_proxy,
            "dilation_proxy_ps_per_day": dilation_proxy,
        }

# =========================
# STORM PROJECTION HELPERS
# =========================

def project_path(lat, lon, bearing_deg, speed_mph, hours=1.0):
    """
    Simple great-circle projection for a storm cell.
    """
    R = 3958.8  # Earth radius in miles
    distance_miles = speed_mph * hours

    lat1, lon1, brng = map(math.radians, [lat, lon, bearing_deg])
    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_miles / R)
        + math.cos(lat1) * math.sin(distance_miles / R) * math.cos(brng)
    )
    lon2 = lon1 + math.atan2(
        math.sin(brng) * math.sin(distance_miles / R) * math.cos(lat1),
        math.cos(distance_miles / R) - math.sin(lat1) * math.sin(lat2),
    )

    return math.degrees(lat2), (math.degrees(lon2) + 180) % 360 - 180


def haversine(lat1, lon1, lat2, lon2):
    """
    Haversine distance in miles.
    """
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 1)

# =========================
# CITY LIST PLACEHOLDER
# =========================

CITY_LIST = [
    # ("City_Name, ST", lat, lon),
    # e.g. ("Seattle, WA", 47.61, -122.33),
    # TODO: paste your full sentinel network list here.
]

# =========================
# MAIN SCAN LOOP (EXAMPLE)
# =========================

def run_network_scan():
    """
    Example: scan all cities in CITY_LIST once,
    write JSON summaries, and log to DB.
    """
    conn = init_memory()
    ashley = AshleyIntelligence(conn)

    geo = get_geomagnetic_data()
    kp = geo["kp"]
    bz = geo["bz"]

    quakes = get_recent_earthquakes(time_period="day")
    nearby_quakes_count = len(quakes)

    summaries = []
    for city in CITY_LIST:
        try:
            summary = ashley.build_outlook_for_city(
                city=city,
                kp=kp,
                bz=bz,
                nearby_quakes_count=nearby_quakes_count,
            )
            summaries.append(summary)
            print(
                f"[{summary['city']}] CAPE={summary['cape']:.1f} "
                f"Risk={summary['risk_label']} "
                f"Dilation={summary['dilation_proxy_ps_per_day']:.2f} ps/day"
            )
        except Exception as e:
            print(f"[CITY_ERROR] {city[0]} -> {e}")

    # Save a snapshot forecast file
    ts = datetime.datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(FORECAST_DIR, f"ashley_forecast_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp_utc": ts,
                "kp": kp,
                "bz": bz,
                "cities": summaries,
            },
            f,
            indent=2,
        )
    print(f"[SNAPSHOT] wrote {out_path}")


if __name__ == "__main__":
    run_network_scan()
