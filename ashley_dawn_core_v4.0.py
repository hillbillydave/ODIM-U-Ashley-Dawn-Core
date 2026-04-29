#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ashley Dawn Core v4.0
ODIM-U / QSTF-aware weather + informational-metric engine
Author: David E. Blackwell (Hillbilly Storm Chasers Research Division)
"""

import os
import math
import time
import json
import sqlite3
import datetime
from datetime import timezone
from typing import List, Dict, Tuple, Optional

import requests
import numpy as np

# ============================================================
# CONFIGURATION
# ============================================================

DB_PATH = "ashley_memory.db"
FORECAST_DIR = "ashley_forecasts"
os.makedirs(FORECAST_DIR, exist_ok=True)

USE_OPEN_METEO = False
OPENWEATHER_API_KEY = ""  # fill if using OpenWeather

REQUEST_HEADERS = {
    "User-Agent": "ODIM-U_Ashley_Dawn_Core_v4.0 (contact: your_email@example.com)"
}

# --- MODE FLAG ---
# "research": log full ODIM-U/QSTF diagnostics, resonance spectra, etc.
# "field":    leaner logging, focus on operational risk + warnings
ASHLEY_MODE = "research"  # or "field"

# --- ODIM-U / QSTF CONSTANTS (v4.0 framing) ---
KB = 1.380649e-23
HBAR = 1.0545718e-34
C = 299792458.0

# Blackwell Limit (order-of-magnitude placeholder; tune as needed)
BLACKWELL_LIMIT_BITS_PER_M3 = 1.0e43

# 2π-Hz resonance target (warp-drive / informational-geometry coupling hook)
TARGET_RESONANCE_FREQ_HZ = 2.0 * math.pi


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def init_memory() -> sqlite3.Connection:
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
            -- ODIM-U / QSTF diagnostics
            s_rel REAL,
            processing_flow REAL,
            dilation_proxy REAL,
            mode TEXT
        )
        """
    )
    conn.commit()
    return conn


# ============================================================
# GEOPHYSICAL DATA HANDLERS
# ============================================================

def get_recent_earthquakes(min_magnitude: float = 2.5,
                           time_period: str = "hour") -> List[Dict]:
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
                    "mag": props.get("mag", 0.0),
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
        print(f"[USGS_EQ_ERROR] {e}")
        return []


def get_geomagnetic_data() -> Dict[str, float]:
    """
    Pulls Kp and Bz as a crude proxy for geomagnetic stress.
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
# ODIM-U / QSTF CORE MATH
# ============================================================

def compute_relative_entropy(rho_diag: List[float],
                             sigma_diag: Optional[List[float]] = None) -> float:
    """
    Quantum relative entropy S_rel for diagonal density matrices.
    rho, sigma given as diagonal entries (non-negative).
    """
    rho = np.array(rho_diag, dtype=float)
    if sigma_diag is None:
        sigma = np.ones_like(rho)
    else:
        sigma = np.array(sigma_diag, dtype=float)

    # normalize
    rho_sum = rho.sum()
    sigma_sum = sigma.sum()
    if rho_sum <= 0 or sigma_sum <= 0:
        return 0.0
    rho /= rho_sum
    sigma /= sigma_sum

    # avoid log(0)
    eps = 1e-15
    rho_safe = np.clip(rho, eps, 1.0)
    sigma_safe = np.clip(sigma, eps, 1.0)

    s_rel = float(np.sum(rho_safe * (np.log(rho_safe) - np.log(sigma_safe))))
    return s_rel


def compute_processing_flow(s_rel: float,
                            volume_m3: float = 1.0) -> float:
    """
    Processing Flow F = I_x * (1 - S_rel / S_B)
    Here we treat I_x ~ 1 (normalized) and S_B ~ BLACKWELL_LIMIT_BITS_PER_M3 * V.
    This is a conceptual ODIM-U v4.0 / QSTF proxy.
    """
    s_b = BLACKWELL_LIMIT_BITS_PER_M3 * max(volume_m3, 1e-6)
    # clamp S_rel
    s_rel_clamped = max(0.0, min(s_rel, s_b))
    return 1.0 * (1.0 - s_rel_clamped / s_b)


def compute_decoherence_rate(temp_eff_K: float,
                             psi_eff: float = -1e-6) -> float:
    """
    Gamma ~ (k_B T_eff / ħ) * (1 + |psi| / c^2)
    """
    gamma = (KB * temp_eff_K / HBAR) * (1.0 + abs(psi_eff) / (C ** 2))
    return gamma


def compute_dilation_proxy(gamma: float) -> float:
    """
    Dilation proxy ~ 1 / gamma (seconds).
    """
    if gamma <= 0:
        return 0.0
    return 1.0 / gamma


# ============================================================
# 2π-HZ RESONANCE / WARP-DRIVE COUPLING HOOKS
# ============================================================

def resonance_spectrum(time_series: List[float],
                       dt: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    Simple FFT-based spectrum of a scalar diagnostic (e.g., dilation_proxy).
    Returns (freqs, power).
    """
    if len(time_series) < 4:
        return np.array([]), np.array([])
    arr = np.array(time_series, dtype=float)
    n = len(arr)
    fft_vals = np.fft.rfft(arr - arr.mean())
    freqs = np.fft.rfftfreq(n, d=dt)
    power = np.abs(fft_vals) ** 2
    return freqs, power


def find_2pi_resonance(freqs: np.ndarray,
                       power: np.ndarray,
                       target_hz: float = TARGET_RESONANCE_FREQ_HZ,
                       tol_hz: float = 0.1) -> Dict[str, float]:
    """
    Look for power near the 2π-Hz band. This is a *diagnostic* hook:
    - In research mode, you can log when the manifold's dilation/flux
      shows structure near the warp-drive resonance band.
    - In field mode, you might ignore or downweight this.
    """
    if freqs.size == 0:
        return {"peak_power": 0.0, "peak_freq": 0.0, "near_target": 0.0}

    mask = (freqs >= target_hz - tol_hz) & (freqs <= target_hz + tol_hz)
    if not np.any(mask):
        return {"peak_power": 0.0, "peak_freq": 0.0, "near_target": 0.0}

    idx = np.argmax(power[mask])
    band_freqs = freqs[mask]
    band_power = power[mask]
    peak_freq = float(band_freqs[idx])
    peak_power = float(band_power[idx])
    near_target = float(peak_power)
    return {
        "peak_power": peak_power,
        "peak_freq": peak_freq,
        "near_target": near_target,
    }


def warp_drive_coupling_hook(dilation_series: List[float],
                             dt: float) -> Dict[str, float]:
    """
    High-level hook: given a time series of dilation proxies,
    compute resonance diagnostics near 2π Hz.

    This does NOT claim a working warp drive; it just provides
    a clean interface for your separate warp-drive simulation
    to subscribe to Ashley's manifold diagnostics.
    """
    freqs, power = resonance_spectrum(dilation_series, dt)
    res = find_2pi_resonance(freqs, power)
    return res


# ============================================================
# ASHLEY INTELLIGENCE (RISK / ECAPE / OUTLOOK)
# ============================================================

class AshleyIntelligence:
    def __init__(self, conn: sqlite3.Connection):
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

    def calculate_ashley_cape(
        self,
        temp_c: float,
        humidity_pct: float,
        wind_speed_mps: float,
        precip_prob_pct: float,
        pressure_delta_hpa: float = 0.0,
        kp: float = 0.0,
        bz: float = 0.0,
        nearby_quakes: int = 0,
    ) -> float:
        if temp_c < 5.0:
            return 0.0

        thermal_base = max(0.0, (temp_c - 5.0) / 30.0)
        thermal = thermal_base ** 3.0

        if temp_c > 15.0:
            moisture = min(1.0, humidity_pct / 90.0)
        else:
            moisture = min(0.7, humidity_pct / 100.0)

        shear = min(0.8, wind_speed_mps / 40.0)
        trigger = min(0.8, precip_prob_pct / 150.0)
        pressure_boost = max(0.0, -pressure_delta_hpa / 5.0)

        geo_factor = 0.0
        if kp >= 7.0:
            geo_factor += (kp - 6.0) * 0.3
        if bz < -15.0:
            geo_factor += (abs(bz) - 10.0) / 100.0

        quake_factor = min(0.3, nearby_quakes * 0.05)

        base = (moisture + thermal) * 0.8
        multipliers = 1.0 + shear + trigger + pressure_boost + geo_factor + quake_factor
        raw = base * multipliers

        ecape = min(1800.0, math.log1p(raw * 5.0) * 600.0)
        if temp_c < 10.0:
            ecape *= 0.5
        return round(ecape, 0)


# ============================================================
# STORM PROJECTION HELPERS
# ============================================================

def project_path(lat: float, lon: float,
                 bearing_deg: float,
                 speed_mph: float,
                 hours: float = 1.0) -> Tuple[float, float]:
    R = 3958.8  # miles
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
    return math.degrees(lat2), (math.degrees(lon2) + 180.0) % 360.0 - 180.0


def haversine(lat1: float, lon1: float,
              lat2: float, lon2: float) -> float:
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return round(R * c, 1)


# ============================================================
# SENTINEL CITY LIST
# ============================================================

CITY_LIST: List[Tuple[str, float, float]] = [
   ("Los_Angeles, CA", 34.05, -118.24),
    ("San_Francisco, CA", 37.77, -122.42),
    ("San_Diego, CA", 32.72, -117.16),
    ("Sacramento, CA", 38.58, -121.49),
    ("Fresno, CA", 36.75, -119.77),
    ("Bakersfield, CA", 35.37, -119.02),
    ("San_Jose, CA", 37.34, -121.89),
    ("Oakland, CA", 37.80, -122.27),
    ("Long_Beach, CA", 33.77, -118.19),
    ("Anaheim, CA", 33.84, -117.91),
    ("Riverside, CA", 33.95, -117.40),
    ("Santa_Barbara, CA", 34.42, -119.70),
    ("Redding, CA", 40.59, -122.39),
    ("Eureka, CA", 40.80, -124.16),
    ("Palm_Springs, CA", 33.83, -116.55),
    ("Monterey, CA", 36.60, -121.89),
    ("Stockton, CA", 37.96, -121.29),
    ("Modesto, CA", 37.64, -120.99),
    ("Santa_Rosa, CA", 38.44, -122.71),
    ("Ventura, CA", 34.28, -119.29),
    ("Dallas, TX", 32.78, -96.80),
    ("Houston, TX", 29.76, -95.37),
    ("San_Antonio, TX", 29.42, -98.49),
    ("Austin, TX", 30.27, -97.74),
    ("Fort_Worth, TX", 32.76, -97.33),
    ("El_Paso, TX", 31.76, -106.49),
    ("Corpus_Christi, TX", 27.80, -97.40),
    ("Lubbock, TX", 33.58, -101.85),
    ("Amarillo, TX", 35.22, -101.83),
    ("Brownsville, TX", 25.90, -97.50),
    ("McAllen, TX", 26.20, -98.23),
    ("Waco, TX", 31.55, -97.15),
    ("Abilene, TX", 32.45, -99.73),
    ("Tyler, TX", 32.35, -95.30),
    ("Beaumont, TX", 30.08, -94.13),
    ("Odessa, TX", 31.85, -102.37),
    ("Midland, TX", 31.99, -102.08),
    ("Killeen, TX", 31.12, -97.73),
    ("College_Station, TX", 30.63, -96.33),
    ("Laredo, TX", 27.53, -99.51),
    ("Miami, FL", 25.76, -80.19),
    ("Orlando, FL", 28.54, -81.38),
    ("Tampa, FL", 27.95, -82.46),
    ("Jacksonville, FL", 30.33, -81.66),
    ("Tallahassee, FL", 30.44, -84.28),
    ("Fort_Lauderdale, FL", 26.12, -80.14),
    ("St_Petersburg, FL", 27.77, -82.64),
    ("Pensacola, FL", 30.42, -87.22),
    ("Gainesville, FL", 29.65, -82.32),
    ("Naples, FL", 26.14, -81.79),
    ("Sarasota, FL", 27.34, -82.53),
    ("Daytona_Beach, FL", 29.21, -81.02),
    ("West_Palm_Beach, FL", 26.72, -80.05),
    ("Fort_Myers, FL", 26.64, -81.87),
    ("Panama_City, FL", 30.16, -85.66),
    ("New_York_City, NY", 40.71, -74.00),
    ("Buffalo, NY", 42.89, -78.88),
    ("Rochester, NY", 43.16, -77.61),
    ("Albany, NY", 42.65, -73.76),
    ("Syracuse, NY", 43.05, -76.15),
    ("Yonkers, NY", 40.93, -73.90),
    ("White_Plains, NY", 41.03, -73.76),
    ("Binghamton, NY", 42.10, -75.91),
    ("Utica, NY", 43.10, -75.23),
    ("Ithaca, NY", 42.44, -76.50),
    ("Chicago, IL", 41.88, -87.63),
    ("Indianapolis, IN", 39.77, -86.16),
    ("Detroit, MI", 42.33, -83.05),
    ("Philadelphia, PA", 39.95, -75.17),
    ("Phoenix, AZ", 33.45, -112.07),
    ("Denver, CO", 39.74, -104.99),
    ("Seattle, WA", 47.61, -122.33),
    ("Boston, MA", 42.36, -71.06),
    ("Atlanta, GA", 33.75, -84.39),
    ("Washington_DC", 38.91, -77.04),
    ("Charlotte, NC", 35.23, -80.84),
    ("Nashville, TN", 36.16, -86.78),
    ("Minneapolis, MN", 44.98, -93.27),
    ("Kansas_City, MO", 39.10, -94.58),
    ("Oklahoma_City, OK", 35.47, -97.52),
    ("Las_Vegas, NV", 36.17, -115.14),
    ("Portland, OR", 45.52, -122.68),
    ("Salt_Lake_City, UT", 40.76, -111.89),
    ("New_Orleans, LA", 29.95, -90.07),
    ("Baltimore, MD", 39.29, -76.61),
    ("Milwaukee, WI", 43.04, -87.91),
    ("Albuquerque, NM", 35.08, -106.61),
    ("Tucson, AZ", 32.22, -110.93),
    ("Mesa, AZ", 33.42, -111.83),
    ("Colorado_Springs, CO", 38.83, -104.82),
    ("Raleigh, NC", 35.78, -78.64),
    ("Virginia_Beach, VA", 36.85, -75.98),
    ("Omaha, NE", 41.26, -96.01),
    ("Cleveland, OH", 41.50, -81.69),
    ("Wichita, KS", 37.69, -97.34),
    ("Tulsa, OK", 36.15, -95.99),
    ("Honolulu, HI", 21.31, -157.86),
    ("Anchorage, AK", 61.22, -149.90),
    ("Boise, ID", 43.62, -116.20),
    ("Spokane, WA", 47.66, -117.43),
    ("Billings, MT", 45.78, -108.50),
    ("Sioux_Falls, SD", 43.54, -96.73),
    ("Fargo, ND", 46.88, -96.79),
    ("Des_Moines, IA", 41.59, -93.62),
    ("Little_Rock, AR", 34.75, -92.29),
    ("Jackson, MS", 32.30, -90.18),
    ("Birmingham, AL", 33.52, -86.80),
    ("Memphis, TN", 35.15, -90.05),
    ("Louisville, KY", 38.25, -85.76),
    ("Richmond, VA", 37.54, -77.43),
    ("Providence, RI", 41.82, -71.41),
    ("Hartford, CT", 41.76, -72.67),
    ("Bridgeport, CT", 41.19, -73.20),
    ("Charleston, SC", 32.78, -79.93),
    ("Columbia, SC", 34.00, -81.03),
    ("Knoxville, TN", 35.96, -83.92),
    ("Chattanooga, TN", 35.05, -85.31),
    ("Savannah, GA", 32.08, -81.10),
    ("Mobile, AL", 30.69, -88.04),
    ("Baton_Rouge, LA", 30.45, -91.15),
    ("Shreveport, LA", 32.51, -93.

]


# ============================================================
# WEATHER API HANDLERS (SIMPLE FIELD PIPELINE)
# ============================================================

def fetch_openweather(lat: float, lon: float) -> Optional[Dict]:
    if not OPENWEATHER_API_KEY:
        return None
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=10, headers=REQUEST_HEADERS)
        if not resp.ok:
            return None
        return resp.json()
    except Exception as e:
        print(f"[OPENWEATHER_ERROR] {e}")
        return None


def extract_basic_weather(data: Dict) -> Dict:
    main = data.get("main", {})
    wind = data.get("wind", {})
    rain = data.get("rain", {})
    snow = data.get("snow", {})

    pressure = float(main.get("pressure", 1013.0))
    temp = float(main.get("temp", 15.0))
    humidity = float(main.get("humidity", 50.0))
    wind_speed = float(wind.get("speed", 0.0))
    wind_deg = float(wind.get("deg", 0.0))
    precip_prob = float(rain.get("1h", 0.0) + snow.get("1h", 0.0)) * 100.0
    snowfall = float(snow.get("1h", 0.0))
    snow_depth = 0.0  # placeholder

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
# CORE PIPELINE: ONE CITY SCAN
# ============================================================

def process_city(conn: sqlite3.Connection,
                 ashley: AshleyIntelligence,
                 city: Tuple[str, float, float]) -> None:
    name, lat, lon = city
    print(f"[SCAN] {name} ({lat:.2f}, {lon:.2f})")

    if USE_OPEN_METEO:
        # you can wire Open-Meteo here if desired
        weather_data = None
    else:
        raw = fetch_openweather(lat, lon)
        if raw is None:
            print("[WARN] No weather data; skipping.")
            return
        weather_data = extract_basic_weather(raw)

    geo = get_geomagnetic_data()
    quakes = get_recent_earthquakes(time_period="hour")
    nearby_quakes = sum(
        1 for q in quakes if haversine(lat, lon, q["lat"], q["lon"]) <= 250.0
    )

    pressure = weather_data["pressure"]
    temp = weather_data["temp"]
    humidity = weather_data["humidity"]
    wind_speed = weather_data["wind_speed"]
    wind_deg = weather_data["wind_deg"]
    precip_prob = weather_data["precip_prob"]
    snowfall = weather_data["snowfall"]
    snow_depth = weather_data["snow_depth"]

    # Simple pressure delta proxy (you can refine with history)
    pressure_delta = 0.0

    cape = ashley.calculate_ashley_cape(
        temp_c=temp,
        humidity_pct=humidity,
        wind_speed_mps=wind_speed,
        precip_prob_pct=precip_prob,
        pressure_delta_hpa=pressure_delta,
        kp=geo["kp"],
        bz=geo["bz"],
        nearby_quakes=nearby_quakes,
    )

    # --- ODIM-U / QSTF DIAGNOSTICS ---
    # Build a simple diagonal "rho" from normalized weather variables
    rho_diag = [
        max(0.0, temp),
        max(0.0, humidity),
        max(0.0, wind_speed),
        max(0.0, precip_prob),
        max(0.0, cape),
    ]
    sigma_diag = [1.0, 1.0, 1.0, 1.0, 1.0]  # vacuum reference proxy

    s_rel = compute_relative_entropy(rho_diag, sigma_diag)
    F = compute_processing_flow(s_rel, volume_m3=1.0)

    temp_eff_K = temp + 273.15
    gamma = compute_decoherence_rate(temp_eff_K=temp_eff_K, psi_eff=-1e-6)
    dilation_proxy = compute_dilation_proxy(gamma)

    # --- LOGGING ---
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO weather_logs (
            timestamp, location, pressure, temp, humidity,
            wind_speed, wind_deg, precip_prob, snowfall, snow_depth,
            cape, risk, s_rel, processing_flow, dilation_proxy, mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.datetime.now(tz=timezone.utc).isoformat(),
            name,
            pressure,
            temp,
            humidity,
            wind_speed,
            wind_deg,
            precip_prob,
            snowfall,
            snow_depth,
            cape,
            0.0,  # risk placeholder; you can derive from CAPE + others
            s_rel,
            F,
            dilation_proxy,
            ASHLEY_MODE,
        ),
    )
    conn.commit()

    # --- FIELD vs RESEARCH OUTPUT ---
    if ASHLEY_MODE == "field":
        print(
            f"[FIELD] {name}: T={temp:.1f}°C, CAPE={cape:.0f}, "
            f"dp={dilation_proxy:.3e}s"
        )
    else:
        print(
            f"[RESEARCH] {name}: T={temp:.1f}°C, CAPE={cape:.0f}, "
            f"S_rel={s_rel:.3e}, F={F:.3e}, dp={dilation_proxy:.3e}s"
        )


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    conn = init_memory()
    ashley = AshleyIntelligence(conn)

    if not CITY_LIST:
        print("[WARN] CITY_LIST is empty. Add sentinel cities before running.")
        return

    for city in CITY_LIST:
        try:
            process_city(conn, ashley, city)
            time.sleep(1.0)
        except KeyboardInterrupt:
            print("[INTERRUPT] Stopping scan.")
            break
        except Exception as e:
            print(f"[ERROR] {city[0]}: {e}")

    conn.close()


if __name__ == "__main__":
    main()
