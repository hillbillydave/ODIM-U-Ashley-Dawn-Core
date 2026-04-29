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
    # --- PLACEHOLDER: paste your full city list here ---
    # ("Los_Angeles, CA", 34.05, -118.24),
    # ...
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
