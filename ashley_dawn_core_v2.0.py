# ===================================================================
# COPYRIGHT (C) 2026 DAVID E. BLACKWELL. ALL RIGHTS RESERVED.
# SYSTEM: ODIM-U v31.0 — Ashley Dawn Core
# ===================================================================

import os
import math
import time
import sqlite3
import requests
import datetime
import json
from datetime import timezone

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
DB_PATH = "ashley_memory.db"
FORECAST_DIR = "ashley_forecasts"
os.makedirs(FORECAST_DIR, exist_ok=True)

USE_OPEN_METEO = False
OPENWEATHER_API_KEY = ""

REQUEST_HEADERS = {
    "User-Agent": "ODIM-U_Ashley_Dawn_Core_v19.5 (your_email@example.com)"
}

# ------------------------------------------------------------
# DATABASE INITIALIZATION
# ------------------------------------------------------------
def init_memory():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS weather_logs (
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
            risk REAL
        )
    """)

    c.execute("PRAGMA table_info(weather_logs)")
    columns = [row[1] for row in c.fetchall()]

    missing = [
        'wind_speed', 'wind_deg', 'precip_prob',
        'snowfall', 'snow_depth', 'cape', 'risk'
    ]

    for col in missing:
        if col not in columns:
            c.execute(f"ALTER TABLE weather_logs ADD COLUMN {col} REAL")

    conn.commit()
    return conn

# ------------------------------------------------------------
# GEOPHYSICAL DATA HANDLERS
# ------------------------------------------------------------
def get_recent_earthquakes(min_magnitude=2.5, time_period='hour'):
    feeds = {
        'hour': 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson',
        'day':  'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson'
    }
    url = feeds.get(time_period, feeds['hour'])

    try:
        response = requests.get(url, timeout=15, headers=REQUEST_HEADERS)
        data = response.json()
        quakes = []

        for feature in data.get('features', [])[:10]:
            props = feature['properties']
            coords = feature['geometry']['coordinates']

            quakes.append({
                'mag': props.get('mag', 0),
                'place': props.get('place', 'Unknown'),
                'time': datetime.datetime.fromtimestamp(
                    props.get('time', 0) / 1000, tz=timezone.utc
                ),
                'lat': coords[1],
                'lon': coords[0],
                'depth_km': coords[2]
            })

        return quakes

    except Exception as e:
        print(f"[USGS_EQUAKE_ERROR] {e}")
        return []

def get_geomagnetic_data():
    try:
        kp_url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
        kp_resp = requests.get(kp_url, timeout=10, headers=REQUEST_HEADERS)
        kp_data = kp_resp.json()[-1] if kp_resp.ok else {}
        kp = float(kp_data[1]) if len(kp_data) > 1 else 0.0

        bz_url = "https://services.swpc.noaa.gov/text/ace-swepam.txt"
        bz_resp = requests.get(bz_url, timeout=10, headers=REQUEST_HEADERS)

        bz = 0.0
        if bz_resp.ok:
            for line in reversed(bz_resp.text.splitlines()):
                if line.strip() and not line.startswith(('#', ':')):
                    parts = line.split()
                    if len(parts) >= 8 and parts[7] != '999.9':
                        bz = float(parts[7])
                        break

        return {'kp': kp, 'bz': bz}

    except Exception as e:
        print(f"[GEO_ERROR] {e}")
        return {'kp': 0.0, 'bz': 0.0}

# ------------------------------------------------------------
# ASHLEY INTELLIGENCE ENGINE
# ------------------------------------------------------------
class AshleyIntelligence:
    def __init__(self, conn):
        self.conn = conn
        self.outlooks = {
            'A': "Settled_Fine",
            'B': "Fine_Weather",
            'C': "Becoming_Fine",
            'D': "Fine_Becoming_Less_Settled",
            'E': "Showers_Likely",
            'Z': "Stormy_High_Volatility_Watch_Closely",
            'T': "Tornado_Risk_Elevated",
            'S': "Significant_Snow_Expected"
        }

    def calculate_ashley_cape(self, temp, humidity, wind_speed,
                              precip_prob, pressure_delta=0,
                              kp=0.0, bz=0.0, nearby_quakes=0):

        if temp < 5:
            return 0

        thermal_base = max(0, (temp - 5) / 30.0)
        thermal = thermal_base ** 3.0

        moisture = (
            min(1.0, humidity / 90.0)
            if temp > 15 else
            min(0.7, humidity / 100.0)
        )

        shear = min(0.8, wind_speed / 40.0)
        trigger = min(0.8, precip_prob / 150.0)
        pressure_boost = max(0, -pressure_delta / 5.0)

        geo_factor = 0.0
        if kp >= 7:
            geo_factor += (kp - 6) * 0.3
        if bz < -15:
            geo_factor += (abs(bz) - 10) / 100.0

        quake_factor = min(0.3, nearby_quakes * 0.05)

        base = (moisture + thermal) * 0.8
        multipliers = 1 + shear + trigger + pressure_boost + geo_factor + quake_factor

        raw = base * multipliers
        ecape = min(1800, math.log1p(raw * 5) * 600)

        if temp < 10:
            ecape *= 0.5

        return round(ecape, 0)

# ------------------------------------------------------------
# STORM PROJECTION HELPERS
# ------------------------------------------------------------
def project_path(lat, lon, bearing, speed_mph, hours=1):
    R = 3958.8
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
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1

    a = (
        math.sin(dlat / 2) ** 2 +
        math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c)

# ------------------------------------------------------------
# CITY LIST (FULL SENTINEL NETWORK)
# ------------------------------------------------------------
CITY_LIST = [
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

# ------------------------------------------------------------
# MAIN ENGINE — PROCESSING EACH LOCATION
# ------------------------------------------------------------

def process_location(name, lat, lon, data, ts, conn, brain):
    if not data:
        print(f"[{name}] Failed to fetch data")
        return

    current = data['current']
    hourly = data['hourly']
    daily = data['daily']

    pressure_now = current.get('surface_pressure', 1013.0)
    temp = current.get('temperature_2m', 0)
    humidity = current.get('relative_humidity_2m', 50)
    wind_speed = current.get('wind_speed_10m', 0)
    wind_dir = current.get('wind_direction_10m', 0)
    precip_prob = current.get('precipitation_probability', 0)
    cape = current.get('cape', 0)

    # 24h snowfall
    snowfall_list = hourly.get('snowfall', [0] * 24)
    snowfall_sum = sum(snowfall_list[:24])

    # Geomagnetic + earthquake data
    geo = get_geomagnetic_data()
    quakes = get_recent_earthquakes()
    nearby_quakes = len(quakes)

    # CAPE calculation
    ecape = brain.calculate_ashley_cape(
        temp=temp,
        humidity=humidity,
        wind_speed=wind_speed,
        precip_prob=precip_prob,
        pressure_delta=0,
        kp=geo['kp'],
        bz=geo['bz'],
        nearby_quakes=nearby_quakes
    )

    # Risk score
    gusts_list = hourly.get('wind_gusts_10m', [0])
    gusts = gusts_list[0] if gusts_list else 0

    risk = round(
        (precip_prob * 0.5) +
        (gusts * 0.3) +
        (humidity * 0.1) +
        ((ecape / 1000) * 20),
        1
    )

    # Save to DB
    try:
        c = conn.cursor()
        c.execute("""
            INSERT INTO weather_logs
            (timestamp, location, pressure, temp, humidity,
             wind_speed, wind_deg, precip_prob, snowfall,
             snow_depth, cape, risk)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ts, name, pressure_now, temp, humidity,
            wind_speed, wind_dir, precip_prob,
            snowfall_list[0] if snowfall_list else 0,
            hourly.get('snow_depth', [0])[0],
            ecape, risk
        ))
        conn.commit()
    except Exception as e:
        print(f"[DB ERROR] {name}: {e}")

    print(
        f"[{name}] P:{pressure_now:.1f}mb | T:{temp:.1f}°C | "
        f"CAPE:{ecape} | Precip:{precip_prob}% | "
        f"Snow24:{snowfall_sum:.1f}cm | Risk:{risk}"
    )


# ------------------------------------------------------------
# MAIN SYSTEM LOOP
# ------------------------------------------------------------

def run_system():
    conn = init_memory()
    brain = AshleyIntelligence(conn)

    ts = datetime.datetime.now(timezone.utc)
    api_name = "Open-Meteo" if USE_OPEN_METEO else "OpenWeatherMap"

    print(f"\n[ODIM-U v31.0] Scan Start | UTC {ts} | API={api_name}\n")

    for name, lat, lon in CITY_LIST:
        print(f"Fetching: {name}...")
        data = get_openweather_single(lat, lon)
        time.sleep(1.05)
        process_location(name, lat, lon, data, ts, conn, brain)

    print("\n[SCAN COMPLETE] Next cycle in 60 minutes.\n")
    conn.close()


# ------------------------------------------------------------
# WEATHER NEXUS (ODIM-U v1.2 Minimal Example)
# ------------------------------------------------------------

import numpy as np
from scipy.linalg import logm

class WeatherNexus:
    def __init__(self, beta_meas=0.9999):
        self.beta = beta_meas
        self.kb = 1.380649e-23
        self.hbar = 1.0545718e-34
        self.c = 299792458

    def compute_relative_entropy(self, rho, sigma):
        rho = rho / np.trace(rho)
        sigma = sigma / np.trace(sigma)
        s_rel = np.trace(rho @ (logm(rho) - logm(sigma)))
        return np.real(s_rel)

    def calculate_decoherence_gamma(self, psi, temp_eff):
        gamma = (self.kb * temp_eff / self.hbar) * (1 + abs(psi) / (self.c ** 2))
        return gamma

def perform_weather_scan(raw_data):
    nexus = WeatherNexus()
    rho = np.diag(raw_data)
    sigma = np.eye(len(raw_data))

    s_rel = nexus.compute_relative_entropy(rho, sigma)
    gamma = nexus.calculate_decoherence_gamma(psi=-1e-6, temp_eff=293)
    delta_t = 1 / gamma

    print(f"Entropic_Proxy: {s_rel:.4e}")
    print(f"Dilation_Proxy: {delta_t:.4e}s")

    return s_rel, gamma


# ------------------------------------------------------------
# AUTO-RUN LOOP
# ------------------------------------------------------------

if __name__ == "__main__":
    print("Ashley Dawn Core v31.0 — Hourly Cycle Started")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            run_system()
            time.sleep(3600)
        except KeyboardInterrupt:
            print("\nShutdown requested. Exiting gracefully.")
            break
        except Exception as e:
            print(f"[RUNTIME ERROR] {e}")
