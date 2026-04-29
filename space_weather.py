"""
===================================================================
COPYRIGHT (C) 2026 DAVID E. BLACKWELL. ALL RIGHTS RESERVED.
===================================================================
SYSTEM: ODIM-U v19.5 (Ashley Dawn Core) - DEEP AI ACTIVATED
DATABASE: SQLite3 Integrated Memory (Local Observations)
===================================================================
"""
import os
import math
import time
import sqlite3
import requests
import datetime
from datetime import timezone

# --- CONFIGURATION ---
DB_PATH = "ashley_memory.db"
FORECAST_DIR = "ashley_forecasts"
os.makedirs(FORECAST_DIR, exist_ok=True)

# API SELECTION
USE_OPEN_METEO = False  # Set to False to use OpenWeatherMap (requires key below)
OPENWEATHER_API_KEY = ""  # Replace with your real key

REQUEST_HEADERS = {
    "User-Agent": "ODIM-U Ashley Dawn Core v19.5 (your_email@example.com)"
}

# --- DATABASE INITIALIZATION ---
def init_memory():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
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
    ''')
    c.execute("PRAGMA table_info(weather_logs)")
    columns = [row[1] for row in c.fetchall()]
    missing = ['wind_speed', 'wind_deg', 'precip_prob', 'snowfall', 'snow_depth', 'cape', 'risk']
    for col in missing:
        if col not in columns:
            c.execute(f"ALTER TABLE weather_logs ADD COLUMN {col} REAL")
    conn.commit()
    return conn

# --- ASHLEY'S INTELLIGENCE - NOW BASED HEAVILY ON 48-HOUR DETAILED TRENDS ---
class AshleyIntelligence:
    def __init__(self, conn):
        self.conn = conn
        self.outlooks = {
            'A': "Settled Fine", 'B': "Fine Weather", 'C': "Becoming Fine",
            'D': "Fine, Becoming Less Settled", 'E': "Showers Likely",
            'Z': "Stormy, High Volatility - Watch Closely",
            'T': "Tornado Risk Elevated", 'S': "Significant Snow Expected"
        }

    def get_last_pressure(self, location):
        c = self.conn.cursor()
        c.execute("SELECT pressure FROM weather_logs WHERE location = ? ORDER BY timestamp DESC LIMIT 1", (location,))
        result = c.fetchone()
        return result[0] if result else None

    def analyze_forecast(self, pressure_now, pressure_last, precip_prob, wind_speed, cape, temp, snowfall_sum):
        outlook = "Observing..."
        if pressure_last is not None:
            delta = pressure_now - pressure_last
            if delta <= -2.0 or (precip_prob > 70 and wind_speed > 20):
                outlook = self.outlooks['Z']
        alerts = []
        if cape > 1000 and wind_speed > 25 and precip_prob > 50:
            alerts.append("HIGH TORNADO RISK")
        elif cape > 500 and wind_speed > 20:
            alerts.append("Elevated Tornado Potential")
        if temp < 2 and snowfall_sum > 2:
            alerts.append(f"Heavy Snow: ~{snowfall_sum:.1f} cm expected")
        elif temp < 4 and snowfall_sum > 0:
            alerts.append(f"Snow Possible: {snowfall_sum:.1f} cm")
        if alerts:
            outlook += " | ALERTS: " + "; ".join(alerts)
        return outlook or self.outlooks['C']

    def generate_extended_forecast(self, current, hourly):
        # Ashley now bases ALL extended outlooks on the detailed 48-hour hourly forecast
        hours_to_use = min(48, len(hourly.get('time', [])))
        if hours_to_use < 24:
            return ""

        pressure_list = hourly.get('surface_pressure', [1013] * hours_to_use)[:hours_to_use]
        temp_list = hourly.get('temperature_2m', [0] * hours_to_use)[:hours_to_use]
        precip_list = hourly.get('precipitation_probability', [0] * hours_to_use)[:hours_to_use]
        cape_list = hourly.get('cape', [0] * hours_to_use)[:hours_to_use]
        snowfall_list = hourly.get('snowfall', [0] * hours_to_use)[:hours_to_use]

        pressure_trend = pressure_list[-1] - pressure_list[0]
        temp_avg = sum(temp_list) / hours_to_use
        temp_trend = temp_list[-1] - temp_list[0]
        precip_avg = sum(precip_list) / hours_to_use
        cape_max = max(cape_list)
        snowfall_48h = sum(snowfall_list)

        outlook = "=== ASHLEY'S EXTENDED OUTLOOK (derived from 48-hour detailed trends) ===\n"

        if pressure_trend < -5:
            pressure_desc = "Strong falling pressure → stormy/volatility increasing"
        elif pressure_trend < -2:
            pressure_desc = "Falling pressure → becoming unsettled"
        elif pressure_trend > 5:
            pressure_desc = "Strong rising pressure → settling/improving"
        elif pressure_trend > 2:
            pressure_desc = "Rising pressure → fine weather persisting"
        else:
            pressure_desc = "Stable pressure → little change expected"

        temp_desc = f"Avg ~{temp_avg:.1f}°C"
        if temp_trend > 3:
            temp_desc += ", strong warming trend"
        elif temp_trend > 1:
            temp_desc += ", mild warming"
        elif temp_trend < -3:
            temp_desc += ", strong cooling trend"
        elif temp_trend < -1:
            temp_desc += ", mild cooling"

        precip_desc = f"Avg precip chance {precip_avg:.0f}%"
        if precip_avg > 50:
            precip_desc += " → wet period likely"
        elif precip_avg > 20:
            precip_desc += " → occasional showers"

        cape_desc = f"Peak CAPE {cape_max:.0f} J/kg"
        if cape_max > 1500:
            cape_desc += " → severe thunderstorm/tornado threat persists"
        elif cape_max > 800:
            cape_desc += " → thunderstorm potential"

        snow_desc = f"~{snowfall_48h:.1f}cm in 48h"
        if snowfall_48h > 10:
            snow_desc += " → heavy snow pattern may continue"

        outlook += f"7-Day Outlook: {pressure_desc}. {temp_desc}. {precip_desc}. {cape_desc}. {snow_desc}.\n"
        outlook += f"14-Day Outlook: Trends expected to continue gradually — {'warming' if temp_trend > 0 else 'cooling' if temp_trend < 0 else 'stable'} overall.\n"
        outlook += f"30-Day Trend: Momentum favors {'volatile/unsettled' if abs(pressure_trend) > 3 else 'settled'} conditions persisting.\n"
        outlook += "========================================\n"
        return outlook

def calculate_blackwell_cape(em_field_intensity, humidity, temp_gradient):
    """
    Derived from ODIM-U v1.2: Calculates Emergent CAPE as a function 
    of informational distinguishability (Srel) and decoherence (Gamma).
    """
    # beta_meas: Contextual efficiency (measurement precision/noise floor)
    beta_meas = 0.85 
    
    # Gamma (Decoherence rate) mapped to EM flux
    # Higher EM activity = higher decoherence = higher informational flux
    gamma = em_field_intensity * (1 / beta_meas)
    
    # Equation 3: Gamma proportional to the potential (instability)
    # We solve for the 'Informational Potential' which manifests as CAPE
    emergent_instability = math.log(gamma + 1) * (temp_gradient + humidity/100)
    
    # Scale to standard J/kg units for Ashley's risk processor
    return emergent_instability * 500

# --- OPEN-METEO BATCH FETCH ---
def get_openmeteo_batch(cities_batch):
    # (unchanged from previous version - included for completeness)
    if not cities_batch:
        return []
    lats = ",".join([f"{city[1]:.4f}" for city in cities_batch])
    lons = ",".join([f"{city[2]:.4f}" for city in cities_batch])
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lats}&longitude={lons}"
        f"&current=temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m,wind_direction_10m,precipitation_probability,cape"
        f"&hourly=temperature_2m,precipitation_probability,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m,snowfall,snow_depth,cape"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_probability_max,wind_speed_10m_max,snowfall_sum"
        f"&forecast_days=16&timezone=auto"
    )
    for attempt in range(1, 4):
        try:
            response = requests.get(url, timeout=60, headers=REQUEST_HEADERS)
            response.raise_for_status()
            batch_data = response.json()
            if isinstance(batch_data, list):
                results = []
                for i in range(len(cities_batch)):
                    if i < len(batch_data):
                        loc_data = batch_data[i]
                        if all(key in loc_data for key in ['current', 'hourly', 'daily']):
                            results.append(loc_data)
                        else:
                            results.append(None)
                    else:
                        results.append(None)
                return results
        except Exception as e:
            print(f"Open-Meteo batch attempt {attempt}/3 failed: {e}")
            if attempt < 3:
                time.sleep(5 + attempt)
    return [None] * len(cities_batch)

# --- OPENWEATHERMAP SINGLE FETCH + ADAPTATION ---
def get_openweather_single(lat, lon):
    if not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY == "":
        print("OpenWeatherMap API key missing!")
        return None
    
    # FREE TIER ENDPOINTS
    curr_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat:.4f}&lon={lon:.4f}&units=metric&appid={OPENWEATHER_API_KEY}"
    fore_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat:.4f}&lon={lon:.4f}&units=metric&appid={OPENWEATHER_API_KEY}"

    try:
        # 1. Get Current Weather
        curr_res = requests.get(curr_url, timeout=30, headers=REQUEST_HEADERS)
        curr_res.raise_for_status()
        c_data = curr_res.json()

        # 2. Get 5-Day Forecast (3-hour increments)
        fore_res = requests.get(fore_url, timeout=30, headers=REQUEST_HEADERS)
        fore_res.raise_for_status()
        f_data = fore_res.json()

        # Adapt to Ashley's structure
        hourly_list = f_data.get('list', []) # This is 40 data points (5 days)
        
        adapted = {
            'current': {
                'temperature_2m': c_data['main']['temp'],
                'relative_humidity_2m': c_data['main']['humidity'],
                'surface_pressure': c_data['main']['pressure'],
                'wind_speed_10m': c_data.get('wind', {}).get('speed', 0) * 3.6,
                'wind_direction_10m': c_data.get('wind', {}).get('deg', 0),
                'precipitation_probability': hourly_list[0].get('pop', 0) * 100 if hourly_list else 0,
                'cape': 0,
            },
            'hourly': {
                'time': [h.get('dt') for h in hourly_list],
                'wind_speed_10m': [h.get('wind', {}).get('speed', 0) * 3.6 for h in hourly_list],
                'wind_direction_10m': [h.get('wind', {}).get('deg', 0) for h in hourly_list],
                'wind_gusts_10m': [h.get('wind', {}).get('gust', h.get('wind', {}).get('speed', 0)) * 3.6 for h in hourly_list],
                'precipitation_probability': [h.get('pop', 0) * 100 for h in hourly_list],
                'snowfall': [h.get('snow', {}).get('3h', 0) / 3 for h in hourly_list], # Average per hour
                'snow_depth': [0] * len(hourly_list),
                'cape': [0] * len(hourly_list),
                'surface_pressure': [h.get('main', {}).get('pressure', 1013) for h in hourly_list],
                'temperature_2m': [h.get('main', {}).get('temp') for h in hourly_list],
            },
            'daily': {
                # Free tier doesn't give a true 'daily' object, so we use the first few forecast points
                'temperature_2m_max': [h.get('main', {}).get('temp_max') for h in hourly_list[::8]],
                'temperature_2m_min': [h.get('main', {}).get('temp_min') for h in hourly_list[::8]],
                'precipitation_probability_max': [h.get('pop', 0) * 100 for h in hourly_list[::8]],
                'snowfall_sum': [0] * 5
            }
        }
        return adapted
    except Exception as e:
        print(f"OpenWeatherMap Free-Tier fetch failed: {e}")
        return None


# --- STORM PROJECTION HELPERS (FULLY INCLUDED) ---
def project_path(lat, lon, bearing, speed_mph, hours=1):
    R = 3958.8
    distance_miles = speed_mph * hours
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    brng = math.radians(bearing)
    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_miles / R) +
        math.cos(lat1) * math.sin(distance_miles / R) * math.cos(brng)
    )
    lon2 = lon1 + math.atan2(
        math.sin(brng) * math.sin(distance_miles / R) * math.cos(lat1),
        math.cos(distance_miles / R) - math.sin(lat1) * math.sin(lat2)
    )
    lon2_deg = math.degrees(lon2)
    lon2_deg = (lon2_deg + 180) % 360 - 180
    return math.degrees(lat2), lon2_deg

def project_storm_path(start_lat, start_lon, hourly_data, hours=48):
    if not hourly_data or 'time' not in hourly_data or len(hourly_data['time']) == 0:
        return [(0, start_lat, start_lon, 0, 0, 0)]
    path = [(0, start_lat, start_lon, 0, 0, 0)]
    current_lat = float(start_lat)
    current_lon = float(start_lon)
    max_steps = min(hours, len(hourly_data['time']))
    wind_dir_list = hourly_data.get('wind_direction_10m', [0] * len(hourly_data['time']))
    wind_speed_list = hourly_data.get('wind_speed_10m', [0] * len(hourly_data['time']))
    precip_list = hourly_data.get('precipitation_probability', [0] * len(hourly_data['time']))
    cape_list = hourly_data.get('cape', [0] * len(hourly_data['time'])) if 'cape' in hourly_data else [0] * len(hourly_data['time'])
    for i in range(max_steps):
        bearing = wind_dir_list[i]
        speed_kmh = wind_speed_list[i]
        precip_prob = precip_list[i]
        cape_val = cape_list[i]
        speed_mph = speed_kmh / 1.60934 if speed_kmh > 0 else 0
        new_lat, new_lon = project_path(current_lat, current_lon, bearing, speed_mph, hours=1)
        path.append((i + 1, new_lat, new_lon, precip_prob, speed_kmh, cape_val))
        current_lat, current_lon = new_lat, new_lon
    return path

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c)

def find_closest_towns(path_lat, path_lon, max_distance=60, max_towns=3):
    nearby = []
    for town_name, town_lat, town_lon in CITY_LIST:
        dist = haversine(path_lat, path_lon, town_lat, town_lon)
        if dist <= max_distance:
            nearby.append((town_name, dist))
    nearby.sort(key=lambda x: x[1])
    return nearby[:max_towns]

# --- FULL CITY LIST (THIS WAS MISSING - CAUSING THE NameError) ---
CITY_LIST = [
    ("Los Angeles, CA", 34.05, -118.24),
    ("San Francisco, CA", 37.77, -122.42),
    ("San Diego, CA", 32.72, -117.16),
    ("Sacramento, CA", 38.58, -121.49),
    ("Fresno, CA", 36.75, -119.77),
    ("Bakersfield, CA", 35.37, -119.02),
    ("San Jose, CA", 37.34, -121.89),
    ("Oakland, CA", 37.80, -122.27),
    ("Long Beach, CA", 33.77, -118.19),
    ("Anaheim, CA", 33.84, -117.91),
    ("Riverside, CA", 33.95, -117.40),
    ("Santa Barbara, CA", 34.42, -119.70),
    ("Redding, CA", 40.59, -122.39),
    ("Eureka, CA", 40.80, -124.16),
    ("Palm Springs, CA", 33.83, -116.55),
    ("Monterey, CA", 36.60, -121.89),
    ("Stockton, CA", 37.96, -121.29),
    ("Modesto, CA", 37.64, -120.99),
    ("Santa Rosa, CA", 38.44, -122.71),
    ("Ventura, CA", 34.28, -119.29),
    ("Dallas, TX", 32.78, -96.80),
    ("Houston, TX", 29.76, -95.37),
    ("San Antonio, TX", 29.42, -98.49),
    ("Austin, TX", 30.27, -97.74),
    ("Fort Worth, TX", 32.76, -97.33),
    ("El Paso, TX", 31.76, -106.49),
    ("Corpus Christi, TX", 27.80, -97.40),
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
    ("College Station, TX", 30.63, -96.33),
    ("Laredo, TX", 27.53, -99.51),
    ("Miami, FL", 25.76, -80.19),
    ("Orlando, FL", 28.54, -81.38),
    ("Tampa, FL", 27.95, -82.46),
    ("Jacksonville, FL", 30.33, -81.66),
    ("Tallahassee, FL", 30.44, -84.28),
    ("Fort Lauderdale, FL", 26.12, -80.14),
    ("St. Petersburg, FL", 27.77, -82.64),
    ("Pensacola, FL", 30.42, -87.22),
    ("Gainesville, FL", 29.65, -82.32),
    ("Naples, FL", 26.14, -81.79),
    ("Sarasota, FL", 27.34, -82.53),
    ("Daytona Beach, FL", 29.21, -81.02),
    ("West Palm Beach, FL", 26.72, -80.05),
    ("Fort Myers, FL", 26.64, -81.87),
    ("Panama City, FL", 30.16, -85.66),
    ("New York City, NY", 40.71, -74.00),
    ("Buffalo, NY", 42.89, -78.88),
    ("Rochester, NY", 43.16, -77.61),
    ("Albany, NY", 42.65, -73.76),
    ("Syracuse, NY", 43.05, -76.15),
    ("Yonkers, NY", 40.93, -73.90),
    ("White Plains, NY", 41.03, -73.76),
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
    ("Washington DC", 38.91, -77.04),
    ("Charlotte, NC", 35.23, -80.84),
    ("Nashville, TN", 36.16, -86.78),
    ("Minneapolis, MN", 44.98, -93.27),
    ("Kansas City, MO", 39.10, -94.58),
    ("Oklahoma City, OK", 35.47, -97.52),
    ("Las Vegas, NV", 36.17, -115.14),
    ("Portland, OR", 45.52, -122.68),
    ("Salt Lake City, UT", 40.76, -111.89),
    ("New Orleans, LA", 29.95, -90.07),
    ("Baltimore, MD", 39.29, -76.61),
    ("Milwaukee, WI", 43.04, -87.91),
    ("Albuquerque, NM", 35.08, -106.61),
    ("Tucson, AZ", 32.22, -110.93),
    ("Mesa, AZ", 33.42, -111.83),
    ("Colorado Springs, CO", 38.83, -104.82),
    ("Raleigh, NC", 35.78, -78.64),
    ("Virginia Beach, VA", 36.85, -75.98),
    ("Omaha, NE", 41.26, -96.01),
    ("Cleveland, OH", 41.50, -81.69),
    ("Wichita, KS", 37.69, -97.34),
    ("Tulsa, OK", 36.15, -95.99),
    ("Honolulu, HI", 21.31, -157.86),
    ("Anchorage, AK", 61.22, -149.90),
    ("Boise, ID", 43.62, -116.20),
    ("Spokane, WA", 47.66, -117.43),
    ("Billings, MT", 45.78, -108.50),
    ("Sioux Falls, SD", 43.54, -96.73),
    ("Fargo, ND", 46.88, -96.79),
    ("Des Moines, IA", 41.59, -93.62),
    ("Little Rock, AR", 34.75, -92.29),
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
    ("Baton Rouge, LA", 30.45, -91.15),
    ("Shreveport, LA", 32.51, -93.75),
    ("Lafayette, LA", 30.22, -92.02),
    ("Springfield, MO", 37.21, -93.29),
    ("Fayetteville, AR", 36.06, -94.16),
    ("Lincoln, NE", 40.81, -96.70),
    ("Madison, WI", 43.07, -89.40),
    ("Grand Rapids, MI", 42.96, -85.67),
    ("Lansing, MI", 42.73, -84.55),
    ("Pittsburgh, PA", 40.44, -79.99),
    ("Cincinnati, OH", 39.10, -84.51),
    ("Columbus, OH", 39.96, -83.00),
    ("Toledo, OH", 41.65, -83.54),
    ("Akron, OH", 41.08, -81.52),
    ("Dayton, OH", 39.76, -84.19),
    ("St. Paul, MN", 44.95, -93.09),
    ("Duluth, MN", 46.79, -92.10),
    ("Green Bay, WI", 44.51, -88.02),
    ("Cedar Rapids, IA", 41.98, -91.67),
    ("Davenport, IA", 41.52, -90.58),
    ("Peoria, IL", 40.69, -89.59),
    ("Springfield, IL", 39.78, -89.65),
    ("Evansville, IN", 37.97, -87.57),
    ("South Bend, IN", 41.68, -86.25),
    ("Fort Wayne, IN", 41.08, -85.14),
    ("Lexington, KY", 38.04, -84.50),
    ("Huntington, WV", 38.42, -82.45),
    ("Charleston, WV", 38.35, -81.63),
    ("Wilmington, DE", 39.74, -75.55),
    ("Newark, NJ", 40.74, -74.17),
    ("Jersey City, NJ", 40.73, -74.08),
    ("Trenton, NJ", 40.22, -74.76),
    ("Camden, NJ", 39.94, -75.12),
    ("Atlantic City, NJ", 39.36, -74.42),
    ("Worcester, MA", 42.26, -71.80),
    ("Springfield, MA", 42.10, -72.59),
    ("Lowell, MA", 42.64, -71.32),
    ("Manchester, NH", 42.99, -71.46),
    ("Burlington, VT", 44.48, -73.21),
    ("Portland, ME", 43.66, -70.26),
    ("Bangor, ME", 44.80, -68.78),
    ("Concord, NH", 43.21, -71.54),
    ("New Haven, CT", 41.31, -72.92),
    ("Stamford, CT", 41.05, -73.54),
    ("Norwalk, CT", 41.12, -73.41),
    ("Waterbury, CT", 41.56, -73.05),
    ("Joplin, MO", 37.08, -94.51),
    ("Topeka, KS", 39.05, -95.68),
    ("Lawrence, KS", 38.97, -95.24),
    ("Salina, KS", 38.84, -97.61),
    ("Hays, KS", 38.87, -99.33),
    ("Garden City, KS", 37.97, -100.87),
    ("Dodge City, KS", 37.75, -100.02),
    ("Liberal, KS", 37.04, -100.92),
    ("Goodland, KS", 39.35, -101.71),
    ("Jay, OK", 36.42, -94.80),
    ("Vinita, OK", 36.64, -95.15),
    ("Muskogee, OK", 35.75, -95.37),
    ("Stillwater, OK", 36.12, -97.06),
    ("Enid, OK", 36.40, -97.88),
    ("Ponca City, OK", 36.71, -97.09),
    ("Bartlesville, OK", 36.75, -95.98),
    ("Tahlequah, OK", 35.92, -94.97),
    ("McAlester, OK", 34.93, -95.77),
    ("Durant, OK", 33.99, -96.37),
    ("Ardmore, OK", 34.17, -97.14),
    ("Lawton, OK", 34.60, -98.39),
    ("Altus, OK", 34.64, -99.33),
    ("Woodward, OK", 36.43, -99.39),
    ("Guymon, OK", 36.68, -101.48),
    ("Pampa, TX", 35.54, -100.96),
    ("Childress, TX", 34.43, -100.20),
    ("Wichita Falls, TX", 33.91, -98.49),
    ("Vernon, TX", 34.15, -99.27),
    ("Quanah, TX", 34.30, -99.74),
    ("Paducah, TX", 34.01, -100.30),
]

MONITOR_POINTS = CITY_LIST

# --- MAIN ENGINE (FULL PROCESS_LOCATION WITH STORM TRACKING + NEW 48H-BASED EXTENDED) ---
def run_system():
    conn = init_memory()
    brain = AshleyIntelligence(conn)
    ts = datetime.datetime.now(timezone.utc)
    api_name = "Open-Meteo" if USE_OPEN_METEO else "OpenWeatherMap"
    print(f"\n[SYSTEM START] ODIM-U v19.5 | UTC: {ts.strftime('%Y-%m-%d %H:%M:%SZ')} | API: {api_name}\n")
    master_report = []
    high_risk_alerts = []
    batch_size = 100

    if USE_OPEN_METEO:
        for batch_start in range(0, len(MONITOR_POINTS), batch_size):
            batch = MONITOR_POINTS[batch_start:batch_start + batch_size]
            print(f"Fetching Open-Meteo batch {batch_start//batch_size + 1}/{ (len(MONITOR_POINTS)-1)//batch_size + 1}...")
            batch_data = get_openmeteo_batch(batch)
            time.sleep(2)
            for idx, (name, lat, lon) in enumerate(batch):
                data = batch_data[idx] if idx < len(batch_data) and batch_data[idx] else None
                process_location(name, lat, lon, data, ts, conn, brain, master_report, high_risk_alerts)
    else:
        for name, lat, lon in MONITOR_POINTS:
            print(f"Fetching OpenWeatherMap data for {name}...")
            data = get_openweather_single(lat, lon)
            time.sleep(1.05)
            process_location(name, lat, lon, data, ts, conn, brain, master_report, high_risk_alerts)

    # SAVE MASTER REPORT
    try:
        master_path = os.path.join(FORECAST_DIR, f"ASHLEY_MASTER_SCAN_{ts.strftime('%Y%m%d_%H%M')}.txt")
        with open(master_path, "w", encoding="utf-8") as mf:
            mf.write(f"ODIM-U v19.5 - ASHLEY DAWN CORE NATIONAL WEATHER SCAN\n")
            mf.write(f"Scan Completed: {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
            mf.write(f"Locations Monitored: {len(MONITOR_POINTS)}\n")
            mf.write("="*80 + "\n\n")
            if high_risk_alerts:
                mf.write("🔴 HIGH RISK ALERTS 🔴\n")
                for alert in high_risk_alerts:
                    mf.write(alert + "\n")
                mf.write("\n" + "-"*60 + "\n\n")
            for entry in master_report:
                mf.write(entry + "\n\n")
            mf.write("[END OF SCAN]\n")
        print(f"\n[MASTER REPORT SAVED] {master_path}")
    except Exception as save_e:
        print(f"\n[ERROR] Failed to save master report: {save_e}")

    print(f"[SYSTEM COMPLETE] Next scan in 60 minutes...\n")
    conn.close()

def process_location(name, lat, lon, data, ts, conn, brain, master_report, high_risk_alerts):
    if not data:
        line = f"[{name}] Failed to fetch data"
        print(line)
        master_report.append(line)
        return

    current = data['current']
    hourly = data['hourly']
    daily = data['daily']

    pressure_now = current.get('surface_pressure', 1013.0)
    pressure_last = brain.get_last_pressure(name)
    precip_prob = current.get('precipitation_probability', 0)
    wind_speed = current.get('wind_speed_10m', 0)
    wind_dir = current.get('wind_direction_10m', 0)
    cape = current.get('cape', 0)
    temp = current['temperature_2m']
    humidity = current.get('relative_humidity_2m', 50)
    snowfall_list = hourly.get('snowfall', [0] * 168)
    snowfall_sum = sum(snowfall_list[:24])

    ashley_says = brain.analyze_forecast(
        pressure_now, pressure_last, precip_prob, wind_speed, cape, temp, snowfall_sum
    )

    gusts_list = hourly.get('wind_gusts_10m', [0])
    gusts = gusts_list[0] if gusts_list else 0
    risk = round(
        (precip_prob or 0) * 0.5 +
        gusts * 0.3 +
        humidity * 0.1 +
        (cape / 1000) * 20,
        1
    )

    # Short 7-day from daily (model summary - kept for quick reference)
    seven_day_short = "7D: N/A"
    if len(daily.get('temperature_2m_max', [])) >= 8:
        avg_high = sum(daily['temperature_2m_max'][1:8]) / 7
        avg_low = sum(daily['temperature_2m_min'][1:8]) / 7
        max_precip = max(daily['precipitation_probability_max'][1:8])
        total_snow = sum(daily.get('snowfall_sum', [0]*8)[1:8])
        seven_day_short = f"7D: {avg_high:.1f}/{avg_low:.1f}°C P{max_precip}% S{total_snow:.1f}cm"

    # DATABASE SAVE
    try:
        c = conn.cursor()
        c.execute('''
            INSERT INTO weather_logs
            (timestamp, location, pressure, temp, humidity, wind_speed, wind_deg, precip_prob, snowfall, snow_depth, cape, risk)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            ts, name, pressure_now, temp, humidity, wind_speed, wind_dir,
            precip_prob,
            snowfall_list[0] if snowfall_list else 0,
            hourly.get('snow_depth', [0])[0],
            cape, risk
        ))
        conn.commit()
    except Exception as db_e:
        print(f" [DB ERROR] Failed to save {name}: {db_e}")

    # STORM TRACKING + ASHLEY'S 7/14/30 DAY (only when elevated risk)
    projection = ""
    if risk > 35 or cape > 800 or snowfall_sum > 2 or precip_prob > 60:
        try:
            path = project_storm_path(lat, lon, hourly, hours=48)
            projection = "\n=== ASHLEY STORM TRACKING (48h advection) ===\n"
            for hour, p_lat, p_lon, precip, wind_kmh, path_cape in path[::6]:
                closest = find_closest_towns(p_lat, p_lon)
                towns_str = ", ".join([f"{t[0]} ({t[1]} mi)" for t in closest]) if closest else "Remote area"
                threats = []
                if path_cape > 1500:
                    threats.append("🔴 HIGH TORNADO RISK")
                elif path_cape > 800:
                    threats.append("🟡 Tornado Possible")
                if precip > 70:
                    threats.append("🌧️ Heavy Rain")
                recent_snow = sum(snowfall_list[max(0, hour-6):hour+6])
                if recent_snow > 3:
                    threats.append(f"❄️ {recent_snow:.1f}cm Snow")
                threat_str = " | " + "; ".join(threats) if threats else ""
                projection += f" +{hour:2}h: {p_lat:.4f}°, {p_lon:.4f}° → Near: {towns_str}{threat_str}\n"
            projection += "========================================\n"

            # Ashley's extended outlook - now fully derived from the 48-hour detailed data
            extended = brain.generate_extended_forecast(current, hourly)
            if extended:
                projection += "\n" + extended

            if risk > 50 or cape > 1000:
                high_risk_alerts.append(f"⚠️ HIGH ALERT: {name} | Risk: {risk} | CAPE: {cape} | {ashley_says}")
        except Exception as proj_e:
            projection = f"\n[Projection Error: {proj_e}]\n"

    line = (f"[{name}] "
            f"P: {pressure_now:.1f} mb | "
            f"T: {temp:.1f}°C | "
            f"CAPE: {cape:.0f} | "
            f"Precip: {precip_prob}% | "
            f"24h Snow: {snowfall_sum:.1f} cm | "
            f"ASHLEY SAYS: {ashley_says} | "
            f"RISK: {risk} | {seven_day_short}{projection}")

    print(line)
    master_report.append(line)

# === AUTO-RUN HOURLY ===
if __name__ == "__main__":
    print("ASHLEY DAWN CORE v19.5 - HOURLY CYCLE STARTED")
    print("All extended forecasts now derived from 48-hour detailed trends")
    print("Press Ctrl+C to stop\n")
    while True:
        try:
            run_system()
            time.sleep(3600)
        except KeyboardInterrupt:
            print("\n\nAshley Dawn Core shutting down gracefully. Stay safe!")
            break
        except Exception as e:
            print(f"\n[CRITICAL ERROR] {e}")
            time.sleep(300)
