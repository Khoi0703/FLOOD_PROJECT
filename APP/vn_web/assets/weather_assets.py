import pickle
import os
import time
import pandas as pd
from utils.geojson_utils import load_geojson, get_province_coordinates
from utils.weather_utils import get_weather_forecast

CACHE_FILE = "weather_cache.pkl"
CACHE_EXPIRY = 3600  # 1 giờ

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            if os.path.getsize(CACHE_FILE) == 0:
                return {}
            with open(CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
                if time.time() - cache["timestamp"] < CACHE_EXPIRY:
                    return cache["data"]
        except Exception:
            # Nếu file lỗi, xóa file cache để tránh lỗi lần sau
            os.remove(CACHE_FILE)
            return {}
    return {}

def save_cache(data):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump({"timestamp": time.time(), "data": data}, f)

def vietnam_geojson():
    return load_geojson()

def province_coordinates(vietnam_geojson):
    coordinates = {}
    for feature in vietnam_geojson["features"]:
        province = feature["properties"]["ten_tinh"]
        lat, lon = get_province_coordinates(province, vietnam_geojson)
        if lat and lon:
            coordinates[province] = {"lat": lat, "lon": lon}
    return coordinates

def weather_forecasts(province_coordinates):
    cache = load_cache()
    weather_data = {}
    for province, coords in province_coordinates.items():
        if province in cache:
            weather_data[province] = cache[province]
        else:
            forecast, total_rain = get_weather_forecast(coords["lat"], coords["lon"])
            weather_data[province] = {
                "forecast": forecast,
                "total_rain": total_rain
            }
    save_cache(weather_data)
    return weather_data
