# main.py

import requests
import json
import os
from datetime import datetime, timezone
import time
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# ---- CONFIG ----
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

# ---- LOAD CITY JSON ----
def load_city_data():
    with open("DB-Data/germany_city_data.json", "r", encoding="utf-8") as f:
        return json.load(f)

# ---- FETCH WEATHER ----
def fetch_weather(lat, lon):
    url = (
        f"https://api.openweathermap.org/data/2.5/weather?"
        f"lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# ---- SAVE TO MONGODB ----
def save_weather_to_mongodb(city_id, w):
    client = MongoClient(
        MONGO_URI,
        server_api=ServerApi("1")
    )
    db = client["germany_weather_db"]
    collection = db.weather_data

    # Convert timestamps to UTC datetime
    sunrise_utc = datetime.fromtimestamp(w["sys"]["sunrise"], tz=timezone.utc)
    sunset_utc = datetime.fromtimestamp(w["sys"]["sunset"], tz=timezone.utc)

    document = {
        "datetime_in_utc": datetime.now(timezone.utc),
        "city_id": city_id,
        "sunrise_in_utc": sunrise_utc,
        "sunset_in_utc": sunset_utc,
        "weather_icon": w["weather"][0]["icon"],
        "weather_description": w["weather"][0]["description"],
        "snow_1h": w.get("snow", {}).get("1h"),
        "rain_1h": w.get("rain", {}).get("1h"),
        "visibility": w.get("visibility"),
        "temperature": w["main"]["temp"],
        "feels_like": w["main"]["feels_like"],
        "cloud": w["clouds"]["all"],
        "humidity": w["main"]["humidity"],
        "pressure": w["main"]["pressure"],
        "wind_deg": w["wind"].get("deg"),
        "wind_speed": w["wind"]["speed"],
        "timezone_offset": w.get("timezone")
    }

    collection.insert_one(document)
    client.close()


# ---- MAIN FUNCTION ----
def main():
    cities = load_city_data()

    for city in cities:
        print(f"Fetching weather for {city['city_id']}...")

        lat = city["latitude"]
        lon = city["longitude"]
        city_id = city["city_id"]

        try:
            weather_json = fetch_weather(lat, lon)
            save_weather_to_mongodb(city_id, weather_json)
        except Exception as e:
            print(f"Error fetching/saving weather for {city_id}: {e}")

        time.sleep(1.2)

    print("Weather data successfully saved to MongoDB!")


# ---- RUN ----
if __name__ == "__main__":
    main()
