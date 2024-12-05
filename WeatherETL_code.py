import requests
import pandas as pd
import mysql.connector

from datetime import date,datetime
# Fetch weather data from weatherapi
API_KEY = "yourapikey"
BASE_URL = "https://api.weatherapi.com/v1/forecast.json"
dt = date.today()
days = 14


def fetch_weather_data(cities):
    weather_data = []
    for city in cities:
        params = {"key": API_KEY, "q": city, "aqi": "no", "dt":dt,"days":days}
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            for index in data["forecast"]["forecastday"]:  # Loop over forecast days (0-14)
                for hour in index["hour"]:  # Loop over hours (0:00-23:00)
                    weather_data.append({
                        "City": data["location"]["name"],
                        "Latitude": data["location"]["lat"],
                        "Longitude": data["location"]["lon"],
                        "Last_refresh": data["current"]["last_updated"],
                        "Forecast_Date_Time": hour["time"],
                        "Forecast_Temp_C": hour["temp_c"],
                        "Forecast_Condition": hour["condition"]["text"],
                        "Forecast_Humidity": hour["humidity"],
                        "Forecast_Wind_kph": hour["wind_kph"],
                        "timestamp": datetime.now()
                    })


        else:
            print(f"Failed to fetch weather data for {city}: {response.status_code} - {response.text}")
    return weather_data
#cities = ["Moscow", "Kyiv", "Paris", "Stockholm", "Berlin", "Finland", "Helsinki", "Oslo", "Warsaw", "Rome", "London"]
# weather_df = fetch_weather_data(cities)
# weather_df.to_csv("the_csv.csv", index=False)
# print(weather_df)

from sqlalchemy import create_engine
# Database credentials and engine setup
DB_USERNAME = "yourusername"
DB_PASSWORD = "yourpassword"
DB_HOST = "xxxxhost"
DB_NAME = "xxxx_DATABASE"

engine = create_engine(f"mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Load data into the database
def load_weather_data(data, engine):
    df = pd.DataFrame(data)
    df.to_sql("weather_data", con=engine, if_exists="append", index=False)

# Main script
if __name__ == "__main__":
    cities = ["Moscow", "Kyiv", "Paris", "Stockholm", "Berlin", "Finland", "Helsinki", "Oslo", "Warsaw", "Rome", "London"]
    weather_data = fetch_weather_data(cities)
    if weather_data:
        load_weather_data(weather_data, engine)
        print("Weather data successfully loaded into the database.")
    else:
        print("No data to load.")