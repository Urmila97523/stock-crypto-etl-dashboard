import requests
import pandas as pd
import mysql.connector
from datetime import datetime
import time

#  Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "1234",
    "database": "multi_api_db"
}

#  API Configuration
COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"
ALPHA_VANTAGE_API = "https://www.alphavantage.co/query"
WEATHER_API = "https://api.openweathermap.org/data/2.5/weather"

#  API Keys
ALPHA_VANTAGE_KEY = "I36JU8ZF5TA9DA9H"
WEATHER_KEY = "9063db1302c30c913f92968d3281099a"

#  Retry Mechanism for Robust API Requests
def safe_request(url, params, retries=3, delay=2):
    """Handle API requests with retries and logging"""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()

            print(f" Attempt {attempt + 1} failed with status {response.status_code}")
            time.sleep(delay)

        except Exception as e:
            print(f"Error: {e}")

    print(f" Failed to fetch data after {retries} attempts.")
    return None

#  Fetch Crypto Data from CoinGecko
def fetch_crypto_data(pages=3, per_page=50):
    all_data = []

    for page in range(1, pages + 1):
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false"
        }

        data = safe_request(COINGECKO_API, params)

        if data:
            all_data.extend(data)
        else:
            print(f" Failed to fetch crypto data on page {page}. Skipping...")
            continue

        time.sleep(2)

    return pd.DataFrame(all_data)

#  Fetch Stock Data from Alpha Vantage
def fetch_stock_data(symbols=["IBM", "AAPL"]):
    all_records = []

    for symbol in symbols:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_VANTAGE_KEY
        }

        data = safe_request(ALPHA_VANTAGE_API, params)

        if data and "Time Series (Daily)" in data:
            for date, stats in data["Time Series (Daily)"].items():
                all_records.append({
                    "symbol": symbol,
                    "date": date,
                    "open_price": float(stats["1. open"]),
                    "high_price": float(stats["2. high"]),
                    "low_price": float(stats["3. low"]),
                    "close_price": float(stats["4. close"]),
                    "volume": int(stats["5. volume"])
                })
        else:
            print(f" Failed to fetch stock data for {symbol}")

        time.sleep(2)

    return pd.DataFrame(all_records)

#  Fetch Weather Data from OpenWeatherMap
def fetch_weather_data(cities=["New York", "Los Angeles", "Chicago"]):
    all_weather = []

    for city in cities:
        params = {
            "q": city,
            "appid": WEATHER_KEY,
            "units": "metric"
        }

        data = safe_request(WEATHER_API, params)

        if data and "main" in data:
            all_weather.append({
                "city": city,
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
                "weather_description": data["weather"][0]["description"],
                "recorded_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  #  Correct format
            })
        else:
            print(f" Failed to fetch weather data for {city}")

        time.sleep(1)

    return pd.DataFrame(all_weather)

#  Improved function to load data into MySQL with batch insertion
def load_to_mysql(df, table_name):
    if df.empty:
        print(f"No data to insert into {table_name}. Skipping...")
        return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if table_name == "crypto_data":
            sql = """
                INSERT INTO crypto_data 
                (id, name, current_price, market_cap, total_volume, price_change_24h, ath, atl, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    row['id'], row['name'], row['current_price'], row['market_cap'],
                    row['total_volume'], row['price_change_percentage_24h'],
                    row['ath'], row['atl'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
                for _, row in df.iterrows()
            ]

        elif table_name == "stock_data":
            sql = """
                INSERT INTO stock_data 
                (symbol, date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    row['symbol'], row['date'], row['open_price'], row['high_price'],
                    row['low_price'], row['close_price'], row['volume']
                )
                for _, row in df.iterrows()
            ]

        elif table_name == "weather_data":
            sql = """
                INSERT INTO weather_data 
                (city, temperature, humidity, wind_speed, weather_description, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    row['city'], row['temperature'], row['humidity'], row['wind_speed'],
                    row['weather_description'], row['recorded_at']
                )
                for _, row in df.iterrows()
            ]

        cursor.executemany(sql, values)
        conn.commit()

        print(f" {len(values)} rows inserted into {table_name}")

    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

    finally:
        cursor.close()
        conn.close()

# Main ETL Execution
print("Starting ETL pipeline...")

# Fetch data
crypto_df = fetch_crypto_data()
stock_df = fetch_stock_data(["IBM", "AAPL", "GOOGL"])  # Multiple stocks
weather_df = fetch_weather_data(["New York", "Los Angeles", "Chicago"])  # Multiple cities

# Load into MySQL
load_to_mysql(crypto_df, "crypto_data")
load_to_mysql(stock_df, "stock_data")
load_to_mysql(weather_df, "weather_data")

print("ETL pipeline completed successfully!")
