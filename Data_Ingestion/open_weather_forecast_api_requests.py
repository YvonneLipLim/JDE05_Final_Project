import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
import time

# OpenWeatherMap API key
API_KEY = "API Key"

# Define Brazilian cities with their coordinates
CITIES = {
    "Sao_Paulo": {"lat": "-23.5505", "lon": "-46.6333"},
    "Rio_de_Janeiro": {"lat": "-22.9068", "lon": "-43.1729"},
    "Brasilia": {"lat": "-15.7975", "lon": "-47.8919"},
    "Salvador": {"lat": "-12.9714", "lon": "-38.5014"},
    "Curitiba": {"lat": "-25.4284", "lon": "-49.2733"}
}

# API endpoint for 5 day forecast (using Developer plan access)
url = "http://api.openweathermap.org/data/2.5/forecast"

# Create empty list to store weather data
weather_data = []

# Define the directory path
output_dir = "/Users/yvonnelip/JDE05_Final_Project/Datasets"
os.makedirs(output_dir, exist_ok=True)

# Collect data for each city
for city_name, coordinates in CITIES.items():
    params = {
        "lat": coordinates["lat"],
        "lon": coordinates["lon"],
        "appid": API_KEY,
        "units": "metric"
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract weather data
            for item in data['list']:
                weather_data.append({
                    'City': city_name,
                    'Date': datetime.fromtimestamp(item['dt']).strftime('%Y-%m-%d %H:%M:%S'),
                    'Temperature (°C)': item['main']['temp'],
                    'Humidity (%)': item['main']['humidity'],
                    'Description': item['weather'][0]['description'],
                    'Wind Speed (m/s)': item['wind']['speed'],
                    'Pressure (hPa)': item['main']['pressure']
                })
            
            print(f"Collected forecast data for: {city_name}")
            
        else:
            print(f"Error for {city_name}: {response.status_code}, {response.text}")
        
        # Add delay to respect API rate limits
        time.sleep(1)
        
    except Exception as e:
        print(f"Error processing {city_name}: {str(e)}")

# Convert to DataFrame
df = pd.DataFrame(weather_data)

# Save files
json_filename = os.path.join(output_dir, "brazil_cities_weather_forecast.json")
csv_filename = os.path.join(output_dir, "brazil_cities_weather_forecast.csv")

# Save as JSON
with open(json_filename, "w") as f:
    json.dump({"data": weather_data}, f, indent=4)

# Save as CSV
df.to_csv(csv_filename, index=False)

print(f"\nData collection completed.")
print(f"Files saved to:\n{json_filename}\n{csv_filename}")
print(f"\nTotal records collected: {len(weather_data)}")
print("\nFirst few rows of the data:")
print(df.head())
