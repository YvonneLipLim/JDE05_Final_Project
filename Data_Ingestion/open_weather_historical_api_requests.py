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

# Set up date range for recent data
end_date = datetime.now() - timedelta(days=1)  # Yesterday
start_date = end_date - timedelta(days=30)  

# Process one day at a time
CHUNK_SIZE = 1
MAX_RETRIES = 3

# Create empty list to store weather data
weather_data = []

# Define the directory path
output_dir = "/Users/yvonnelip/JDE05_Final_Project/Datasets"
os.makedirs(output_dir, exist_ok=True)

# Process each city
for city_name, coordinates in CITIES.items():
    print(f"Processing {city_name}...")
    current_date = start_date
    
    while current_date <= end_date:
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # Historical data endpoint
                url = "http://history.openweathermap.org/data/2.5/history/city"
                
                # Calculate end time for this chunk
                chunk_end = min(current_date + timedelta(days=CHUNK_SIZE), end_date)
                
                params = {
                    "lat": coordinates["lat"],
                    "lon": coordinates["lon"],
                    "type": "hour",
                    "start": int(current_date.timestamp()),
                    "cnt": 24, # Request 24 hours of data instead of using end parameter
                    "appid": API_KEY,
                    "units": "metric"
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract weather data
                    for item in data.get('list', []):
                        weather_data.append({
                            'City': city_name,
                            'Date': datetime.fromtimestamp(item['dt']).strftime('%Y-%m-%d %H:%M:%S'),
                            'Temperature (°C)': item['main']['temp'],
                            'Humidity (%)': item['main']['humidity'],
                            'Description': item['weather'][0]['description'],
                            'Wind Speed (m/s)': item['wind'].get('speed', 0),
                            'Pressure (hPa)': item['main']['pressure']
                        })
                    
                    print(f"Collected data for {city_name}: {current_date.date()}")
                    break
                else:
                    print(f"Error for {city_name} on {current_date.date()}: {response.status_code}, {response.text}")
                    retry_count += 1
                    time.sleep(5)  # Wait longer between retries
                
            except Exception as e:
                print(f"Error processing {city_name} for {current_date.date()}: {str(e)}")
                retry_count += 1
                time.sleep(5)
        
        # Move to next day
        current_date = chunk_end + timedelta(days=1)
        time.sleep(2)  # Respect API rate limits

# Convert to DataFrame
df = pd.DataFrame(weather_data)

# Save files with timestamp and date range
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
json_filename = os.path.join(output_dir, f"brazil_cities_weather_{date_range}_{timestamp}.json")
csv_filename = os.path.join(output_dir, f"brazil_cities_weather_{date_range}_{timestamp}.csv")

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
