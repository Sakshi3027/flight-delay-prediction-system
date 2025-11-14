"""
Weather Data Fetcher - Gets real-time weather data using Open-Meteo (FREE, no API key!)
"""
import requests
from datetime import datetime
import time

class WeatherDataFetcher:
    def __init__(self):
        # Open-Meteo is completely free, no API key needed!
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        
    def fetch_weather(self, lat, lon):
        """
        Fetch current weather for a specific location using Open-Meteo
        """
        try:
            params = {
                'latitude': lat,
                'longitude': lon,
                'current': 'temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m',
                'temperature_unit': 'celsius',
                'wind_speed_unit': 'ms',
                'precipitation_unit': 'mm'
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self.parse_weather_data(data, lat, lon)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None
    
    def get_weather_description(self, code):
        """Convert WMO weather code to description"""
        weather_codes = {
            0: ('Clear sky', 'Clear'),
            1: ('Mainly clear', 'Clear'),
            2: ('Partly cloudy', 'Clouds'),
            3: ('Overcast', 'Clouds'),
            45: ('Foggy', 'Fog'),
            48: ('Depositing rime fog', 'Fog'),
            51: ('Light drizzle', 'Drizzle'),
            53: ('Moderate drizzle', 'Drizzle'),
            55: ('Dense drizzle', 'Drizzle'),
            61: ('Slight rain', 'Rain'),
            63: ('Moderate rain', 'Rain'),
            65: ('Heavy rain', 'Rain'),
            71: ('Slight snow', 'Snow'),
            73: ('Moderate snow', 'Snow'),
            75: ('Heavy snow', 'Snow'),
            77: ('Snow grains', 'Snow'),
            80: ('Slight rain showers', 'Rain'),
            81: ('Moderate rain showers', 'Rain'),
            82: ('Violent rain showers', 'Rain'),
            85: ('Slight snow showers', 'Snow'),
            86: ('Heavy snow showers', 'Snow'),
            95: ('Thunderstorm', 'Thunderstorm'),
            96: ('Thunderstorm with slight hail', 'Thunderstorm'),
            99: ('Thunderstorm with heavy hail', 'Thunderstorm')
        }
        return weather_codes.get(code, ('Unknown', 'Unknown'))
    
    def parse_weather_data(self, raw_data, lat, lon):
        """Parse raw weather data into structured format"""
        current = raw_data['current']
        
        description, main = self.get_weather_description(current['weather_code'])
        
        weather = {
            'latitude': lat,
            'longitude': lon,
            'temperature': current['temperature_2m'],
            'humidity': current['relative_humidity_2m'],
            'pressure': current['pressure_msl'],
            'surface_pressure': current['surface_pressure'],
            'wind_speed': current['wind_speed_10m'],
            'wind_direction': current['wind_direction_10m'],
            'wind_gusts': current['wind_gusts_10m'],
            'clouds': current['cloud_cover'],
            'precipitation': current['precipitation'],
            'rain': current['rain'],
            'snowfall': current['snowfall'],
            'weather_code': current['weather_code'],
            'weather_main': main,
            'weather_description': description,
            'fetch_time': datetime.now().isoformat()
        }
        return weather
    
    def assess_flight_conditions(self, weather):
        """Assess if weather conditions might cause flight delays"""
        risk_factors = []
        risk_level = 'LOW'
        
        # Check various weather conditions
        if weather['clouds'] > 90:
            risk_factors.append(f"Heavy cloud cover: {weather['clouds']}%")
            risk_level = 'MEDIUM'
        
        if weather['wind_speed'] > 15:  # m/s (33 mph)
            risk_factors.append(f"High winds: {weather['wind_speed']:.1f}m/s ({weather['wind_speed']*2.237:.0f}mph)")
            risk_level = 'HIGH' if weather['wind_speed'] > 20 else 'MEDIUM'
        
        if weather['wind_gusts'] > 20:
            risk_factors.append(f"Wind gusts: {weather['wind_gusts']:.1f}m/s ({weather['wind_gusts']*2.237:.0f}mph)")
            risk_level = 'HIGH'
        
        if weather['weather_main'] in ['Thunderstorm', 'Snow']:
            risk_factors.append(f"Severe weather: {weather['weather_main']}")
            risk_level = 'HIGH'
        
        if weather['weather_main'] == 'Rain' and weather['rain'] > 5:
            risk_factors.append(f"Heavy rain: {weather['rain']}mm/h")
            risk_level = 'MEDIUM'
        
        if weather['weather_main'] == 'Fog':
            risk_factors.append(f"Low visibility: Fog")
            risk_level = 'HIGH'
        
        if weather['snowfall'] > 0:
            risk_factors.append(f"Snowfall: {weather['snowfall']}mm")
            risk_level = 'HIGH'
        
        return {
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'delay_probability': self.calculate_delay_probability(weather, risk_level)
        }
    
    def calculate_delay_probability(self, weather, risk_level):
        """Simple delay probability calculation"""
        if risk_level == 'HIGH':
            return 0.7
        elif risk_level == 'MEDIUM':
            return 0.3
        else:
            return 0.1
    
    def display_weather(self, weather, assessment):
        """Display weather information in readable format"""
        print(f"\n{'='*70}")
        print(f"Weather at ({weather['latitude']:.2f}, {weather['longitude']:.2f})")
        print(f"{'='*70}")
        print(f"Conditions: {weather['weather_main']} - {weather['weather_description']}")
        print(f"Temperature: {weather['temperature']}°C")
        print(f"Pressure: {weather['pressure']} hPa")
        print(f"Humidity: {weather['humidity']}%")
        print(f"Wind: {weather['wind_speed']:.1f}m/s ({weather['wind_speed']*2.237:.0f}mph) from {weather['wind_direction']}°")
        if weather['wind_gusts'] > 0:
            print(f"Wind gusts: {weather['wind_gusts']:.1f}m/s ({weather['wind_gusts']*2.237:.0f}mph)")
        print(f"Cloud cover: {weather['clouds']}%")
        
        if weather['precipitation'] > 0:
            print(f"Precipitation: {weather['precipitation']}mm")
        if weather['rain'] > 0:
            print(f"Rain: {weather['rain']}mm")
        if weather['snowfall'] > 0:
            print(f"Snowfall: {weather['snowfall']}mm")
        
        print(f"\n{'='*70}")
        print(f"DELAY ASSESSMENT")
        print(f"{'='*70}")
        print(f"Risk Level: {assessment['risk_level']}")
        print(f"Delay Probability: {assessment['delay_probability']*100:.0f}%")
        if assessment['risk_factors']:
            print(f"Risk Factors:")
            for factor in assessment['risk_factors']:
                print(f"  ⚠️  {factor}")
        else:
            print("✅ Good flying conditions")

def main():
    """Test the weather fetcher with major US airports"""
    print("Starting Weather Data Fetcher (Open-Meteo - No API Key Required!)")
    print("Fetching weather for major airports...\n")
    
    fetcher = WeatherDataFetcher()
    
    # Major US airports (name, lat, lon)
    airports = [
        ("JFK - New York", 40.6413, -73.7781),
        ("LAX - Los Angeles", 33.9416, -118.4085),
        ("ORD - Chicago", 41.9742, -87.9073),
        ("DFW - Dallas", 32.8998, -97.0403),
        ("DEN - Denver", 39.8561, -104.6737),
    ]
    
    for airport_name, lat, lon in airports:
        print(f"\n{'#'*70}")
        print(f"Checking: {airport_name}")
        print(f"{'#'*70}")
        
        weather = fetcher.fetch_weather(lat, lon)
        
        if weather:
            assessment = fetcher.assess_flight_conditions(weather)
            fetcher.display_weather(weather, assessment)
        else:
            print(f"❌ Failed to fetch weather for {airport_name}")
        
        # Be nice to the free API
        time.sleep(0.5)
    
    print("\n" + "="*70)
    print("Weather data fetcher test complete!")
    print("="*70)

if __name__ == "__main__":
    main()