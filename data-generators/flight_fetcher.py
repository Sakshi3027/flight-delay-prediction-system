"""
Flight Data Fetcher - Streams real-time flight data from OpenSky Network
"""
import requests
import json
import time
import pandas as pd
from datetime import datetime

class FlightDataFetcher:
    def __init__(self):
        self.base_url = "https://opensky-network.org/api/states/all"
        
    def fetch_flights(self, bbox=None):
        """
        Fetch current flight data from OpenSky Network
        bbox: (min_lat, max_lat, min_lon, max_lon) for geographic filtering
        Example for USA: (24.396308, 49.384358, -125.0, -66.93457)
        """
        try:
            params = {}
            if bbox:
                params = {
                    'lamin': bbox[0],
                    'lamax': bbox[1], 
                    'lomin': bbox[2],
                    'lomax': bbox[3]
                }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self.parse_flight_data(data)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching flight data: {e}")
            return None
    
    def parse_flight_data(self, raw_data):
        """Parse raw OpenSky data into structured format"""
        if not raw_data or 'states' not in raw_data:
            return None
        
        flights = []
        timestamp = raw_data.get('time', int(time.time()))
        
        for state in raw_data['states']:
            flight = {
                'icao24': state[0],           # Unique aircraft ID
                'callsign': state[1].strip() if state[1] else 'N/A',
                'origin_country': state[2],
                'time_position': state[3],
                'last_contact': state[4],
                'longitude': state[5],
                'latitude': state[6],
                'baro_altitude': state[7],    # meters
                'on_ground': state[8],
                'velocity': state[9],          # m/s
                'true_track': state[10],       # degrees
                'vertical_rate': state[11],    # m/s
                'sensors': state[12],
                'geo_altitude': state[13],     # meters
                'squawk': state[14],
                'spi': state[15],
                'position_source': state[16],
                'timestamp': timestamp,
                'fetch_time': datetime.now().isoformat()
            }
            flights.append(flight)
        
        return flights
    
    def display_flights(self, flights, limit=10):
        """Display flight information in a readable format"""
        if not flights:
            print("No flights data available")
            return
        
        print(f"\n{'='*80}")
        print(f"Live Flight Data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total flights: {len(flights)}")
        print(f"{'='*80}\n")
        
        for i, flight in enumerate(flights[:limit]):
            if flight['on_ground']:
                continue
                
            print(f"Flight #{i+1}")
            print(f"  Callsign: {flight['callsign']}")
            print(f"  Country: {flight['origin_country']}")
            print(f"  Position: ({flight['latitude']:.4f}, {flight['longitude']:.4f})")
            print(f"  Altitude: {flight['baro_altitude']}m ({flight['baro_altitude']*3.28084:.0f}ft)")
            print(f"  Velocity: {flight['velocity']}m/s ({flight['velocity']*2.23694:.0f}mph)")
            print(f"  Heading: {flight['true_track']}°")
            print(f"  Vertical Rate: {flight['vertical_rate']}m/s")
            print("-" * 40)

def main():
    """Test the flight data fetcher"""
    print("Starting Flight Data Fetcher...")
    print("Fetching live flight data from OpenSky Network...\n")
    
    fetcher = FlightDataFetcher()
    
    # Fetch flights over USA (you can change this bbox)
    # USA bounding box: (min_lat, max_lat, min_lon, max_lon)
    usa_bbox = (24.396308, 49.384358, -125.0, -66.93457)
    
    # For testing, let's fetch 3 times with 10 second intervals
    for i in range(3):
        print(f"\n{'#'*80}")
        print(f"Fetch #{i+1}")
        print(f"{'#'*80}")
        
        flights = fetcher.fetch_flights(bbox=usa_bbox)
        
        if flights:
            fetcher.display_flights(flights, limit=5)
            
            # Save to JSON for later analysis
            filename = f"data-generators/sample_flights_{int(time.time())}.json"
            with open(filename, 'w') as f:
                json.dump(flights, f, indent=2)
            print(f"\n✅ Saved {len(flights)} flights to {filename}")
        else:
            print("❌ Failed to fetch flight data")
        
        if i < 2:  # Don't wait after last iteration
            print("\nWaiting 10 seconds before next fetch...")
            time.sleep(10)
    
    print("\n" + "="*80)
    print("Flight data fetcher test complete!")
    print("="*80)

if __name__ == "__main__":
    main()