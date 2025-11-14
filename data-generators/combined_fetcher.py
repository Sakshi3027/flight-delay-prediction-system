"""
Combined Flight + Weather Data Fetcher
Streams real-time flight data and enriches it with weather information
"""
import json
import time
import pandas as pd
from datetime import datetime
from flight_fetcher import FlightDataFetcher
from weather_fetcher import WeatherDataFetcher

class CombinedDataStream:
    def __init__(self):
        self.flight_fetcher = FlightDataFetcher()
        self.weather_fetcher = WeatherDataFetcher()
        
    def enrich_flights_with_weather(self, flights, sample_size=20):
        """
        Add weather data to flight records
        Uses sampling to avoid rate limiting (we can't call weather API for all 5000+ flights)
        """
        print(f"\nEnriching {sample_size} flights with weather data...")
        
        enriched_flights = []
        
        # Sample flights that are in the air (not on ground)
        airborne_flights = [f for f in flights if not f['on_ground'] and f['latitude'] and f['longitude']]
        
        if len(airborne_flights) < sample_size:
            sample_size = len(airborne_flights)
        
        # Take evenly distributed samples
        step = max(1, len(airborne_flights) // sample_size)
        sampled_flights = airborne_flights[::step][:sample_size]
        
        print(f"Processing {len(sampled_flights)} flights...")
        
        for i, flight in enumerate(sampled_flights):
            print(f"  [{i+1}/{len(sampled_flights)}] Processing {flight['callsign']}...", end=' ')
            
            # Fetch weather for flight location
            weather = self.weather_fetcher.fetch_weather(flight['latitude'], flight['longitude'])
            
            if weather:
                assessment = self.weather_fetcher.assess_flight_conditions(weather)
                
                # Combine flight and weather data
                enriched = {
                    **flight,  # All flight data
                    'weather': weather,
                    'delay_assessment': assessment
                }
                enriched_flights.append(enriched)
                print(f"✓ Risk: {assessment['risk_level']}")
            else:
                print("✗ Weather fetch failed")
            
            # Rate limiting - be nice to the API
            if i < len(sampled_flights) - 1:
                time.sleep(0.5)
        
        return enriched_flights
    
    def create_ml_dataset(self, enriched_flights):
        """
        Convert enriched flight data into a flat dataset suitable for ML
        """
        dataset = []
        
        for flight in enriched_flights:
            record = {
                # Flight features
                'callsign': flight['callsign'],
                'origin_country': flight['origin_country'],
                'latitude': flight['latitude'],
                'longitude': flight['longitude'],
                'altitude_m': flight['baro_altitude'],
                'velocity_ms': flight['velocity'],
                'vertical_rate_ms': flight['vertical_rate'],
                'heading_deg': flight['true_track'],
                
                # Weather features
                'temperature_c': flight['weather']['temperature'],
                'humidity_pct': flight['weather']['humidity'],
                'pressure_hpa': flight['weather']['pressure'],
                'wind_speed_ms': flight['weather']['wind_speed'],
                'wind_gusts_ms': flight['weather']['wind_gusts'],
                'cloud_cover_pct': flight['weather']['clouds'],
                'precipitation_mm': flight['weather']['precipitation'],
                'weather_condition': flight['weather']['weather_main'],
                
                # Target variable
                'delay_risk': flight['delay_assessment']['risk_level'],
                'delay_probability': flight['delay_assessment']['delay_probability'],
                
                # Metadata
                'timestamp': flight['timestamp'],
                'fetch_time': flight['fetch_time']
            }
            dataset.append(record)
        
        return pd.DataFrame(dataset)
    
    def display_summary(self, enriched_flights):
        """Display summary statistics"""
        print(f"\n{'='*80}")
        print("DATA COLLECTION SUMMARY")
        print(f"{'='*80}")
        
        total = len(enriched_flights)
        high_risk = sum(1 for f in enriched_flights if f['delay_assessment']['risk_level'] == 'HIGH')
        medium_risk = sum(1 for f in enriched_flights if f['delay_assessment']['risk_level'] == 'MEDIUM')
        low_risk = sum(1 for f in enriched_flights if f['delay_assessment']['risk_level'] == 'LOW')
        
        print(f"Total enriched flights: {total}")
        print(f"High risk: {high_risk} ({high_risk/total*100:.1f}%)")
        print(f"Medium risk: {medium_risk} ({medium_risk/total*100:.1f}%)")
        print(f"Low risk: {low_risk} ({low_risk/total*100:.1f}%)")
        
        # Show some examples of high-risk flights
        high_risk_flights = [f for f in enriched_flights if f['delay_assessment']['risk_level'] == 'HIGH']
        if high_risk_flights:
            print(f"\n{'='*80}")
            print("HIGH RISK FLIGHTS")
            print(f"{'='*80}")
            for flight in high_risk_flights[:3]:
                print(f"\nCallsign: {flight['callsign']}")
                print(f"Location: ({flight['latitude']:.2f}, {flight['longitude']:.2f})")
                print(f"Altitude: {flight['baro_altitude']}m")
                print(f"Weather: {flight['weather']['weather_description']}")
                print(f"Risk factors: {', '.join(flight['delay_assessment']['risk_factors'])}")

def main():
    """Main execution"""
    print("="*80)
    print("REAL-TIME FLIGHT DELAY PREDICTION - DATA COLLECTION")
    print("="*80)
    
    stream = CombinedDataStream()
    
    # Step 1: Fetch flights
    print("\n[1/4] Fetching live flight data...")
    usa_bbox = (24.396308, 49.384358, -125.0, -66.93457)
    flights = stream.flight_fetcher.fetch_flights(bbox=usa_bbox)
    
    if not flights:
        print("❌ Failed to fetch flight data")
        return
    
    print(f"✅ Retrieved {len(flights)} flights")
    
    # Step 2: Enrich with weather
    print("\n[2/4] Enriching flights with weather data...")
    enriched_flights = stream.enrich_flights_with_weather(flights, sample_size=30)
    print(f"✅ Enriched {len(enriched_flights)} flights with weather data")
    
    # Step 3: Create ML dataset
    print("\n[3/4] Creating ML-ready dataset...")
    df = stream.create_ml_dataset(enriched_flights)
    print(f"✅ Created dataset with {len(df)} records and {len(df.columns)} features")
    
    # Step 4: Save everything
    print("\n[4/4] Saving data...")
    
    timestamp = int(time.time())
    
    # Save enriched JSON
    json_filename = f"data-generators/enriched_flights_{timestamp}.json"
    with open(json_filename, 'w') as f:
        json.dump(enriched_flights, f, indent=2)
    print(f"✅ Saved enriched data: {json_filename}")
    
    # Save CSV for ML
    csv_filename = f"data-generators/ml_dataset_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"✅ Saved ML dataset: {csv_filename}")
    
    # Display summary
    stream.display_summary(enriched_flights)
    
    print("\n" + "="*80)
    print("Data collection complete! Ready for Kafka + Spark Streaming")
    print("="*80)
    
    # Show sample of the dataset
    print("\nSample of ML dataset:")
    print(df[['callsign', 'temperature_c', 'wind_speed_ms', 'weather_condition', 'delay_risk']].head(10))

if __name__ == "__main__":
    main()