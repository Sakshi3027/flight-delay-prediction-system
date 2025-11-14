"""
Kafka Producer - Streams flight + weather data to Kafka
"""
import json
import time
import sys
import os
from confluent_kafka import Producer

# Add data-generators to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data-generators'))

from flight_fetcher import FlightDataFetcher
from weather_fetcher import WeatherDataFetcher

class FlightDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer"""
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'flight-producer'
        }
        self.producer = Producer(conf)
        
        self.flight_fetcher = FlightDataFetcher()
        self.weather_fetcher = WeatherDataFetcher()
        
        self.topic_flights = 'flight-data'
        self.topic_enriched = 'enriched-flight-data'
        
        print(f"✅ Kafka Producer initialized")
        print(f"   Topics: {self.topic_flights}, {self.topic_enriched}")
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            print(f'❌ Message delivery failed: {err}')
        # Silently succeed - don't print for every message
    
    def send_flight_data(self, flight):
        """Send raw flight data to Kafka"""
        try:
            self.producer.produce(
                self.topic_flights,
                key=flight['icao24'].encode('utf-8'),
                value=json.dumps(flight).encode('utf-8'),
                callback=self.delivery_callback
            )
            return True
        except Exception as e:
            print(f"❌ Failed to send flight data: {e}")
            return False
    
    def send_enriched_data(self, enriched_flight):
        """Send enriched flight data (with weather) to Kafka"""
        try:
            self.producer.produce(
                self.topic_enriched,
                key=enriched_flight['icao24'].encode('utf-8'),
                value=json.dumps(enriched_flight).encode('utf-8'),
                callback=self.delivery_callback
            )
            return True
        except Exception as e:
            print(f"❌ Failed to send enriched data: {e}")
            return False
    
    def stream_flights(self, duration_seconds=300, sample_weather=10):
        """
        Stream flight data continuously
        
        Args:
            duration_seconds: How long to stream (default 5 minutes)
            sample_weather: How many flights per batch to enrich with weather
        """
        print(f"\n{'='*80}")
        print("STARTING REAL-TIME FLIGHT DATA STREAM")
        print(f"{'='*80}")
        print(f"Duration: {duration_seconds} seconds")
        print(f"Weather sampling: {sample_weather} flights per batch")
        print(f"{'='*80}\n")
        
        start_time = time.time()
        batch_count = 0
        total_flights_sent = 0
        total_enriched_sent = 0
        
        usa_bbox = (24.396308, 49.384358, -125.0, -66.93457)
        
        while (time.time() - start_time) < duration_seconds:
            batch_count += 1
            batch_start = time.time()
            
            print(f"\n{'='*80}")
            print(f"BATCH #{batch_count} - {time.strftime('%H:%M:%S')}")
            print(f"{'='*80}")
            
            # Fetch flights
            print("[1/3] Fetching flights...")
            try:
                flights = self.flight_fetcher.fetch_flights(bbox=usa_bbox)
            except Exception as e:
                print(f"❌ Error fetching flights: {e}")
                flights = None
            
            if not flights:
                print("❌ Failed to fetch flights (rate limited or API error), retrying in 15 seconds...")
                time.sleep(15)
                continue
            
            print(f"✅ Retrieved {len(flights)} flights")
            
            # Send raw flight data
            print("[2/3] Streaming flight data to Kafka...")
            sent_count = 0
            for flight in flights:
                if self.send_flight_data(flight):
                    sent_count += 1
            
            # Flush messages
            self.producer.poll(0)
            
            total_flights_sent += sent_count
            print(f"✅ Sent {sent_count} flight records to '{self.topic_flights}' topic")
            
            # Enrich sample with weather
            print(f"[3/3] Enriching {sample_weather} flights with weather...")
            airborne = [f for f in flights if not f['on_ground'] and f['latitude'] and f['longitude']]
            
            if len(airborne) >= sample_weather:
                step = max(1, len(airborne) // sample_weather)
                sampled = airborne[::step][:sample_weather]
                
                enriched_count = 0
                for i, flight in enumerate(sampled):
                    weather = self.weather_fetcher.fetch_weather(flight['latitude'], flight['longitude'])
                    
                    if weather:
                        assessment = self.weather_fetcher.assess_flight_conditions(weather)
                        enriched = {
                            **flight,
                            'weather': weather,
                            'delay_assessment': assessment
                        }
                        
                        if self.send_enriched_data(enriched):
                            enriched_count += 1
                            risk = assessment['risk_level']
                            print(f"  [{i+1}/{len(sampled)}] {flight['callsign']} - Risk: {risk}")
                    
                    time.sleep(0.5)  # Rate limiting
                
                # Flush enriched messages
                self.producer.flush()
                
                total_enriched_sent += enriched_count
                print(f"✅ Sent {enriched_count} enriched records to '{self.topic_enriched}' topic")
            
            batch_duration = time.time() - batch_start
            print(f"\n📊 Batch completed in {batch_duration:.1f} seconds")
            print(f"📈 Total sent - Flights: {total_flights_sent}, Enriched: {total_enriched_sent}")
            
            # Wait before next batch
            wait_time = 60
            remaining = duration_seconds - (time.time() - start_time)
            
            if remaining > wait_time:
                print(f"⏱️  Waiting {wait_time} seconds before next batch...")
                time.sleep(wait_time)
        
        print(f"\n{'='*80}")
        print("STREAMING COMPLETE")
        print(f"{'='*80}")
        print(f"Total batches: {batch_count}")
        print(f"Total flight records sent: {total_flights_sent}")
        print(f"Total enriched records sent: {total_enriched_sent}")
        print(f"Duration: {time.time() - start_time:.1f} seconds")
        print(f"{'='*80}\n")
    
    def close(self):
        """Close the producer connection"""
        self.producer.flush()
        print("✅ Kafka Producer closed")

def main():
    """Run the producer"""
    producer = FlightDataProducer()
    
    try:
        # Stream for 5 minutes (300 seconds)
        producer.stream_flights(duration_seconds=300, sample_weather=10)
    except KeyboardInterrupt:
        print("\n⚠️  Streaming interrupted by user")
    finally:
        producer.close()

if __name__ == "__main__":
    main()