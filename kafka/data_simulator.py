"""
Data Simulator - Replays collected flight data to Kafka
Uses your existing collected data to simulate real-time streaming
"""
import json
import time
import random
import glob
from confluent_kafka import Producer

class DataSimulator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer"""
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'simulator-producer'
        }
        self.producer = Producer(conf)
        self.topic_enriched = 'enriched-flight-data'
        
        print(f"✅ Data Simulator initialized")
        print(f"   Topic: {self.topic_enriched}")
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            print(f'❌ Message delivery failed: {err}')
    
    def load_collected_data(self):
        """Load previously collected enriched flight data"""
        data_files = glob.glob('data-generators/enriched_flights_*.json')
        
        if not data_files:
            print("❌ No collected data files found!")
            print("Please run: python data-generators/combined_fetcher.py first")
            return []
        
        # Use the most recent file
        latest_file = max(data_files)
        print(f"📂 Loading data from: {latest_file}")
        
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        print(f"✅ Loaded {len(data)} enriched flight records")
        return data
    
    def simulate_streaming(self, duration_seconds=300, batch_size=5, interval=2):
        """
        Simulate streaming by replaying collected data
        
        Args:
            duration_seconds: How long to stream
            batch_size: How many records to send per batch
            interval: Seconds between batches
        """
        print(f"\n{'='*80}")
        print("STARTING DATA SIMULATION")
        print(f"{'='*80}")
        print(f"Duration: {duration_seconds} seconds")
        print(f"Batch size: {batch_size} records")
        print(f"Interval: {interval} seconds")
        print(f"{'='*80}\n")
        
        # Load data
        all_data = self.load_collected_data()
        
        if not all_data:
            return
        
        start_time = time.time()
        batch_count = 0
        total_sent = 0
        
        # Keep cycling through the data
        data_index = 0
        
        while (time.time() - start_time) < duration_seconds:
            batch_count += 1
            
            print(f"\n{'='*80}")
            print(f"BATCH #{batch_count} - {time.strftime('%H:%M:%S')}")
            print(f"{'='*80}")
            
            # Get next batch of records
            batch_data = []
            for _ in range(batch_size):
                if data_index >= len(all_data):
                    data_index = 0  # Loop back to start
                batch_data.append(all_data[data_index])
                data_index += 1
            
            # Add some random variation to make it feel live
            for record in batch_data:
                # Slightly modify temperature and wind speed
                if 'weather' in record:
                    record['weather']['temperature'] += random.uniform(-2, 2)
                    record['weather']['wind_speed'] += random.uniform(-1, 1)
                
                # Send to Kafka
                try:
                    self.producer.produce(
                        self.topic_enriched,
                        key=record['icao24'].encode('utf-8'),
                        value=json.dumps(record).encode('utf-8'),
                        callback=self.delivery_callback
                    )
                    total_sent += 1
                except Exception as e:
                    print(f"❌ Failed to send: {e}")
            
            # Flush messages
            self.producer.flush()
            
            # Show what was sent
            risk_summary = {}
            for record in batch_data:
                risk = record.get('delay_assessment', {}).get('risk_level', 'UNKNOWN')
                risk_summary[risk] = risk_summary.get(risk, 0) + 1
            
            print(f"✅ Sent {len(batch_data)} records")
            print(f"   Risk breakdown: {risk_summary}")
            print(f"📈 Total sent: {total_sent}")
            
            # Wait before next batch
            remaining = duration_seconds - (time.time() - start_time)
            if remaining > interval:
                print(f"⏱️  Waiting {interval} seconds...")
                time.sleep(interval)
        
        print(f"\n{'='*80}")
        print("SIMULATION COMPLETE")
        print(f"{'='*80}")
        print(f"Total batches: {batch_count}")
        print(f"Total records sent: {total_sent}")
        print(f"Duration: {time.time() - start_time:.1f} seconds")
        print(f"{'='*80}\n")
    
    def close(self):
        """Close the producer connection"""
        self.producer.flush()
        print("✅ Data Simulator closed")

def main():
    """Run the simulator"""
    simulator = DataSimulator()
    
    try:
        # Simulate for 5 minutes, sending 5 records every 2 seconds
        simulator.simulate_streaming(duration_seconds=300, batch_size=5, interval=2)
    except KeyboardInterrupt:
        print("\n⚠️  Simulation interrupted by user")
    finally:
        simulator.close()

if __name__ == "__main__":
    main()