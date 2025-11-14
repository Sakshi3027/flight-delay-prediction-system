"""
Real-Time Flight Delay Dashboard
Consumes data from Kafka and displays in web interface
"""
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaError
import json
import threading
import time
from collections import defaultdict, deque

app = Flask(__name__)
app.config['SECRET_KEY'] = 'flight-delay-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Store latest data
latest_stats = {
    'total_flights': 0,
    'high_risk': 0,
    'medium_risk': 0,
    'low_risk': 0,
    'avg_altitude': 0,
    'avg_speed': 0,
    'top_countries': [],
    'high_risk_flights': [],
    'recent_flights': deque(maxlen=100)
}

risk_history = {
    'timestamps': deque(maxlen=50),
    'high': deque(maxlen=50),
    'medium': deque(maxlen=50),
    'low': deque(maxlen=50)
}

def consume_enriched_data():
    """Background thread to consume enriched flight data"""
    try:
        consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'dashboard-consumer',
            'auto.offset.reset': 'earliest',  # Changed to earliest to get all data
            'enable.auto.commit': True
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe(['enriched-flight-data'])
        
        print("✅ Connected to Kafka - enriched-flight-data topic")
        
        risk_counts = defaultdict(int)
        country_counts = defaultdict(int)
        high_risk_list = []
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Update risk counts
                risk_level = data.get('delay_assessment', {}).get('risk_level', 'UNKNOWN')
                risk_counts[risk_level] += 1
                
                # Track country
                country_counts[data.get('origin_country', 'Unknown')] += 1
                
                # Track high risk flights
                if risk_level == 'HIGH':
                    high_risk_list.append({
                        'callsign': data.get('callsign', 'N/A'),
                        'country': data.get('origin_country', 'Unknown'),
                        'temp': data.get('weather', {}).get('temperature', 0),
                        'wind': data.get('weather', {}).get('wind_speed', 0),
                        'condition': data.get('weather', {}).get('weather_main', 'Unknown'),
                        'probability': data.get('delay_assessment', {}).get('delay_probability', 0) * 100
                    })
                    high_risk_list = high_risk_list[-10:]
                
                # Update global stats
                latest_stats['total_flights'] = sum(risk_counts.values())
                latest_stats['high_risk'] = risk_counts['HIGH']
                latest_stats['medium_risk'] = risk_counts['MEDIUM']
                latest_stats['low_risk'] = risk_counts['LOW']
                latest_stats['top_countries'] = sorted(
                    [{'name': k, 'count': v} for k, v in country_counts.items()],
                    key=lambda x: x['count'],
                    reverse=True
                )[:5]
                latest_stats['high_risk_flights'] = high_risk_list
                
                # Add to recent flights
                latest_stats['recent_flights'].append({
                    'callsign': data.get('callsign', 'N/A'),
                    'altitude': data.get('baro_altitude', 0),
                    'velocity': data.get('velocity', 0),
                    'risk': risk_level,
                    'temp': data.get('weather', {}).get('temperature', 0)
                })
                
                # Update history
                current_time = time.strftime('%H:%M:%S')
                if len(risk_history['timestamps']) == 0 or risk_history['timestamps'][-1] != current_time:
                    risk_history['timestamps'].append(current_time)
                    risk_history['high'].append(risk_counts['HIGH'])
                    risk_history['medium'].append(risk_counts['MEDIUM'])
                    risk_history['low'].append(risk_counts['LOW'])
                
                # Emit to clients - CONVERT DEQUE TO LIST
                stats_to_send = {
                    'total_flights': latest_stats['total_flights'],
                    'high_risk': latest_stats['high_risk'],
                    'medium_risk': latest_stats['medium_risk'],
                    'low_risk': latest_stats['low_risk'],
                    'top_countries': latest_stats['top_countries'],
                    'high_risk_flights': latest_stats['high_risk_flights'],
                    'recent_flights': list(latest_stats['recent_flights'])
                }
                
                socketio.emit('update_stats', stats_to_send)
                socketio.emit('update_history', {
                    'timestamps': list(risk_history['timestamps']),
                    'high': list(risk_history['high']),
                    'medium': list(risk_history['medium']),
                    'low': list(risk_history['low'])
                })
                
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                continue
                
    except Exception as e:
        print(f"❌ Kafka consumer error: {e}")

@app.route('/')
def index():
    """Render dashboard"""
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('✅ Client connected')
    
    # Convert deque to list before sending
    stats_to_send = {
        'total_flights': latest_stats['total_flights'],
        'high_risk': latest_stats['high_risk'],
        'medium_risk': latest_stats['medium_risk'],
        'low_risk': latest_stats['low_risk'],
        'top_countries': latest_stats['top_countries'],
        'high_risk_flights': latest_stats['high_risk_flights'],
        'recent_flights': list(latest_stats['recent_flights'])
    }
    
    emit('update_stats', stats_to_send)
    emit('update_history', {
        'timestamps': list(risk_history['timestamps']),
        'high': list(risk_history['high']),
        'medium': list(risk_history['medium']),
        'low': list(risk_history['low'])
    })

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('⚠️  Client disconnected')

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    consumer_thread = threading.Thread(target=consume_enriched_data, daemon=True)
    consumer_thread.start()
    
    print("\n" + "="*80)
    print("REAL-TIME FLIGHT DELAY DASHBOARD")
    print("="*80)
    print("Dashboard running at: http://localhost:5000")
    print("="*80 + "\n")
    
    # Run Flask app
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)