import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import threading
import requests


class TrafficDataSimulator:
    def __init__(self):
        self.kafka_available = False
        self.setup_kafka()

        # Realistic road network for a city
        self.roads = [
            {"road_id": "RD001", "name": "Main Street", "coords": [(40.7500, -74.0050), (40.7600, -73.9950)]},
            {"road_id": "RD002", "name": "Broadway", "coords": [(40.7550, -74.0100), (40.7650, -74.0000)]},
            {"road_id": "RD003", "name": "5th Avenue", "coords": [(40.7450, -74.0050), (40.7550, -73.9950)]},
            {"road_id": "RD004", "name": "Park Avenue", "coords": [(40.7480, -74.0150), (40.7580, -74.0050)]},
        ]

        # Simulate 200 vehicles
        self.vehicles = [f"V{i:04d}" for i in range(1, 201)]
        self.vehicle_positions = {}

        # Initialize vehicle positions
        for vehicle in self.vehicles:
            self.vehicle_positions[vehicle] = self.get_random_position()

    def setup_kafka(self):
        """Setup Kafka producer with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=['localhost:9092'],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    request_timeout_ms=10000,
                    retries=3
                )
                # Test connection
                self.producer.send('test_topic', {'test': 'data'})
                self.producer.flush()
                self.kafka_available = True
                print("✅ Kafka connected successfully!")
                break
            except Exception as e:
                print(f"❌ Kafka connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print("❌ Kafka not available. Running in simulation mode only.")
                    self.kafka_available = False

    def get_random_position(self):
        """Get random position along a road"""
        road = random.choice(self.roads)
        start_lat, start_lon = road['coords'][0]
        end_lat, end_lon = road['coords'][1]

        # Interpolate along the road
        progress = random.random()
        lat = start_lat + (end_lat - start_lat) * progress
        lon = start_lon + (end_lon - start_lon) * progress

        return {
            'road': road,
            'lat': lat,
            'lon': lon,
            'progress': progress
        }

    def move_vehicle(self, vehicle_id):
        """Move vehicle along its road"""
        position = self.vehicle_positions[vehicle_id]
        road = position['road']

        # Move forward or backward along the road
        position['progress'] += (random.random() - 0.5) * 0.1
        position['progress'] = max(0, min(1, position['progress']))  # Clamp between 0-1

        start_lat, start_lon = road['coords'][0]
        end_lat, end_lon = road['coords'][1]

        position['lat'] = start_lat + (end_lat - start_lat) * position['progress']
        position['lon'] = start_lon + (end_lon - start_lon) * position['progress']

        return position

    def generate_traffic_data(self):
        """Generate realistic traffic data batch"""
        batch_data = []

        # Simulate rush hour patterns
        current_hour = datetime.now().hour
        if 7 <= current_hour <= 9 or 16 <= current_hour <= 18:  # Rush hours
            base_speed = 20
            speed_variation = 15
            vehicle_count = random.randint(15, 25)
        else:  # Normal hours
            base_speed = 45
            speed_variation = 25
            vehicle_count = random.randint(8, 15)

        # Generate data for random vehicles
        for _ in range(vehicle_count):
            vehicle_id = random.choice(self.vehicles)
            position = self.move_vehicle(vehicle_id)

            # Calculate speed based on position and time
            speed = max(5, base_speed + random.uniform(-speed_variation, speed_variation))

            data = {
                "vehicle_id": vehicle_id,
                "timestamp": datetime.now().isoformat(),
                "latitude": round(position['lat'], 6),
                "longitude": round(position['lon'], 6),
                "speed": round(speed, 2),
                "road_id": position['road']['road_id'],
                "road_name": position['road']['name'],
                "vehicle_type": random.choice(["car", "truck", "bus", "motorcycle"]),
                "congestion_level": "high" if speed < 20 else "medium" if speed < 40 else "low"
            }

            batch_data.append(data)

            # Send to Kafka if available
            if self.kafka_available:
                try:
                    self.producer.send('vehicle_gps', data)
                except Exception as e:
                    print(f"❌ Kafka send error: {e}")
                    self.kafka_available = False

        return batch_data

    def detect_congestion(self, traffic_data):
        """Detect congestion based on traffic data"""
        # Group by road
        road_data = {}
        for data in traffic_data:
            road_id = data['road_id']
            if road_id not in road_data:
                road_data[road_id] = []
            road_data[road_id].append(data)

        # Check for congestion on each road
        for road_id, vehicles in road_data.items():
            avg_speed = sum(v['speed'] for v in vehicles) / len(vehicles)
            vehicle_count = len(vehicles)

            if avg_speed < 25 and vehicle_count > 10:
                # Congestion detected!
                road_name = vehicles[0]['road_name']
                congestion_data = {
                    "alert_id": f"CONG_{int(time.time())}",
                    "timestamp": datetime.now().isoformat(),
                    "road_id": road_id,
                    "road_name": road_name,
                    "severity": "high" if avg_speed < 15 else "medium",
                    "avg_speed": round(avg_speed, 2),
                    "vehicle_count": vehicle_count,
                    "cause": random.choice(["accident", "construction", "volume", "weather"]),
                    "resolved": False
                }

                # Send congestion alert
                if self.kafka_available:
                    try:
                        self.producer.send('congestion_alerts', congestion_data)
                    except Exception as e:
                        print(f"❌ Kafka congestion alert error: {e}")

                # Also store in MongoDB via API
                try:
                    requests.post('http://localhost:5000/api/simulate-data',
                                  json=congestion_data, timeout=2)
                except:
                    pass  # Silent fail for demo

    def start_streaming(self):
        """Start real-time traffic data streaming"""
        print("🚦 Starting Advanced Traffic Data Simulation...")
        print(f"📊 Vehicles: {len(self.vehicles)} | Roads: {len(self.roads)}")
        print(f"📡 Kafka: {'Connected' if self.kafka_available else 'Offline'}")
        print("⏰ Real-time patterns: Rush hour simulation enabled")
        print("=" * 50)

        batch_count = 0
        while True:
            try:
                # Generate traffic data batch
                traffic_data = self.generate_traffic_data()
                batch_count += 1

                # Detect congestion
                if batch_count % 3 == 0:  # Check congestion every 3 batches
                    self.detect_congestion(traffic_data)

                # Print status
                current_time = datetime.now().strftime("%H:%M:%S")
                print(f"[{current_time}] Batch {batch_count}: {len(traffic_data)} vehicles | "
                      f"Avg speed: {sum(d['speed'] for d in traffic_data) / len(traffic_data):.1f} km/h")

                # Store sample in MongoDB via API
                if traffic_data and batch_count % 5 == 0:
                    try:
                        sample = random.choice(traffic_data)
                        requests.post('http://localhost:5000/api/simulate-data',
                                      json=sample, timeout=1)
                    except:
                        pass  # Silent fail for demo

                time.sleep(3)  # Send data every 3 seconds

            except Exception as e:
                print(f"❌ Error in streaming: {e}")
                time.sleep(5)


if __name__ == "__main__":
    simulator = TrafficDataSimulator()
    simulator.start_streaming()