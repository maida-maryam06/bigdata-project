from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from pymongo import MongoClient
import pymysql
import bcrypt
import json
from datetime import datetime
import os
from kafka import KafkaConsumer
import threading

app = Flask(__name__)
CORS(app)

# Database configurations
# MongoDB for traffic data - WITH AUTHENTICATION FALLBACK
traffic_db = None
mongo_client = None
try:
    # Try with authentication first
    mongo_client = MongoClient('mongodb://traffic_user:traffic_pass@localhost:27017/traffic_analytics')
    mongo_client.server_info()  # Test connection
    traffic_db = mongo_client['traffic_analytics']
    print("✅ MongoDB connected successfully with authentication!")
except Exception as e:
    print(f"❌ MongoDB auth connection failed: {e}")
    # Fallback - try without authentication
    try:
        mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        mongo_client.server_info()
        traffic_db = mongo_client['traffic_analytics']
        print("✅ MongoDB connected without authentication!")
    except Exception as e2:
        print(f"❌ MongoDB fallback also failed: {e2}")
        traffic_db = None
        mongo_client = None

# MySQL for user authentication
mysql_conn = None
try:
    mysql_conn = pymysql.connect(
        host='localhost',
        user='traffic_user',
        password='traffic_pass',
        database='traffic_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print("✅ MySQL connected successfully!")
except Exception as e:
    print(f"❌ MySQL connection error: {e}")
    mysql_conn = None


@app.route('/')
def home():
    return jsonify({"message": "Smart City Traffic Analytics API", "status": "active"})


@app.route('/api/signup', methods=['POST'])
def signup():
    try:
        if mysql_conn is None:
            return jsonify({"error": "MySQL database not available"}), 503

        data = request.get_json()
        username = data['username']
        email = data['email']
        password = data['password']

        # Hash password
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        with mysql_conn.cursor() as cursor:
            # Check if user exists
            cursor.execute("SELECT id FROM users WHERE username = %s OR email = %s", (username, email))
            if cursor.fetchone():
                return jsonify({"error": "User already exists"}), 400

            # Insert new user
            cursor.execute(
                "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)",
                (username, email, hashed_password.decode('utf-8'))
            )
            mysql_conn.commit()

            # Also store in MongoDB for analytics if available
            if traffic_db is not None:
                traffic_db.users.insert_one({
                    "username": username,
                    "email": email,
                    "signup_date": datetime.now(),
                    "mysql_user_id": cursor.lastrowid
                })

        return jsonify({"message": "User created successfully"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/login', methods=['POST'])
def login():
    try:
        if mysql_conn is None:
            return jsonify({"error": "MySQL database not available"}), 503

        data = request.get_json()
        username = data['username']
        password = data['password']

        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
            user = cursor.fetchone()

            if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
                return jsonify({
                    "message": "Login successful",
                    "user": {
                        "id": user['id'],
                        "username": user['username'],
                        "email": user['email']
                    }
                }), 200
            else:
                return jsonify({"error": "Invalid credentials"}), 401

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/traffic-data', methods=['GET'])
def get_traffic_data():
    try:
        if traffic_db is None:
            return jsonify({"error": "MongoDB not available"}), 503

        # Get recent traffic data from MongoDB
        data = list(traffic_db.traffic_data.find().sort('timestamp', -1).limit(100))

        # Convert ObjectId to string for JSON serialization
        for item in data:
            item['_id'] = str(item['_id'])

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/congestion-alerts', methods=['GET'])
def get_congestion_alerts():
    try:
        if traffic_db is None:
            return jsonify({"error": "MongoDB not available"}), 503

        alerts = list(traffic_db.congestion_alerts.find({'resolved': False}).sort('timestamp', -1))

        for alert in alerts:
            alert['_id'] = str(alert['_id'])

        return jsonify(alerts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint to verify all services"""
    health_status = {
        "flask_app": "healthy",
        "mongodb": "connected" if traffic_db is not None else "disconnected",
        "mysql": "connected" if mysql_conn is not None else "disconnected",
        "timestamp": datetime.now().isoformat()
    }

    # Test MySQL connection
    if mysql_conn is not None:
        try:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            health_status["mysql"] = "connected"
        except:
            health_status["mysql"] = "disconnected"

    return jsonify(health_status)


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get basic statistics"""
    stats = {}

    # MySQL stats
    if mysql_conn is not None:
        try:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as user_count FROM users")
                stats['total_users'] = cursor.fetchone()['user_count']
        except:
            stats['total_users'] = 0
    else:
        stats['total_users'] = 0

    # MongoDB stats
    if traffic_db is not None:
        try:
            stats['total_alerts'] = traffic_db.congestion_alerts.count_documents({})
            stats['active_alerts'] = traffic_db.congestion_alerts.count_documents({'resolved': False})
        except:
            stats['total_alerts'] = 0
            stats['active_alerts'] = 0
    else:
        stats['total_alerts'] = 0
        stats['active_alerts'] = 0

    return jsonify(stats)


@app.route('/api/simulate-data', methods=['POST'])
def simulate_data():
    """Endpoint to manually add sample traffic data"""
    try:
        if traffic_db is None:
            return jsonify({"error": "MongoDB not available"}), 503

        data = request.get_json()

        # Add sample traffic data
        sample_traffic = {
            "vehicle_id": data.get('vehicle_id', 'V_TEST_001'),
            "timestamp": datetime.now().isoformat(),
            "latitude": data.get('latitude', 40.7510),
            "longitude": data.get('longitude', -74.0020),
            "speed": data.get('speed', 45.5),
            "road_id": data.get('road_id', 'RD001'),
            "road_name": data.get('road_name', 'Main Street'),
            "vehicle_type": data.get('vehicle_type', 'car')
        }

        result = traffic_db.traffic_data.insert_one(sample_traffic)

        return jsonify({
            "message": "Sample data added successfully",
            "inserted_id": str(result.inserted_id)
        }), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/real-time-stats', methods=['GET'])
def get_real_time_stats():
    """Get real-time traffic statistics"""
    try:
        if traffic_db is None:
            return jsonify({"error": "MongoDB not available"}), 503

        # Get recent traffic data (last 5 minutes)
        five_min_ago = datetime.now().timestamp() - 300
        recent_data = list(traffic_db.traffic_data.find({
            "timestamp": {"$gte": datetime.fromtimestamp(five_min_ago).isoformat()}
        }))

        if not recent_data:
            return jsonify({"message": "No recent data available"})

        # Calculate real-time stats
        total_vehicles = len(recent_data)
        avg_speed = sum(d.get('speed', 0) for d in recent_data) / total_vehicles
        roads = set(d.get('road_name', 'Unknown') for d in recent_data)

        # Count vehicles by type
        vehicle_types = {}
        for data in recent_data:
            v_type = data.get('vehicle_type', 'unknown')
            vehicle_types[v_type] = vehicle_types.get(v_type, 0) + 1

        stats = {
            "total_vehicles": total_vehicles,
            "avg_speed": round(avg_speed, 2),
            "active_roads": len(roads),
            "vehicle_types": vehicle_types,
            "timestamp": datetime.now().isoformat()
        }

        return jsonify(stats)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/congestion-map', methods=['GET'])
def get_congestion_map():
    """Get congestion data for map visualization"""
    try:
        if traffic_db is None:
            return jsonify({"error": "MongoDB not available"}), 503

        # Get recent congestion alerts
        congestion_data = list(traffic_db.congestion_alerts.find({
            "resolved": False
        }).sort("timestamp", -1).limit(20))

        # Get recent vehicle positions for heatmap
        recent_vehicles = list(traffic_db.traffic_data.find().sort("timestamp", -1).limit(100))

        map_data = {
            "congestion_alerts": [],
            "vehicle_positions": [],
            "last_updated": datetime.now().isoformat()
        }

        # Process congestion alerts
        for alert in congestion_data:
            map_data["congestion_alerts"].append({
                "road_id": alert.get("road_id"),
                "road_name": alert.get("road_name"),
                "severity": alert.get("severity", "medium"),
                "avg_speed": alert.get("avg_speed", 0),
                "vehicle_count": alert.get("vehicle_count", 0)
            })

        # Process vehicle positions
        for vehicle in recent_vehicles:
            map_data["vehicle_positions"].append({
                "vehicle_id": vehicle.get("vehicle_id"),
                "latitude": vehicle.get("latitude"),
                "longitude": vehicle.get("longitude"),
                "speed": vehicle.get("speed", 0),
                "road_name": vehicle.get("road_name", "Unknown")
            })

        return jsonify(map_data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    print("🚀 Starting Smart City Traffic Analytics API...")
    print("📍 Endpoints:")
    print("   GET  /api/health          - Service health check")
    print("   POST /api/signup          - User registration")
    print("   POST /api/login           - User authentication")
    print("   GET  /api/traffic-data    - Get traffic data")
    print("   GET  /api/congestion-alerts - Get congestion alerts")
    print("   GET  /api/stats           - Get system statistics")
    print("   POST /api/simulate-data   - Add sample traffic data")

    app.run(debug=True, port=5000, host='0.0.0.0')