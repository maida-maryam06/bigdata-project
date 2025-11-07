from pymongo import MongoClient
import pymysql
from datetime import datetime


def init_mongodb():
    """Initialize MongoDB with sample data"""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['traffic_analytics']

        # Create sample congestion data
        sample_congestion = {
            "alert_id": "CONG001",
            "timestamp": datetime.now().isoformat(),
            "road_id": "RD001",
            "road_name": "Main Street",
            "severity": "medium",
            "avg_speed": 15.5,
            "vehicle_count": 28,
            "cause": "volume",
            "resolved": False
        }

        db.congestion_alerts.insert_one(sample_congestion)
        print("✅ MongoDB initialized with sample data")

    except Exception as e:
        print(f"❌ MongoDB init error: {e}")


def init_mysql():
    """Verify MySQL setup"""
    try:
        conn = pymysql.connect(
            host='localhost',
            user='traffic_user',
            password='traffic_pass',
            database='traffic_db',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM users")
            result = cursor.fetchone()
            print(f"✅ MySQL connected. Users in database: {result['count']}")

    except Exception as e:
        print(f"❌ MySQL init error: {e}")


if __name__ == "__main__":
    print("🗄️ Initializing databases...")
    init_mongodb()
    init_mysql()
    print("🎉 Database initialization complete!")