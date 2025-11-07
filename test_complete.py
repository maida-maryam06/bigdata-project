import requests
import json
import time

BASE_URL = "http://localhost:5000/api"


def test_connections():
    """Test if services are running"""
    print("🔍 Testing service connections...")

    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"✅ Flask app: {response.json()}")
    except:
        print("❌ Flask app not running")


def test_signup():
    """Test user signup"""
    print("\n👤 Testing user signup...")

    user_data = {
        "username": "maida",
        "email": "maida@example.com",
        "password": "test123"
    }

    try:
        response = requests.post(f"{BASE_URL}/signup", json=user_data)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 201
    except Exception as e:
        print(f"Error: {e}")
        return False


def test_login():
    """Test user login"""
    print("\n🔐 Testing user login...")

    login_data = {
        "username": "maida",
        "password": "test123"
    }

    try:
        response = requests.post(f"{BASE_URL}/login", json=login_data)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False


def test_traffic_endpoints():
    """Test traffic data endpoints"""
    print("\n🚗 Testing traffic endpoints...")

    try:
        # Test traffic data endpoint
        response = requests.get(f"{BASE_URL}/traffic-data")
        print(f"Traffic data: {response.status_code}")

        # Test congestion alerts endpoint
        response = requests.get(f"{BASE_URL}/congestion-alerts")
        print(f"Congestion alerts: {response.status_code}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print("🚀 Starting Smart City Traffic Analytics Tests...")

    test_connections()
    time.sleep(1)

    if test_signup():
        time.sleep(1)
        test_login()

    test_traffic_endpoints()
    print("\n🎉 Test completed!")