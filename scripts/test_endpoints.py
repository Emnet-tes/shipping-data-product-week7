"""
Test API endpoints using requests
"""
import requests
import json
import time

def test_endpoint(url, description):
    """Test a single endpoint"""
    try:
        start_time = time.time()
        response = requests.get(url, timeout=10)
        end_time = time.time()
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ {description}")
            print(f"   Status: {response.status_code}")
            print(f"   Response time: {end_time - start_time:.2f}s")
            if 'data' in data:
                print(f"   Records: {len(data['data'])}")
            print()
            return True
        else:
            print(f"❌ {description}")
            print(f"   Status: {response.status_code}")
            print(f"   Error: {response.text}")
            print()
            return False
    except Exception as e:
        print(f"❌ {description}")
        print(f"   Error: {e}")
        print()
        return False

def test_api_endpoints():
    """Test all API endpoints"""
    base_url = "http://localhost:8000"
    
    print("=== Testing Telegram Analytics API Endpoints ===\n")
    
    tests = [
        (f"{base_url}/health", "Health Check"),
        (f"{base_url}/", "Root Endpoint"),
        (f"{base_url}/api/reports/top-products?limit=5", "Top Products"),
        (f"{base_url}/api/channels/tikvahpharma/activity?days=7", "Channel Activity"),
        (f"{base_url}/api/search/messages?query=medicine&limit=10", "Message Search"),
        (f"{base_url}/api/channels/tikvahpharma/analytics", "Channel Analytics"),
        (f"{base_url}/api/channels?limit=5", "Top Channels"),
        (f"{base_url}/api/engagement/metrics?days=7", "Engagement Metrics"),
        (f"{base_url}/api/detections?limit=10", "Object Detections"),
        (f"{base_url}/api/channels/list", "Channel List"),
        (f"{base_url}/api/stats/summary", "Summary Statistics"),
    ]
    
    passed = 0
    total = len(tests)
    
    for url, description in tests:
        if test_endpoint(url, description):
            passed += 1
    
    print(f"📊 Test Results: {passed}/{total} endpoints passed")
    
    if passed == total:
        print("🎉 All API endpoints are working correctly!")
    else:
        print("⚠️  Some endpoints failed. Check the server logs for details.")

if __name__ == "__main__":
    print("🧪 Make sure the API server is running on http://localhost:8000")
    print("💡 Start server with: python start_api.py")
    input("Press Enter when server is ready...")
    
    test_api_endpoints()
