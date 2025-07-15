"""
Simple test script for API components
"""
import sys
import os

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from api.database import db_manager
from api.crud import analytics_crud

def test_database():
    """Test database connection"""
    try:
        result = db_manager.execute_query("SELECT 1 as test")
        print("✅ Database connection successful!")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_crud_operations():
    """Test CRUD operations"""
    print("\n🧪 Testing CRUD operations...")
    
    try:
        # Test top products
        products = analytics_crud.get_top_products(5)
        print(f"✅ Top products: {len(products)} results")
        
        # Test channel list
        channels = analytics_crud.get_channel_list()
        print(f"✅ Channel list: {len(channels)} channels")
        
        # Test channel analytics for first channel
        if channels:
            channel_name = channels[0]['channel_name']
            analytics = analytics_crud.get_channel_analytics(channel_name)
            print(f"✅ Channel analytics for {channel_name}: Success")
        
        return True
    except Exception as e:
        print(f"❌ CRUD operations failed: {e}")
        return False

def main():
    """Main test function"""
    print("=== API Components Test ===\n")
    
    if test_database():
        if test_crud_operations():
            print("\n🎉 All tests passed! API is ready to start.")
            print("💡 Start the API server with: uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload")
        else:
            print("\n❌ CRUD tests failed. Check your data models.")
    else:
        print("\n❌ Database test failed. Check your database configuration.")

if __name__ == "__main__":
    main()
