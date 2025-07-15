"""
Simple FastAPI server startup from scripts folder
"""
import sys
import os
import uvicorn

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from api.main import app

if __name__ == "__main__":
    print("🚀 Starting Telegram Analytics API...")
    print("📚 API Documentation: http://localhost:8000/docs")
    print("🔗 API Root: http://localhost:8000")
    print("🔧 ReDoc Documentation: http://localhost:8000/redoc")
    print("📊 Health Check: http://localhost:8000/health")
    print()
    print("💡 Press Ctrl+C to stop the server")
    print("="*50)
    
    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        print("💡 Make sure the database is running and accessible")
