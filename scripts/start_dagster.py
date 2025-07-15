"""
Dagster Pipeline Startup Script
Run this script to start the Dagster development server
"""
import os
import subprocess
import sys
import time

def main():
    """Start Dagster development server"""
    
    print("🚀 Starting Dagster Pipeline Development Server")
    print("=" * 50)
    
    # Check if we're in the right directory
    current_dir = os.getcwd()
    if not os.path.exists("dagster_pipeline"):
        print("❌ Error: dagster_pipeline directory not found")
        print("Please run this script from the project root directory")
        return
    
    # Set environment variables
    os.environ['DAGSTER_HOME'] = current_dir
    
    print("📁 Current directory:", current_dir)
    print("📦 Dagster pipeline directory:", os.path.join(current_dir, "dagster_pipeline"))
    print()
    
    # Check Dagster installation
    try:
        result = subprocess.run("dagster --version", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Dagster is installed:", result.stdout.strip())
        else:
            print("❌ Dagster is not installed properly")
            print("Please install with: pip install dagster dagster-webserver")
            return
    except Exception as e:
        print(f"❌ Error checking Dagster installation: {e}")
        return
    
    # Check database connection
    try:
        import psycopg2
        db_config = {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "kara_medical",
            "user": "postgres",
            "password": "your_secure_password"
        }
        
        with psycopg2.connect(**db_config) as conn:
            print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Please ensure PostgreSQL is running and the database exists")
        return
    
    print()
    print("🔄 Starting Dagster development server...")
    print("📊 Web UI will be available at: http://localhost:3000")
    print("🛑 Press Ctrl+C to stop the server")
    print()
    
    try:
        # Start dagster dev server
        subprocess.run(
            "dagster dev -f dagster_pipeline/__init__.py",
            shell=True,
            cwd=current_dir
        )
    except KeyboardInterrupt:
        print("\n🛑 Dagster server stopped")
    except Exception as e:
        print(f"❌ Error starting Dagster server: {e}")

if __name__ == "__main__":
    main()
