"""
Setup script for ETL Pipeline
- Installs dependencies from requirements.txt
- Creates necessary directories
- Performs a health check on all database connections
"""
import os
import subprocess
import sys
from pathlib import Path

# --- FIX: Add project root to sys.path ---
# This allows finding the 'src' and 'config' packages
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# --- End Fix ---

def install_dependencies():
    """Install required Python packages"""
    print("\n" + "="*60)
    print(" 2. Installing Dependencies")
    print("="*60)
    
    # Use the parent directory of this script (project root) to find requirements.txt
    req_path = PROJECT_ROOT / 'requirements.txt'
    
    if not req_path.exists():
        print(f" ✗ ERROR: requirements.txt not found at {req_path}")
        return False
        
    print(f" > Running: pip install -r {req_path.relative_to(PROJECT_ROOT)}")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(req_path)])
        print(" ✓ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f" ✗ Failed to install dependencies: {e}")
        print(" ! Please try running 'pip install -r requirements.txt' manually.")
        return False

def create_directories():
    """Create necessary directories"""
    print("\n" + "="*60)
    print(" 1. Creating Directories")
    print("="*60)
    
    # --- FIX: Added 'exports' directory ---
    directories = ["data", "logs", "exports"]
    
    for directory in directories:
        dir_path = PROJECT_ROOT / directory
        if not dir_path.exists():
            dir_path.mkdir()
            print(f" ✓ Directory '{directory}' created.")
        else:
            print(f" ✓ Directory '{directory}' exists or was created.")

def check_config_file():
    """Check if .env file exists"""
    print("\n" + "="*60)
    print(" 3. Checking Configuration File")
    print("="*60)
    
    env_path = PROJECT_ROOT / '.env'
    if env_path.exists():
        print(" ✓ .env file found.")
        return True
    else:
        print(f" ✗ WARNING: .env file not found at {env_path}")
        print(" ! The pipeline will use default credentials, which may fail.")
        print(" ! Please create a .env file with your database credentials.")
        return False # Continue but warn

def check_database_connections():
    """Check connections to all required databases."""
    print("\n" + "="*60)
    print(" 4. Testing Database Connections")
    print("="*60)
    
    # --- FIX: Import modules *after* setting path ---
    try:
        from config.config import validate_config
        print(" ✓ Configuration file imported.")
        validate_config()
        # config.py prints its own success message
    except ImportError as e:
        print(f" ✗ ImportError: Could not import 'config'. {e}")
        return False
    except Exception as e:
        print(f" ✗ Config Error: {e}")
        return False

    mysql_ok = False
    postgres_ok = False
    
    # Test MySQL
    try:
        from src.extract import MySQLExtractor
        extractor = MySQLExtractor()
        if extractor.connect():
            # extractor.connect() prints its own success message
            print(" ✓ MySQL (Staging) connection successful.")
            extractor.close()
            print("   - MySQL connection closed.")
            mysql_ok = True
        else:
            print(" ✗ MySQL (Staging) connection FAILED.")
    except ImportError as e:
        # --- FIX: Print the full import error for debugging ---
        print(f" ✗ ImportError: Could not import MySQLExtractor: {e}")
    except Exception as e:
        print(f" ✗ MySQL Connection Error: {e}")

    # Test PostgreSQL
    try:
        from src.load import IncrementalLoader
        loader = IncrementalLoader()
        if loader.connect_postgresql():
            # loader.connect_postgresql() prints its own success message
            print(" ✓ PostgreSQL (Production) connection successful.")
            loader.close_connections()
            print("   - PostgreSQL connection closed.")
            postgres_ok = True
        else:
            print(" ✗ PostgreSQL (Production) connection FAILED.")
    except ImportError as e:
        # --- FIX: Print the full import error for debugging ---
        print(f" ✗ ImportError: Could not import IncrementalLoader: {e}")
    except Exception as e:
        print(f" ✗ PostgreSQL Connection Error: {e}")
        
    return mysql_ok and postgres_ok

def main():
    print("\n" + "="*60)
    print(" Starting ETL Pipeline Setup & Health Check")
    print("="*60)
    
    try:
        # Create directories first
        create_directories()
        
        # Then, install dependencies
        if not install_dependencies():
            raise Exception("Dependency installation failed.")
        
        # Check for .env file
        check_config_file()
        
        # Test database connections
        if not check_database_connections():
            raise Exception("Database connection error.")
        
        print("\n" + "="*60)
        print(" 🎉 Setup Completed Successfully! 🎉")
        print("="*60)
        print("\nYour system is ready. Next steps:")
        print(" 1. Ensure your CSV files are in the 'data' directory.")
        print(" 2. Run the full pipeline with: python main.py --mode full")
        print(" 3. Run the scheduler with:   python main.py --mode schedule")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\nSetup failed. {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()