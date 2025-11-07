
import sys
import os
import mysql.connector
import psycopg2

# --- FIX: Add project root to Python path ---
# This allows finding the 'config' package
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# --- End Fix ---

try:
    from config.config import (
        MYSQL_STAGING_CONFIG,
        MYSQL_TRANSFORMED_CONFIG,
        POSTGRESQL_CONFIG,
        validate_config
    )
except ImportError as e:
    print(f" CRITICAL: Could not import config. {e}")
    print("Please ensure your 'config/config.py' file exists and is correct.")
    sys.exit(1)


def test_mysql_staging():
    """Test MySQL staging database connection"""
    print("\n" + "="*60)
    print("Testing MySQL Staging Database (stagging)")
    print("="*60)
    try:
        conn = mysql.connector.connect(**MYSQL_STAGING_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE(), VERSION()")
        db_name, version = cursor.fetchone()
        print(f" Connected to MySQL Staging")
        print(f"  Database: {db_name}")
        print(f"  Version: {version}")

        # List tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"  Tables: {len(tables)} found")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"    - {table[0]}: {count:,} rows")

        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f" MySQL Staging connection failed: {e}")
        return False

def test_mysql_transformed():
    """Test MySQL transformed database connection"""
    print("\n" + "="*60)
    print("Testing MySQL Transformed Database (transformed)")
    print("="*60)
    try:
        conn = mysql.connector.connect(**MYSQL_TRANSFORMED_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE(), VERSION()")
        db_name, version = cursor.fetchone()
        print(f" Connected to MySQL Transformed")
        print(f"  Database: {db_name}")
        print(f"  Version: {version}")

        # List tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"  Tables: {len(tables)} found")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"    - {table[0]}: {count:,} rows")

        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"✗ MySQL Transformed connection failed: {e}")
        return False

def test_postgresql_production():
    """Test PostgreSQL production database connection"""
    print("\n" + "="*60)
    print("Testing PostgreSQL Production Database (bank_production)")
    print("="*60)
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT current_database(), version()")
        db_name, version = cursor.fetchone()
        print(f"✓ Connected to PostgreSQL Production")
        print(f"  Database: {db_name}")
        print(f"  Version: {version.split(',')[0]}")

        # List tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        tables = cursor.fetchall()
        print(f"  Tables: {len(tables)} found")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor.fetchone()[0]
                print(f"    - {table[0]}: {count:,} rows")
            except psycopg2.Error as e:
                print(f"    - {table[0]}: Error counting rows ({e.pgcode})")
                conn.rollback() # Need to rollback after an error in a transaction

        cursor.close()
        conn.close()
        return True
    except psycopg2.Error as e:
        print(f"✗ PostgreSQL Production connection failed: {e}")
        return False

def main():
    """Test all database connections"""
    print("\n" + "="*80)
    print(" Database Connection & Data Health Check")
    print("="*80)

    # First, validate the .env config
    try:
        validate_config()
        print(" .env configuration validated successfully")
    except ValueError as e:
        print(f" .env configuration error: {e}")
        print("Please check your .env file before proceeding.")
        return

    results = {
        'MySQL Staging': test_mysql_staging(),
        'MySQL Transformed': test_mysql_transformed(),
        'PostgreSQL Production': test_postgresql_production()
    }

    print("\n" + "="*80)
    print(" Connection Test Summary")
    print("="*80)

    for db_name, success in results.items():
        status = " SUCCESS" if success else "✗ FAILED"
        print(f"  {db_name}: {status}")

    total_success = sum(1 for s in results.values() if s)
    total_dbs = len(results)

    print(f"\nTotal: {total_success}/{total_dbs} databases connected successfully")
    print("="*80)

    if total_success == total_dbs:
        print("\n All database connections are working!")
    else:
        print("\n Some database connections failed. Please check your .env file and Radmin VPN.")

if __name__ == "__main__":
    main()