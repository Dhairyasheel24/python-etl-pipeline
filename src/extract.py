"""
Complete ETL Extract Module - Stores CSV data AS-IS
Handles both new and updated records (Upsert)
"""
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import glob
import sys
import re
from datetime import datetime
import numpy as np
from typing import Dict, Tuple, List, Set
import logging
import hashlib

# Add project paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from config.config import MYSQL_CONFIG, TABLE_SCHEMAS, CSV_DATA_PATH, BATCH_SIZE
    from src.logger import setup_logger, log_extraction_stats
except ImportError:
    MYSQL_CONFIG = {
        'host': '26.9.242.172',
        'user': 'TeamETL',
        'password': 'TeamETL@123',
        'database': 'stagging',
        'port': 3306
    }
    
    TABLE_SCHEMAS = {
        'branches': {
            'columns': ['branch_id', 'branch_name', 'city', 'state', 'manager_name'],
            'primary_key': 'branch_id',
            'date_columns': []
        },
        'customers': {
            'columns': ['customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 
                       'gender', 'email', 'phone', 'address', 'account_open_date'],
            'primary_key': 'customer_id',
            'date_columns': ['dob', 'account_open_date']
        },
        'loans': {
            'columns': ['loan_id', 'customer_id', 'loan_type', 'loan_amount', 
                       'interest_rate', 'start_date', 'end_date', 'loan_status'],
            'primary_key': 'loan_id',
            'date_columns': ['start_date', 'end_date']
        },
        'transactions': {
            'columns': ['transaction_id', 'customer_id', 'transaction_date', 
                       'transaction_type', 'amount', 'balance_after', 'fraud_flag'],
            'primary_key': 'transaction_id',
            'date_columns': ['transaction_date']
        }
    }
    
    CSV_DATA_PATH = './data'
    BATCH_SIZE = 500
    
    def setup_logger(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not logger.handlers:
            logger.addHandler(handler)
        return logger
    
    def log_extraction_stats(logger, table_name, total_rows, new_rows, updated_rows):
        logger.info(f"Extraction completed for {table_name}:")
        logger.info(f"  Total rows in CSV: {total_rows}")
        logger.info(f"  New rows inserted: {new_rows}")
        logger.info(f"  Rows updated: {updated_rows}")
        success_rate = ((new_rows + updated_rows) / total_rows * 100) if total_rows > 0 else 0
        logger.info(f"  Success rate: {success_rate:.1f}%")

class MySQLExtractor:
    """MySQL extractor - stores CSV data AS-IS without conversions"""
    
    def __init__(self):
        self.logger = setup_logger("MySQLExtractor")
        self.config = MYSQL_CONFIG
        self.connection = None
        self.processed_files = set()
        self.large_file_threshold_mb = 10
        self.chunk_size = 1000
        
    def connect(self) -> bool:
        """Establish connection to MySQL database"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                self.logger.info(f"Successfully connected to MySQL database: {self.config['database']}")
                return True
            return False
        except Error as e:
            self.logger.error(f"Failed to connect to MySQL: {str(e)}")
            return False
    
    def create_staging_tables(self) -> bool:
        """Create staging tables that store raw CSV data as VARCHAR"""
        if not self.connection or not self.connection.is_connected():
            self.logger.error("No database connection")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # File tracking table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS etl_file_tracker (
                    file_name VARCHAR(255) PRIMARY KEY,
                    file_hash VARCHAR(64),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    record_count INT,
                    file_size_mb DECIMAL(10,2)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # Staging tables - all columns as VARCHAR to preserve raw data
            # NOTE: The UNIQUE constraint on the primary key is CRITICAL for UPSERT
            table_definitions = {
                'staging_branches': '''
                    CREATE TABLE IF NOT EXISTS staging_branches (
                        staging_row_id INT AUTO_INCREMENT PRIMARY KEY,
                        branch_id VARCHAR(50) UNIQUE,
                        branch_name VARCHAR(255),
                        city VARCHAR(100),
                        state VARCHAR(100),
                        manager_name VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_branch_id (branch_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''',
                'staging_customers': '''
                    CREATE TABLE IF NOT EXISTS staging_customers (
                        staging_row_id INT AUTO_INCREMENT PRIMARY KEY,
                        customer_id VARCHAR(50) UNIQUE,
                        branch_id VARCHAR(50),
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        dob VARCHAR(50),
                        gender VARCHAR(10),
                        email VARCHAR(255),
                        phone VARCHAR(20),
                        address TEXT,
                        account_open_date VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_customer_id (customer_id),
                        INDEX idx_branch_id (branch_id),
                        INDEX idx_email (email)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''',
                'staging_loans': '''
                    CREATE TABLE IF NOT EXISTS staging_loans (
                        staging_row_id INT AUTO_INCREMENT PRIMARY KEY,
                        loan_id VARCHAR(50) UNIQUE,
                        customer_id VARCHAR(50),
                        loan_type VARCHAR(100),
                        loan_amount VARCHAR(100),
                        interest_rate VARCHAR(100),
                        start_date VARCHAR(50),
                        end_date VARCHAR(50),
                        loan_status VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_loan_id (loan_id),
                        INDEX idx_customer_id (customer_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''',
                'staging_transactions': '''
                    CREATE TABLE IF NOT EXISTS staging_transactions (
                        staging_row_id INT AUTO_INCREMENT PRIMARY KEY,
                        transaction_id VARCHAR(50) UNIQUE,
                        customer_id VARCHAR(50),
                        transaction_date VARCHAR(50),
                        transaction_type VARCHAR(50),
                        amount VARCHAR(100),
                        balance_after VARCHAR(100),
                        fraud_flag VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_transaction_id (transaction_id),
                        INDEX idx_customer_id (customer_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                '''
            }
            
            for table_name, create_sql in table_definitions.items():
                cursor.execute(create_sql)
                self.logger.info(f"Created/verified staging table: {table_name}")
            
            self.connection.commit()
            cursor.close()
            
            self.logger.info("Staging tables created successfully")
            return True
            
        except Error as e:
            self.logger.error(f"Failed to create staging tables: {str(e)}")
            return False
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.error(f"Failed to calculate hash for {file_path}: {str(e)}")
            return ""
    
    def _is_file_processed(self, file_path: str) -> bool:
        """Check if file has been processed before"""
        file_name = os.path.basename(file_path)
        
        if not self.connection or not self.connection.is_connected():
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT file_hash FROM etl_file_tracker WHERE file_name = %s",
                (file_name,)
            )
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                current_hash = self._calculate_file_hash(file_path)
                return result[0] == current_hash
            return False
        except Error as e:
            self.logger.error(f"Failed to check file tracking: {str(e)}")
            return False
    
    def _mark_file_processed(self, file_path: str, record_count: int) -> bool:
        """Mark file as processed"""
        file_name = os.path.basename(file_path)
        file_hash = self._calculate_file_hash(file_path)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        if not self.connection or not self.connection.is_connected():
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """INSERT INTO etl_file_tracker (file_name, file_hash, record_count, file_size_mb) 
                   VALUES (%s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE 
                   file_hash = %s, processed_at = CURRENT_TIMESTAMP, 
                   record_count = %s, file_size_mb = %s""",
                (file_name, file_hash, record_count, file_size_mb, file_hash, record_count, file_size_mb)
            )
            self.connection.commit()
            cursor.close()
            return True
        except Error as e:
            self.logger.error(f"Failed to mark file as processed: {str(e)}")
            return False
    
    def _extract_numeric_id(self, id_value):
        """Extract the complete numeric value from an ID string"""
        if pd.isna(id_value):
            return 0
        
        id_str = str(id_value)
        # Find all digits in the string and join them
        digits = ''.join(re.findall(r'\d', id_str))
        
        if digits:
            return int(digits)
        return 0
    
    def _process_dataframe_raw(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Store CSV data AS-IS - NO conversions, but SORT by complete numerical ID"""
        processed_df = df.copy()
        
        # Sort by complete numerical ID before storing
        primary_key = TABLE_SCHEMAS[table_name]['primary_key']
        if primary_key in processed_df.columns:
            # Extract complete numeric value for sorting
            processed_df['_sort_key'] = processed_df[primary_key].apply(self._extract_numeric_id)
            processed_df = processed_df.sort_values('_sort_key').drop('_sort_key', axis=1).reset_index(drop=True)
        
        # Only convert pandas NaN to None (for MySQL compatibility)
        for col in processed_df.columns:
            processed_df[col] = processed_df[col].apply(
                lambda x: None if pd.isna(x) else x
            )
        
        # Keep everything as strings (exact copy from CSV)
        for col in processed_df.columns:
            processed_df[col] = processed_df[col].astype(str)
            # Only replace 'nan' strings with None
            processed_df.loc[processed_df[col].isin(['nan', 'None', 'NaT', '<NA>', '']), col] = None
        
        return processed_df
    
    def _get_existing_primary_keys(self, table_name: str) -> Set[str]:
        """Get existing primary keys from staging table"""
        if not self.connection or not self.connection.is_connected():
            return set()
        
        primary_key = TABLE_SCHEMAS[table_name]['primary_key']
        staging_table = f"staging_{table_name}"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT DISTINCT {primary_key} FROM {staging_table}")
            existing_keys = {str(row[0]) for row in cursor}
            cursor.close()
            return existing_keys
        except Error as e:
            self.logger.error(f"Failed to get existing keys for {table_name}: {str(e)}")
            return set()
    
    def _upsert_batch_data(self, staging_table: str, columns: List[str], batch_data: List[tuple]) -> int:
        """Insert/Update batch data into staging table using UPSERT"""
        if not batch_data:
            return 0
        
        try:
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Build the UPDATE part
            # e.g. "col1 = VALUES(col1), col2 = VALUES(col2)"
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns])
            
            upsert_query = f"""INSERT INTO {staging_table} ({columns_str}) 
                               VALUES ({placeholders})
                               ON DUPLICATE KEY UPDATE {update_clause}"""
            
            cursor = self.connection.cursor()
            cursor.executemany(upsert_query, batch_data)
            affected_rows = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            # For logging, we'll rely on the Python-side counts.
            # Return the number of rows we attempted to process.
            return len(batch_data)
        except Error as e:
            self.logger.error(f"Failed to upsert batch data: {str(e)}")
            self.connection.rollback()
            return 0
    
    def _process_large_file_in_chunks(self, table_name: str, csv_file_path: str) -> Tuple[int, int, int]:
        """Process large CSV files in chunks"""
        total_rows_processed = 0
        total_new = 0
        total_updated = 0
        
        try:
            file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
            self.logger.info(f"Processing large file {os.path.basename(csv_file_path)} ({file_size_mb:.1f}MB) in chunks of {self.chunk_size}")
            
            existing_keys = self._get_existing_primary_keys(table_name)
            self.logger.info(f"Found {len(existing_keys)} existing records in database")
            
            expected_columns = TABLE_SCHEMAS[table_name]['columns']
            primary_key = TABLE_SCHEMAS[table_name]['primary_key']
            staging_table = f"staging_{table_name}"
            
            chunk_num = 0
            for chunk_df in pd.read_csv(csv_file_path, chunksize=self.chunk_size, keep_default_na=False, dtype=str):
                chunk_num += 1
                
                if set(chunk_df.columns) != set(expected_columns):
                    self.logger.error(f"Column mismatch in chunk {chunk_num}")
                    continue
                
                chunk_df = chunk_df[expected_columns]
                chunk_df = self._process_dataframe_raw(chunk_df, table_name)
                
                chunk_df = chunk_df[chunk_df[primary_key].notna()]
                chunk_df = chunk_df[chunk_df[primary_key] != '']
                chunk_df = chunk_df.drop_duplicates(subset=[primary_key], keep='last')
                
                if chunk_df.empty:
                    continue

                total_rows_processed += len(chunk_df)
                
                # Calculate new vs update counts
                chunk_keys = set(chunk_df[primary_key].astype(str))
                new_keys = chunk_keys - existing_keys
                update_keys = chunk_keys.intersection(existing_keys)
                
                total_new += len(new_keys)
                total_updated += len(update_keys)
                
                # Send the *entire* chunk to be upserted
                if not chunk_df.empty:
                    batch_data = [tuple(row) for row in chunk_df.to_numpy()]
                    
                    for i in range(0, len(batch_data), BATCH_SIZE):
                        sub_batch = batch_data[i:i+BATCH_SIZE]
                        self._upsert_batch_data(staging_table, expected_columns, sub_batch)
                    
                    # Add new keys to our set so they aren't double-counted
                    existing_keys.update(new_keys)
                
                if chunk_num % 10 == 0:
                    self.logger.info(f"Processed {chunk_num} chunks, {total_rows_processed} rows total, {total_new} new, {total_updated} updated")
            
            self.logger.info(f"Large file completed: {total_new} new records, {total_updated} updated records from {total_rows_processed} total rows")
            return total_rows_processed, total_new, total_updated
            
        except Exception as e:
            self.logger.error(f"Error processing large file: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return 0, 0, 0
    
    def _process_small_file(self, table_name: str, csv_file_path: str) -> Tuple[int, int, int]:
        """Process small CSV files in memory"""
        try:
            df = pd.read_csv(csv_file_path, keep_default_na=False, dtype=str)
            total_rows = len(df)
            
            if df.empty:
                self.logger.warning(f"Empty CSV file: {csv_file_path}")
                return 0, 0, 0
            
            expected_columns = TABLE_SCHEMAS[table_name]['columns']
            if set(df.columns) != set(expected_columns):
                self.logger.error(f"Column mismatch for {table_name}")
                return 0, 0, 0
            
            df = df[expected_columns]
            df = self._process_dataframe_raw(df, table_name)
            
            primary_key = TABLE_SCHEMAS[table_name]['primary_key']
            df = df[df[primary_key].notna()]
            df = df[df[primary_key] != '']
            df = df.drop_duplicates(subset=[primary_key], keep='last')
            
            if df.empty:
                self.logger.info(f"No valid data after cleaning in {csv_file_path}")
                return total_rows, 0, 0

            # Calculate new vs update counts
            existing_keys = self._get_existing_primary_keys(table_name)
            csv_keys = set(df[primary_key].astype(str))
            
            new_keys = csv_keys - existing_keys
            update_keys = csv_keys.intersection(existing_keys)
            
            new_count = len(new_keys)
            update_count = len(update_keys)
            
            # Send the *entire* DataFrame to be upserted
            if not df.empty:
                staging_table = f"staging_{table_name}"
                batch_data = [tuple(row) for row in df.to_numpy()]
                
                for i in range(0, len(batch_data), BATCH_SIZE):
                    sub_batch = batch_data[i:i+BATCH_SIZE]
                    self._upsert_batch_data(staging_table, expected_columns, sub_batch)
            
            self.logger.info(f"Small file: {new_count} new records, {update_count} updated records from {total_rows} CSV rows")
            return total_rows, new_count, update_count
            
        except Exception as e:
            self.logger.error(f"Failed to process {csv_file_path}: {str(e)}")
            return 0, 0, 0
    
    def load_csv_to_staging(self, table_name: str, csv_file_path: str) -> Tuple[int, int, int]:
        """Load CSV file to staging table with raw data"""
        if not self.connection or not self.connection.is_connected():
            self.logger.error("No database connection")
            return 0, 0, 0
        
        try:
            csv_row_count = sum(1 for _ in open(csv_file_path, encoding='utf-8')) - 1
        except:
            try:
                csv_row_count = sum(1 for _ in open(csv_file_path, encoding='latin-1')) - 1
            except:
                csv_row_count = 0
        
        if self._is_file_processed(csv_file_path):
            self.logger.info(f"Skipping already processed file: {csv_file_path}")
            return csv_row_count, 0, 0
        
        try:
            file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
            self.logger.info(f"Processing file: {os.path.basename(csv_file_path)} ({file_size_mb:.1f}MB)")
            
            if file_size_mb > self.large_file_threshold_mb:
                total_rows, new_rows, updated_rows = self._process_large_file_in_chunks(table_name, csv_file_path)
            else:
                total_rows, new_rows, updated_rows = self._process_small_file(table_name, csv_file_path)
            
            if total_rows > 0:
                self._mark_file_processed(csv_file_path, total_rows)
            
            return total_rows, new_rows, updated_rows
            
        except Exception as e:
            self.logger.error(f"Failed to load {csv_file_path}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return 0, 0, 0
    
    def extract_all_csv_files(self) -> Dict[str, Tuple[int, int, int]]:
        """Extract all CSV files with raw data"""
        results = {}
        
        if not os.path.exists(CSV_DATA_PATH):
            self.logger.error(f"Data directory not found: {CSV_DATA_PATH}")
            return results
        
        for table_name in TABLE_SCHEMAS.keys():
            csv_pattern = os.path.join(CSV_DATA_PATH, f"{table_name}*.csv")
            csv_files = glob.glob(csv_pattern)
            
            if not csv_files:
                self.logger.warning(f"No CSV files found for {table_name}")
                continue
            
            total_rows = 0
            total_new_rows = 0
            total_updated_rows = 0
            
            for csv_file in sorted(csv_files):
                self.logger.info(f"Processing: {csv_file}")
                rows, new_rows, updated_rows = self.load_csv_to_staging(table_name, csv_file)
                
                total_rows += rows
                total_new_rows += new_rows
                total_updated_rows += updated_rows
            
            results[table_name] = (total_rows, total_new_rows, total_updated_rows)
            log_extraction_stats(self.logger, table_name, total_rows, total_new_rows, total_updated_rows)
        
        return results
    
    def get_staging_table_count(self, table_name: str) -> int:
        """Get record count from staging table"""
        if not self.connection or not self.connection.is_connected():
            return 0
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM staging_{table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Error as e:
            self.logger.error(f"Failed to get count for {table_name}: {str(e)}")
            return 0
    
    def get_file_tracking_summary(self) -> Dict:
        """Get summary of processed files"""
        if not self.connection or not self.connection.is_connected():
            return {}
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    SUM(record_count) as total_records,
                    SUM(file_size_mb) as total_size_mb,
                    MAX(processed_at) as last_processed
                FROM etl_file_tracker
            """)
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return {
                    'total_files': result[0],
                    'total_records': result[1],
                    'total_size_mb': float(result[2]) if result[2] else 0,
                    'last_processed': result[3]
                }
            return {}
        except Error as e:
            self.logger.error(f"Failed to get file tracking summary: {str(e)}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

def main():
    """Test the extractor"""
    extractor = MySQLExtractor()
    
    try:
        if not extractor.connect():
            return
        
        if not extractor.create_staging_tables():
            return
        
        results = extractor.extract_all_csv_files()
        
        print("\nExtraction Results:")
        print("=" * 40)
        for table_name, (csv_rows, inserted, updated) in results.items():
            current_count = extractor.get_staging_table_count(table_name)
            print(f"{table_name}:")
            print(f"  CSV rows: {csv_rows}")
            print(f"  New records: {inserted}")
            print(f"  Updated records: {updated}")
            print(f"  Total in DB: {current_count}")
            print()
        
        summary = extractor.get_file_tracking_summary()
        if summary:
            print("File Tracking Summary:")
            print(f"  Files processed: {summary.get('total_files', 0)}")
            print(f"  Total records: {summary.get('total_records', 0):,}")
            print(f"  Total size: {summary.get('total_size_mb', 0):.1f} MB")
            print(f"  Last processed: {summary.get('last_processed', 'N/A')}")
    
    finally:
        extractor.close()

if __name__ == "__main__":
    main()