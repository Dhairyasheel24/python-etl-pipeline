import pandas as pd
import mysql.connector
from datetime import date, datetime, timedelta
import re
import logging
import sys
import os
from pathlib import Path
import time
import numpy as np
import warnings

# Suppress all warnings
warnings.filterwarnings('ignore')
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)
pd.set_option('future.no_silent_downcasting', True)

# Ensure we can find the config folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataTransformer:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.staging_connection = None
        self.transformed_connection = None
        self.exports_dir = Path("exports")
        self.exports_dir.mkdir(exist_ok=True)
        
        self.batch_size = 5000
        self.query_timeout = 300
        
        self.stats = {
            'branches': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0},
            'customers': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0},
            'loans': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0},
            'transactions': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0}
        }
        
        self.quality = {}
    
    def connect_databases(self):
        try:
            self.staging_connection = mysql.connector.connect(
                host=self.config['MYSQL_HOST'],
                user=self.config['MYSQL_USER'],
                password=self.config['MYSQL_PASSWORD'],
                database=self.config['MYSQL_DATABASE'],
                port=self.config['MYSQL_PORT'],
                autocommit=False,
                connection_timeout=self.query_timeout
            )
            
            temp_conn = mysql.connector.connect(
                host=self.config['MYSQL_HOST'],
                user=self.config['MYSQL_USER'],
                password=self.config['MYSQL_PASSWORD'],
                port=self.config['MYSQL_PORT'],
                connection_timeout=self.query_timeout
            )
            temp_cursor = temp_conn.cursor()
            temp_cursor.execute("CREATE DATABASE IF NOT EXISTS transformed")
            temp_conn.close()
            
            self.transformed_connection = mysql.connector.connect(
                host=self.config['MYSQL_HOST'],
                user=self.config['MYSQL_USER'],
                password=self.config['MYSQL_PASSWORD'],
                database='transformed',
                port=self.config['MYSQL_PORT'],
                autocommit=False,
                connection_timeout=self.query_timeout
            )
            
        except mysql.connector.Error as e:
            self.logger.error(f"Connection error: {e}")
            raise
    
    def create_transformed_tables(self):
        """Create tables with clean sequential IDs - NO original_id columns"""
        self.transformed_connection.ping(reconnect=True)
        cursor = self.transformed_connection.cursor()
        
        # Drop tables if they exist
        tables = ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Branches - Keep original branch_id as string
        cursor.execute("""
            CREATE TABLE transformed_branches (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                branch_id VARCHAR(20) UNIQUE,
                branch_name VARCHAR(100) DEFAULT 'NA',
                city VARCHAR(50) DEFAULT 'NA',
                state VARCHAR(50) DEFAULT 'NA',
                manager_name VARCHAR(100) DEFAULT 'NA',
                region VARCHAR(20) DEFAULT 'NA',
                INDEX idx_branch_id (branch_id)
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        # Customers - Keep original branch_id as string, sequential customer_id
        cursor.execute("""
            CREATE TABLE transformed_customers (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT UNIQUE,
                branch_id VARCHAR(20),
                first_name VARCHAR(50) DEFAULT 'NA',
                last_name VARCHAR(50) DEFAULT 'NA',
                dob DATE,
                age INT DEFAULT 0,
                gender VARCHAR(10) DEFAULT 'NA',
                email VARCHAR(100) DEFAULT 'NA',
                phone VARCHAR(20) DEFAULT 'NA',
                address TEXT,
                account_open_date DATE,
                customer_tenure_days INT DEFAULT 0,
                customer_segment VARCHAR(20) DEFAULT 'NA',
                outlier_flag BOOLEAN DEFAULT FALSE,
                INDEX idx_customer_id (customer_id),
                INDEX idx_branch_id (branch_id)
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        # Loans - Sequential loan_id
        cursor.execute("""
            CREATE TABLE transformed_loans (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                loan_id INT UNIQUE,
                customer_id INT,
                loan_type VARCHAR(50) DEFAULT 'NA',
                loan_amount DECIMAL(15,2),
                interest_rate DECIMAL(5,2),
                start_date DATE,
                end_date DATE,
                loan_status VARCHAR(50) DEFAULT 'NA',
                loan_duration_months INT DEFAULT 0,
                risk_category VARCHAR(20) DEFAULT 'NA',
                outlier_flag BOOLEAN DEFAULT FALSE,
                INDEX idx_loan_id (loan_id),
                INDEX idx_customer_id (customer_id)
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        # Transactions - Sequential transaction_id
        cursor.execute("""
            CREATE TABLE transformed_transactions (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                transaction_id INT UNIQUE,
                customer_id INT,
                transaction_date DATE,
                transaction_type VARCHAR(50) DEFAULT 'NA',
                amount DECIMAL(15,2),
                balance_after DECIMAL(15,2) DEFAULT 0,
                fraud_flag BOOLEAN DEFAULT FALSE,
                transaction_category VARCHAR(20) DEFAULT 'NA',
                outlier_flag BOOLEAN DEFAULT FALSE,
                INDEX idx_transaction_id (transaction_id),
                INDEX idx_customer_id (customer_id)
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        self.transformed_connection.commit()
        cursor.close()
        print("✓ Transformed tables created with clean sequential ID structure")

    def reset_sequence_numbers(self):
        """Reset all sequence numbers to start from 1 for fresh transformation"""
        print("Resetting sequence numbers for fresh transformation...")
        cursor = self.transformed_connection.cursor()
        
        try:
            tables = ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']
            for table in tables:
                cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
            
            self.transformed_connection.commit()
            print("✓ Sequence numbers reset successfully")
        except Exception as e:
            print(f"⚠ Warning: Could not reset sequences: {e}")
        finally:
            cursor.close()

    def get_existing_ids(self, table_name, id_column):
        """Fetch highest sequential ID from transformed table"""
        self.transformed_connection.ping(reconnect=True)
        cursor = self.transformed_connection.cursor()
        try:
            cursor.execute(f"SELECT MAX({id_column}) FROM {table_name}")
            result = cursor.fetchone()
            max_id = result[0] if result[0] is not None else 0
            return max_id
        except mysql.connector.Error:
            return 0
        finally:
            cursor.close()

    def fetch_data_in_batches(self, cursor, table_name, primary_key):
        """Fetch data in batches with progress indicator"""
        offset = 0
        all_data = []
        
        while True:
            try:
                query = f"SELECT * FROM {table_name} ORDER BY {primary_key} LIMIT {self.batch_size} OFFSET {offset}"
                cursor.execute(query)
                batch = cursor.fetchall()
                if not batch:
                    break
                all_data.extend(batch)
                offset += self.batch_size
                
                if len(all_data) % 10000 == 0:
                    print(f"  Fetched {len(all_data)} records from staging...", end='\r')
                    
            except mysql.connector.Error as e:
                print(f"  Error fetching batch: {e}")
                break
        
        if all_data:
            print(f"  Fetched total {len(all_data)} records from staging.      ")
        return all_data

    # --- Safe Utilities ---
    def safe_val(self, val, default='NA', title=False, upper=False, lower=False):
        if (val is None or pd.isna(val) or str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL'] or str(val).strip().lower() == 'nan'):
            return default
        result = str(val).strip()
        if not result: return default
        if title: result = result.title()
        if upper: result = result.upper()
        if lower: result = result.lower()
        return result

    def safe_date(self, val, return_string_na=False):
        """Robust date parser that correctly handles 2-digit years"""
        if pd.isna(val) or str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a']:
            return None if not return_string_na else 'NA'
        
        try:
            val_str = str(val).strip()

            # Try 4-digit year formats first
            four_digit_match = None
            if re.match(r'^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$', val_str):
                 four_digit_match = True
            elif re.match(r'^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$', val_str):
                 four_digit_match = True
            
            if four_digit_match:
                try:
                    parsed = pd.to_datetime(val_str, errors='coerce')
                    if pd.notna(parsed):
                        parsed_date = parsed.date()
                        if 1900 <= parsed_date.year <= date.today().year:
                            return parsed_date
                except Exception:
                    pass

            # Handle 2-digit year dates with sliding window logic
            two_digit_match = re.match(r'^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2})$', val_str)
            if two_digit_match:
                day, month, year_2digit = map(int, two_digit_match.groups())
                
                # Handle day/month ambiguity
                if day > 12 and month <= 12:
                    # Likely DD-MM-YY format
                    pass
                elif month > 12 and day <= 12:
                    # Likely MM-DD-YY format, swap day and month
                    day, month = month, day
                
                current_year_2digit = date.today().year % 100
                
                # Sliding window logic: years 00-23 → 2000-2023, years 24-99 → 1924-1999
                if year_2digit <= current_year_2digit:
                    year_4digit = 2000 + year_2digit
                else:
                    year_4digit = 1900 + year_2digit
                
                try:
                    parsed = date(year_4digit, month, day)
                    if 1900 <= parsed.year <= date.today().year:
                        return parsed
                except ValueError:
                    # If invalid date (like Feb 30), try with first day of month
                    try:
                        parsed = date(year_4digit, month, 1)
                        if 1900 <= parsed.year <= date.today().year:
                            return parsed
                    except ValueError:
                        pass

            # Final fallback
            try:
                parsed = pd.to_datetime(val_str, errors='coerce', dayfirst=True)
                if pd.notna(parsed):
                    parsed_date = parsed.date()
                    if 1900 <= parsed_date.year <= date.today().year:
                        return parsed_date
            except Exception:
                pass

            return None if not return_string_na else 'NA'
        except Exception:
            return None if not return_string_na else 'NA'

    def safe_num(self, val, default=0):
        try: 
            cleaned = str(val).replace('₹','').replace('$','').replace(',','').replace(' ','').strip()
            return float(cleaned) if cleaned else default
        except: 
            return default
    
    def calc_age(self, dob):
        if pd.isna(dob) or dob is None or dob > date.today(): 
            return 0
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return max(0, min(age, 120))  # Ensure reasonable age range

    # --- TRANSFORMATION FUNCTIONS WITHOUT ORIGINAL ID COLUMNS ---

    def transform_branches(self):
        """Transform branches - keep original branch_id as string"""
        print("Transforming Branches...")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        cursor = self.staging_connection.cursor()
        try:
            cursor.execute("SELECT * FROM staging_branches LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall()
            all_data = self.fetch_data_in_batches(cursor, "staging_branches", "branch_id")
        finally:
            cursor.close()

        if not all_data:
            print("  No data in staging.")
            return

        df = pd.DataFrame(all_data, columns=columns)
        self.stats['branches']['processed'] = len(df)
        
        # Remove duplicates based on original branch_id
        df_clean = df.drop_duplicates(subset=['branch_id'], keep='first').copy()
        
        # Keep original branch_id as string (like "QT0021")
        df_clean['branch_id'] = df_clean['branch_id'].apply(lambda x: self.safe_val(x, 'NA'))
        
        # Clean data
        df_clean['branch_name'] = df_clean['branch_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['city'] = df_clean['city'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['state'] = df_clean['state'].apply(lambda x: self.safe_val(x, 'NA', upper=True))
        df_clean['manager_name'] = df_clean['manager_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        # Region mapping
        def map_region(state):
            state_upper = str(state).upper()
            if any(x in state_upper for x in ['DELHI','PUNJAB','HARYANA','UP','UTTAR']): 
                return 'North'
            if any(x in state_upper for x in ['MAHARASHTRA','GUJARAT','GOA']): 
                return 'West'
            if any(x in state_upper for x in ['KARNATAKA','TAMIL','KERALA','ANDHRA','TELANGANA']): 
                return 'South'
            if any(x in state_upper for x in ['BENGAL','BIHAR','ODISHA','ASSAM','MEGHALAYA','CHHATTISGARH','ARUNACHAL','MANIPUR']): 
                return 'East'
            return 'NA'
        
        df_clean['region'] = df_clean['state'].apply(map_region)

        # Insert data
        tcursor = self.transformed_connection.cursor()
        cols = ['branch_id', 'branch_name', 'city', 'state', 'manager_name', 'region']
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        if batch_data:
            tcursor.executemany("""
                INSERT INTO transformed_branches (branch_id, branch_name, city, state, manager_name, region)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, batch_data)
            self.transformed_connection.commit()
            self.stats['branches']['transformed'] = len(batch_data)
            print(f"  ✓ Transformed {len(batch_data)} branches with original branch IDs")
        
        tcursor.close()

    def transform_customers(self):
        """Transform customers - keep original branch_id as string, sequential customer_id"""
        print("Transforming Customers...")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        cursor = self.staging_connection.cursor()
        try:
            cursor.execute("SELECT * FROM staging_customers LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall()
            all_data = self.fetch_data_in_batches(cursor, "staging_customers", "customer_id")
        finally:
            cursor.close()

        if not all_data:
            print("  No data in staging.")
            return

        df = pd.DataFrame(all_data, columns=columns)
        self.stats['customers']['processed'] = len(df)
        
        # Remove duplicates
        df_clean = df.drop_duplicates(subset=['customer_id'], keep='first').copy()
        
        # Generate proper sequential customer_id starting from 1
        df_clean = df_clean.reset_index(drop=True)
        df_clean['customer_id'] = df_clean.index + 1  # This ensures 1, 2, 3, 4...
        
        # Keep original branch_id as string (like "QT0021")
        df_clean['branch_id'] = df_clean['branch_id'].apply(lambda x: self.safe_val(x, 'NA'))
        
        # Clean data
        df_clean['first_name'] = df_clean['first_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['last_name'] = df_clean['last_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        df_clean['dob'] = df_clean['dob'].apply(lambda x: self.safe_date(x, return_string_na=False))
        df_clean['account_open_date'] = df_clean['account_open_date'].apply(lambda x: self.safe_date(x, return_string_na=False))
        
        df_clean['age'] = df_clean['dob'].apply(self.calc_age)
        
        # Calculate tenure days
        today = pd.to_datetime(date.today())
        df_clean['customer_tenure_days'] = (today - pd.to_datetime(df_clean['account_open_date'])).dt.days
        df_clean['customer_tenure_days'] = df_clean['customer_tenure_days'].fillna(0).astype(int)
        df_clean.loc[df_clean['customer_tenure_days'] < 0, 'customer_tenure_days'] = 0
        
        # Customer segmentation
        conditions = [
            df_clean['customer_tenure_days'] >= 730,
            df_clean['customer_tenure_days'] >= 180,
            df_clean['customer_tenure_days'] > 0
        ]
        choices = ['VIP', 'Regular', 'New']
        df_clean['customer_segment'] = np.select(conditions, choices, default='NA')
        
        df_clean['email'] = df_clean['email'].apply(lambda x: self.safe_val(x, 'NA', lower=True))
        df_clean['phone'] = df_clean['phone'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['address'] = df_clean['address'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        # Gender standardization
        gender_map = {'m': 'Male', 'f': 'Female', 'male': 'Male', 'female': 'Female'}
        df_clean['gender'] = (df_clean['gender']
                             .apply(lambda x: self.safe_val(x, 'NA'))
                             .str.lower()
                             .map(gender_map)
                             .fillna('NA'))
        
        df_clean['outlier_flag'] = False
        
        # Handle NaN values for database insertion
        df_clean = df_clean.replace({np.nan: None})
        
        # Insert data in batches
        tcursor = self.transformed_connection.cursor()
        cols = ['customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 'age', 'gender', 'email', 'phone', 'address', 'account_open_date', 'customer_tenure_days', 'customer_segment', 'outlier_flag']
        
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        for i in range(0, len(batch_data), self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            tcursor.executemany("""
                INSERT INTO transformed_customers 
                (customer_id, branch_id, first_name, last_name, dob, age, gender, email, phone, address, account_open_date, customer_tenure_days, customer_segment, outlier_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
        
        self.transformed_connection.commit()
        self.stats['customers']['transformed'] = len(batch_data)
        print(f"  ✓ Transformed {len(batch_data)} customers with sequential IDs: 1 to {len(batch_data)}")
        print(f"  ✓ Branch IDs preserved as original strings (QT0021, etc.)")
        tcursor.close()

    def transform_loans(self):
        """Transform loans with proper sequential IDs starting from 1"""
        print("Transforming Loans...")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        cursor = self.staging_connection.cursor()
        try:
            cursor.execute("SELECT * FROM staging_loans LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall()
            all_data = self.fetch_data_in_batches(cursor, "staging_loans", "loan_id")
        finally:
            cursor.close()

        if not all_data:
            print("  No data in staging.")
            return

        df = pd.DataFrame(all_data, columns=columns)
        self.stats['loans']['processed'] = len(df)
        
        # Remove duplicates
        df_clean = df.drop_duplicates(subset=['loan_id'], keep='first').copy()
        
        # Generate proper sequential loan_id starting from 1
        df_clean = df_clean.reset_index(drop=True)
        df_clean['loan_id'] = df_clean.index + 1  # This ensures 1, 2, 3, 4...
        
        # Map customer_id to sequential ID from transformed_customers
        cust_cursor = self.transformed_connection.cursor()
        cust_cursor.execute("SELECT customer_id FROM transformed_customers")
        valid_customer_ids = set(row[0] for row in cust_cursor.fetchall())
        cust_cursor.close()
        
        # Filter to only include valid customer_ids
        df_clean['customer_id'] = df_clean['customer_id'].astype(int)
        df_clean = df_clean[df_clean['customer_id'].isin(valid_customer_ids)].copy()
        
        # Clean data
        df_clean['loan_amount'] = df_clean['loan_amount'].apply(lambda x: self.safe_num(x, 0))
        df_clean['loan_type'] = df_clean['loan_type'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['loan_status'] = df_clean['loan_status'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['interest_rate'] = df_clean['interest_rate'].apply(lambda x: self.safe_num(x, 0))
        
        df_clean['start_date'] = df_clean['start_date'].apply(lambda x: self.safe_date(x))
        df_clean['end_date'] = df_clean['end_date'].apply(lambda x: self.safe_date(x))

        # Calculate loan duration months
        start_dates = pd.to_datetime(df_clean['start_date'], errors='coerce')
        end_dates = pd.to_datetime(df_clean['end_date'], errors='coerce')
        
        df_clean['loan_duration_months'] = ((end_dates.dt.year - start_dates.dt.year) * 12 + 
                                          (end_dates.dt.month - start_dates.dt.month))
        df_clean['loan_duration_months'] = df_clean['loan_duration_months'].fillna(0).astype(int)
        df_clean.loc[df_clean['loan_duration_months'] < 0, 'loan_duration_months'] = 0

        # Risk categorization
        conditions = [
            df_clean['loan_amount'] > 500000,
            df_clean['loan_amount'] > 100000
        ]
        choices = ['High', 'Medium']
        df_clean['risk_category'] = np.select(conditions, choices, default='Low')
        
        df_clean['outlier_flag'] = False
        
        # Handle NaN values for database insertion
        df_clean = df_clean.replace({np.nan: None})

        # Insert data in batches
        tcursor = self.transformed_connection.cursor()
        cols = ['loan_id', 'customer_id', 'loan_type', 'loan_amount', 'interest_rate', 'start_date', 'end_date', 'loan_status', 'loan_duration_months', 'risk_category', 'outlier_flag']
        
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        for i in range(0, len(batch_data), self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            tcursor.executemany("""
                INSERT INTO transformed_loans 
                (loan_id, customer_id, loan_type, loan_amount, interest_rate, start_date, end_date, loan_status, loan_duration_months, risk_category, outlier_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
        
        self.transformed_connection.commit()
        self.stats['loans']['transformed'] = len(batch_data)
        print(f"  ✓ Transformed {len(batch_data)} loans with sequential IDs: 1 to {len(batch_data)}")
        tcursor.close()

    def transform_transactions(self):
        """Transform transactions with proper sequential IDs starting from 1"""
        print("Transforming Transactions...")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        cursor = self.staging_connection.cursor()
        try:
            cursor.execute("SELECT * FROM staging_transactions LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall()
            all_data = self.fetch_data_in_batches(cursor, "staging_transactions", "transaction_id")
        finally:
            cursor.close()

        if not all_data:
            print("  No data in staging.")
            return

        df = pd.DataFrame(all_data, columns=columns)
        self.stats['transactions']['processed'] = len(df)
        
        # Remove duplicates
        df_clean = df.drop_duplicates(subset=['transaction_id'], keep='first').copy()
        
        # Generate proper sequential transaction_id starting from 1
        df_clean = df_clean.reset_index(drop=True)
        df_clean['transaction_id'] = df_clean.index + 1  # This ensures 1, 2, 3, 4...
        
        # Map customer_id to sequential ID from transformed_customers
        cust_cursor = self.transformed_connection.cursor()
        cust_cursor.execute("SELECT customer_id FROM transformed_customers")
        valid_customer_ids = set(row[0] for row in cust_cursor.fetchall())
        cust_cursor.close()
        
        # Filter to only include valid customer_ids
        df_clean['customer_id'] = df_clean['customer_id'].astype(int)
        df_clean = df_clean[df_clean['customer_id'].isin(valid_customer_ids)].copy()
        
        # Clean data
        df_clean['amount'] = df_clean['amount'].apply(lambda x: self.safe_num(x, 0))
        df_clean['transaction_date'] = df_clean['transaction_date'].apply(lambda x: self.safe_date(x))
        df_clean['transaction_type'] = df_clean['transaction_type'].apply(lambda x: self.safe_val(x, 'NA', upper=True))
        df_clean['balance_after'] = df_clean['balance_after'].apply(lambda x: self.safe_num(x, 0))
        
        # Fraud flag conversion
        fraud_map = {'true': True, '1': True, 'yes': True, 'y': True}
        df_clean['fraud_flag'] = (df_clean['fraud_flag']
                                 .astype(str)
                                 .str.lower()
                                 .map(fraud_map)
                                 .fillna(False)
                                 .astype(bool))
        
        # Transaction categorization
        conditions = [
            df_clean['amount'] > 10000,
            df_clean['amount'] > 1000
        ]
        choices = ['Large', 'Medium']
        df_clean['transaction_category'] = np.select(conditions, choices, default='Small')
        
        df_clean['outlier_flag'] = False
        
        # Handle NaN values for database insertion
        df_clean = df_clean.replace({np.nan: None})

        # Insert data in batches
        tcursor = self.transformed_connection.cursor()
        cols = ['transaction_id', 'customer_id', 'transaction_date', 'transaction_type', 'amount', 'balance_after', 'fraud_flag', 'transaction_category', 'outlier_flag']
        
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        for i in range(0, len(batch_data), self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            tcursor.executemany("""
                INSERT INTO transformed_transactions 
                (transaction_id, customer_id, transaction_date, transaction_type, amount, balance_after, fraud_flag, transaction_category, outlier_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
        
        self.transformed_connection.commit()
        self.stats['transactions']['transformed'] = len(batch_data)
        print(f"  ✓ Transformed {len(batch_data)} transactions with sequential IDs: 1 to {len(batch_data)}")
        tcursor.close()

    def export_csv(self):
        """Export transformed data to CSV"""
        print("\nExporting transformed data to CSV...")
        files = []
        
        for table in ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']:
            print(f"  Exporting {table}...")
            self.transformed_connection.ping(reconnect=True)
            cursor = self.transformed_connection.cursor()
            
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                columns = [c[0] for c in cursor.description]
                cursor.fetchall()
                
                all_data = self.fetch_data_in_batches(cursor, table, "display_id")
                if not all_data:
                    continue

                df = pd.DataFrame(all_data, columns=columns)
                df = df.fillna('NA')
                
                filename = f"transformed_{table.replace('transformed_', '')}.csv"
                filepath = self.exports_dir / filename
                df.to_csv(filepath, index=False)
                files.append(filename)
                print(f"    ✓ Exported {len(df)} records to {filename}")
            finally:
                cursor.close()
        
        return files

    def print_summary(self):
        """Print transformation summary"""
        print("\n" + "="*50)
        print("TRANSFORMATION COMPLETED SUCCESSFULLY")
        print("="*50)
        total_transformed = 0
        
        for table, stats in self.stats.items():
            transformed = stats['transformed']
            total_transformed += transformed
            print(f"{table.title():12} : {transformed:>6} records")
        
        print("-"*50)
        print(f"{'Total':12} : {total_transformed:>6} records")
        print("✓ All main IDs have been re-sequenced to 1, 2, 3, 4...")
        print("✓ Branch IDs preserved as original strings (QT0021, etc.)")

    def run_transformation(self):
        """Main transformation pipeline"""
        try:
            print("\n" + "="*60)
            print("STARTING DATA TRANSFORMATION")
            print("="*60)
            
            self.connect_databases()
            self.create_transformed_tables()
            self.reset_sequence_numbers()
            
            start_time = time.time()
            
            # Run transformations in order to maintain referential integrity
            self.transform_branches()
            self.transform_customers() 
            self.transform_loans()
            self.transform_transactions()
            
            files = self.export_csv()
            self.print_summary()
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            print(f"\nExecution time: {execution_time:.2f} seconds")
            print("✓ Transformation pipeline completed successfully!")
            
            return {'stats': self.stats, 'files': files}
            
        except Exception as e:
            print(f"\n❌ Transformation Error: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            if self.staging_connection:
                self.staging_connection.close()
            if self.transformed_connection:
                self.transformed_connection.close()

def main():
    """Main function"""
    config = None
    
    # Try to load config from various locations
    try:
        from config.config import config as cfg
        config = cfg
        print("Config loaded from config.config")
    except ImportError:
        pass
    
    if config is None:
        try:
            from config import config as cfg
            config = cfg
            print("Config loaded from config")
        except ImportError:
            pass
    
    if config is None:
        print("Warning: Config file not found. Using defaults (localhost).")
        config = {
            'MYSQL_HOST': '127.0.0.1',
            'MYSQL_USER': 'root',
            'MYSQL_PASSWORD': 'Mysql@1234',
            'MYSQL_DATABASE': 'stagging',
            'MYSQL_PORT': 3306
        }

    # Run transformation
    transformer = DataTransformer(config)
    transformer.run_transformation()

if __name__ == "__main__":
    main()