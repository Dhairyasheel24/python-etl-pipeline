import pandas as pd
import mysql.connector
from datetime import date, datetime, timedelta
import re
import logging
import sys
import os
from pathlib import Path
import time
import numpy as np # Import numpy for date handling

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataTransformer:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.staging_connection = None
        self.transformed_connection = None
        self.exports_dir = Path("exports")
        self.exports_dir.mkdir(exist_ok=True)
        
        self.batch_size = 1000  # For both fetching and inserting
        self.query_timeout = 300
        
        self.stats = {
            'branches': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0},
            'customers': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0},
            'loans': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0},
            'transactions': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0}
        }
        
        self.quality = {}
    
    # ... connect_databases, create_transformed_tables, fetch_data_in_batches ...
    # (These are unchanged from our last version)
    def connect_databases(self):
        """Connect to databases"""
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
            
            print("Connected to databases")
            
        except mysql.connector.Error as e:
            self.logger.error(f"Connection error: {e}")
            raise
    
    def create_transformed_tables(self):
        """Create tables with sequential display IDs"""
        self.transformed_connection.ping(reconnect=True)
        cursor = self.transformed_connection.cursor()
        
        tables = ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        # (Table creation SQL is unchanged)
        # Branches
        cursor.execute("""
            CREATE TABLE transformed_branches (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                branch_id VARCHAR(20) UNIQUE,
                branch_name VARCHAR(100) DEFAULT 'NA',
                city VARCHAR(50) DEFAULT 'NA',
                state VARCHAR(50) DEFAULT 'NA',
                manager_name VARCHAR(100) DEFAULT 'NA',
                region VARCHAR(20) DEFAULT 'NA'
            )
        """)
        
        # Customers
        cursor.execute("""
            CREATE TABLE transformed_customers (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id VARCHAR(20) UNIQUE,
                branch_id VARCHAR(20) DEFAULT 'NA',
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
                outlier_flag BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Loans
        cursor.execute("""
            CREATE TABLE transformed_loans (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                loan_id VARCHAR(20) UNIQUE,
                customer_id VARCHAR(20) DEFAULT 'NA',
                loan_type VARCHAR(50) DEFAULT 'NA',
                loan_amount DECIMAL(15,2),
                interest_rate DECIMAL(5,2),
                start_date DATE,
                end_date DATE,
                loan_status VARCHAR(50) DEFAULT 'NA',
                loan_duration_months INT DEFAULT 0,
                risk_category VARCHAR(20) DEFAULT 'NA',
                outlier_flag BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Transactions
        cursor.execute("""
            CREATE TABLE transformed_transactions (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                transaction_id VARCHAR(20) UNIQUE,
                customer_id VARCHAR(20) DEFAULT 'NA',
                transaction_date DATE,
                transaction_type VARCHAR(50) DEFAULT 'NA',
                amount DECIMAL(15,2),
                balance_after DECIMAL(15,2) DEFAULT 0,
                fraud_flag BOOLEAN DEFAULT FALSE,
                transaction_category VARCHAR(20) DEFAULT 'NA',
                outlier_flag BOOLEAN DEFAULT FALSE
            )
        """)
        
        self.transformed_connection.commit()
        print("Tables created with sequential IDs")
    
    def fetch_data_in_batches(self, cursor, table_name, primary_key):
        """Fetch data in batches to prevent timeout"""
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
                if len(all_data) % (self.batch_size * 10) == 0:
                     print(f"  Fetched {len(all_data)} records...")
                
            except mysql.connector.Error as e:
                print(f"  Error fetching batch for {table_name}: {e}")
                break
        
        print(f"  Total fetched: {len(all_data)} records")
        return all_data

    # ... (All safe_ utility functions are unchanged) ...
    def safe_val(self, val, default='NA', title=False, upper=False, lower=False):
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return default
        result = str(val).strip()
        if not result: return default
        if title: result = result.title()
        if upper: result = result.upper()
        if lower: result = result.lower()
        return result

    def safe_date(self, val, return_string_na=False):
        # This function is complex, so we'll still use .apply()
        # but it's better than full iterrows()
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return pd.NaT if not return_string_na else 'NA'
        try:
            val_str = str(val).strip()
            if not val_str: return pd.NaT if not return_string_na else 'NA'
            
            # Using pandas to_datetime is much faster and more robust
            parsed = pd.to_datetime(val_str, errors='coerce', dayfirst=True)
            if pd.notna(parsed):
                parsed_date = parsed.date()
                current_year = date.today().year
                if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                        return parsed_date
            
            # Fallback for 2-digit years if pandas fails
            two_digit_match = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$', val_str)
            if two_digit_match:
                day, month, year = two_digit_match.groups()
                day, month, year_2digit = int(day), int(month), int(year)
                year_4digit = 2000 + year_2digit if year_2digit <= 49 else 1900 + year_2digit
                if 1 <= month <= 12 and 1 <= day <= 31:
                    try:
                        parsed_date = date(year_4digit, month, day)
                        current_year = date.today().year
                        if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                            return parsed_date
                    except ValueError: pass
            
            return pd.NaT if not return_string_na else 'NA'
        except Exception as e:
            return pd.NaT if not return_string_na else 'NA'

    def safe_num(self, val, default=0):
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return default
        try: 
            cleaned = str(val).replace('₹','').replace('$','').replace(',','').replace(' ','').strip()
            if not cleaned: return default
            return float(cleaned)
        except: 
            return default
    
    def calc_age(self, dob):
        # Optimized for pandas .apply()
        if pd.isna(dob) or dob is None or dob > date.today():
            return 0
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age if 0 <= age <= 120 else 0
    
    # calc_quality is fine, no change needed
    def calc_quality(self, df, table, required_cols):
        total = len(df)
        if total == 0:
            completeness, accuracy, dup_rate = 0, 0, 0
        else:
            completeness = (df.count().sum() / (total * len(df.columns)) * 100)
            accuracy = (total / self.stats[table]['processed'] * 100) if self.stats[table]['processed'] > 0 else 0
            dup_rate = (self.stats[table]['duplicates'] / self.stats[table]['processed'] * 100) if self.stats[table]['processed'] > 0 else 0
        
        self.quality[table] = {
            'completeness': completeness,
            'accuracy': accuracy,
            'duplication_rate': dup_rate,
            'outlier_rate': (self.stats[table].get('outliers', 0) / total * 100) if total > 0 else 0
        }

    # ---
    # NEW, FULLY OPTIMIZED TRANSFORM FUNCTIONS
    # ---

    def transform_branches(self):
        """Transform branches (Vectorized)"""
        print("\nTRANSFORMING BRANCHES")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        cursor = self.staging_connection.cursor()
        try:
            cursor.execute("SELECT * FROM staging_branches LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall()
            cursor.execute("SELECT COUNT(*) FROM staging_branches")
            total_count = cursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error: {e}")
            cursor.close()
            return
        
        all_data = self.fetch_data_in_batches(cursor, "staging_branches", "branch_id")
        cursor.close() 
        if not all_data:
            print("  No data found")
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        self.stats['branches']['processed'] = len(df)
        
        df_clean = df.drop_duplicates(subset=['branch_id'], keep='first').copy()
        duplicates_removed = len(df) - len(df_clean)
        self.stats['branches']['duplicates'] = duplicates_removed
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate branches")
        
        # --- Vectorized Transformations ---
        df_clean['branch_name'] = df_clean['branch_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['city'] = df_clean['city'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['state'] = df_clean['state'].apply(lambda x: self.safe_val(x, 'NA', upper=True))
        df_clean['manager_name'] = df_clean['manager_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        # Region mapping
        north = ['DELHI', 'PUNJAB', 'HARYANA', 'UP', 'UTTAR']
        west = ['MAHARASHTRA', 'GUJARAT', 'GOA']
        south = ['KARNATAKA', 'TAMIL', 'KERALA', 'ANDHRA', 'TELANGANA']
        east = ['BENGAL', 'BIHAR', 'ODISHA', 'ASSAM', 'MEGHALAYA', 'CHHATTISGARH', 'ARUNACHAL', 'MANIPUR']
        
        def map_region(state):
            if any(x in state for x in north): return 'North'
            if any(x in state for x in west): return 'West'
            if any(x in state for x in south): return 'South'
            if any(x in state for x in east): return 'East'
            return 'NA'
        
        df_clean['region'] = df_clean['state'].apply(map_region)
        df_clean['branch_id'] = df_clean['branch_id'].apply(lambda x: self.safe_val(x, 'NA'))
        
        self.stats['branches']['nulls'] = df_clean[['branch_name', 'city', 'state', 'manager_name']].isin(['NA']).sum().sum()
        
        # --- Batch Insert ---
        tcursor = self.transformed_connection.cursor()
        cols_to_insert = ['branch_id', 'branch_name', 'city', 'state', 'manager_name', 'region']
        batch_data = [tuple(x) for x in df_clean[cols_to_insert].to_numpy()]
        
        if batch_data:
            try:
                tcursor.executemany("""
                    INSERT INTO transformed_branches (branch_id, branch_name, city, state, manager_name, region)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, batch_data)
                self.stats['branches']['transformed'] = len(batch_data)
            except mysql.connector.Error as e:
                if "Duplicate entry" not in str(e): print(f"Error inserting branches: {e}")
        
        self.transformed_connection.commit()
        
        tcursor.execute("SELECT * FROM transformed_branches")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'branches', ['branch_id', 'branch_name'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['branches']['transformed']}")
        print(f"  Duplicates: {self.stats['branches']['duplicates']}, NULLs: {self.stats['branches']['nulls']}")

    def transform_customers(self):
        """Transform customers (Vectorized)"""
        print("\nTRANSFORMING CUSTOMERS")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        cursor = self.staging_connection.cursor()
        try:
            cursor.execute("SELECT * FROM staging_customers LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall()
            cursor.execute("SELECT COUNT(*) FROM staging_customers")
            total_count = cursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error: {e}")
            cursor.close()
            return

        all_data = self.fetch_data_in_batches(cursor, "staging_customers", "customer_id")
        cursor.close() 
        if not all_data:
            print("  No data found")
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        self.stats['customers']['processed'] = len(df)
        
        df_clean = df.drop_duplicates(subset=['customer_id'], keep='first').copy()
        duplicates_removed = len(df) - len(df_clean)
        self.stats['customers']['duplicates'] = duplicates_removed
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate customers")
        
        # --- Vectorized Transformations ---
        print("  Applying transformations...")
        df_clean['customer_id'] = df_clean['customer_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['first_name'] = df_clean['first_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['last_name'] = df_clean['last_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['branch_id'] = df_clean['branch_id'].apply(lambda x: self.safe_val(x, 'NA'))
        
        df_clean['dob'] = df_clean['dob'].apply(lambda x: self.safe_date(x, return_string_na=False))
        df_clean['account_open_date'] = df_clean['account_open_date'].apply(lambda x: self.safe_date(x, return_string_na=False))
        
        df_clean['age'] = df_clean['dob'].apply(self.calc_age)
        
        # Vectorized tenure calculation
        today = pd.to_datetime(date.today())
        df_clean['customer_tenure_days'] = (today - pd.to_datetime(df_clean['account_open_date'])).dt.days
        df_clean['customer_tenure_days'] = df_clean['customer_tenure_days'].fillna(0).astype(int)
        df_clean.loc[df_clean['customer_tenure_days'] < 0, 'customer_tenure_days'] = 0
        
        # Vectorized segmentation
        conditions = [
            (df_clean['customer_tenure_days'] >= 730),
            (df_clean['customer_tenure_days'] >= 180),
            (df_clean['customer_tenure_days'] > 0)
        ]
        choices = ['VIP', 'Regular', 'New']
        df_clean['customer_segment'] = np.select(conditions, choices, default='NA')
        
        df_clean['email'] = df_clean['email'].apply(lambda x: self.safe_val(x, 'NA', lower=True))
        df_clean['phone'] = df_clean['phone'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['address'] = df_clean['address'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        # Gender mapping
        gender_map = {'m': 'Male', 'f': 'Female', 'male': 'Male', 'female': 'Female'}
        df_clean['gender'] = df_clean['gender'].apply(lambda x: self.safe_val(x, 'NA')).str.lower().map(gender_map).fillna('NA')
        
        df_clean['outlier_flag'] = False
        
        self.stats['customers']['nulls'] = df_clean[['email', 'phone', 'gender']].isin(['NA']).sum().sum()
        self.stats['customers']['nulls'] += df_clean['dob'].isna().sum()
        self.stats['customers']['nulls'] += df_clean['account_open_date'].isna().sum()

        # Handle NaT for SQL (convert to None)
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        
        # --- Batch Insert ---
        print("  Starting batch inserts...")
        tcursor = self.transformed_connection.cursor()
        cols_to_insert = [
            'customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 'age', 'gender',
            'email', 'phone', 'address', 'account_open_date', 'customer_tenure_days', 
            'customer_segment', 'outlier_flag'
        ]
        
        batch_data = [tuple(x) for x in df_clean[cols_to_insert].to_numpy()]

        total_rows = len(batch_data)
        for i in range(0, total_rows, self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            try:
                tcursor.executemany("""
                    INSERT INTO transformed_customers 
                    (customer_id, branch_id, first_name, last_name, dob, age, gender, 
                     email, phone, address, account_open_date, customer_tenure_days, 
                     customer_segment, outlier_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                self.stats['customers']['transformed'] += len(batch)
                print(f"  Inserted {self.stats['customers']['transformed']}/{total_rows} customers...")
            except mysql.connector.Error as e:
                if "Duplicate entry" not in str(e): print(f"Error inserting customers: {e}")
        
        self.transformed_connection.commit()
        
        tcursor.execute("SELECT * FROM transformed_customers")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'customers', ['customer_id', 'first_name'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['customers']['transformed']}")
        print(f"  Duplicates: {self.stats['customers']['duplicates']}, NULLs: {self.stats['customers']['nulls']}")

    def transform_loans(self):
        """Transform loans (Vectorized)"""
        print("\nTRANSFORMING LOANS")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        tcursor = self.transformed_connection.cursor()
        try:
            tcursor.execute("SELECT customer_id FROM transformed_customers")
            valid_custs = set(r[0] for r in tcursor.fetchall())
        except mysql.connector.Error as e:
            print(f"  Failed to get valid customers: {e}")
            tcursor.close()
            return
        
        scursor = self.staging_connection.cursor()
        try:
            scursor.execute("SELECT * FROM staging_loans LIMIT 1")
            columns = [c[0] for c in scursor.description]
            scursor.fetchall()
            scursor.execute("SELECT COUNT(*) FROM staging_loans")
            total_count = scursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error: {e}")
            scursor.close()
            tcursor.close()
            return

        all_data = self.fetch_data_in_batches(scursor, "staging_loans", "loan_id")
        scursor.close()
        if not all_data:
            print("  No data found")
            tcursor.close()
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        self.stats['loans']['processed'] = len(df)
        
        df_clean = df.drop_duplicates(subset=['loan_id'], keep='first').copy()
        duplicates_removed = len(df) - len(df_clean)
        self.stats['loans']['duplicates'] = duplicates_removed
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate loans")
        
        # --- Vectorized Transformations ---
        print("  Applying transformations...")
        df_clean['customer_id'] = df_clean['customer_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['loan_amount'] = df_clean['loan_amount'].apply(lambda x: self.safe_num(x, 0))
        
        # Filter out invalid rows FIRST
        df_clean = df_clean[df_clean['customer_id'].isin(valid_custs)]
        df_clean = df_clean[df_clean['loan_amount'] > 0]
        
        df_clean['loan_id'] = df_clean['loan_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['loan_type'] = df_clean['loan_type'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['loan_status'] = df_clean['loan_status'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['interest_rate'] = df_clean['interest_rate'].apply(lambda x: self.safe_num(x, 0))
        
        df_clean['start_date'] = df_clean['start_date'].apply(lambda x: self.safe_date(x, return_string_na=False))
        df_clean['end_date'] = df_clean['end_date'].apply(lambda x: self.safe_date(x, return_string_na=False))

        # Vectorized duration calculation
        df_clean['loan_duration_months'] = (pd.to_datetime(df_clean['end_date']).dt.to_period('M') - 
                                            pd.to_datetime(df_clean['start_date']).dt.to_period('M'))
        df_clean['loan_duration_months'] = df_clean['loan_duration_months'].apply(lambda x: x.n if pd.notna(x) else 0)
        df_clean.loc[df_clean['loan_duration_months'] < 0, 'loan_duration_months'] = 0

        # Vectorized risk category
        conditions = [
            (df_clean['loan_amount'] > 500000),
            (df_clean['loan_amount'] > 100000)
        ]
        choices = ['High', 'Medium']
        df_clean['risk_category'] = np.select(conditions, choices, default='Low')
        
        df_clean['outlier_flag'] = False
        
        self.stats['loans']['nulls'] = df_clean['end_date'].isna().sum()
        
        # Handle NaT for SQL (convert to None)
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        
        # --- Batch Insert ---
        print("  Starting batch inserts...")
        cols_to_insert = [
            'loan_id', 'customer_id', 'loan_type', 'loan_amount', 'interest_rate', 
            'start_date', 'end_date', 'loan_status', 'loan_duration_months', 
            'risk_category', 'outlier_flag'
        ]
        
        batch_data = [tuple(x) for x in df_clean[cols_to_insert].to_numpy()]
        
        total_rows = len(batch_data)
        for i in range(0, total_rows, self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            try:
                tcursor.executemany("""
                    INSERT INTO transformed_loans 
                    (loan_id, customer_id, loan_type, loan_amount, interest_rate, 
                     start_date, end_date, loan_status, loan_duration_months, 
                     risk_category, outlier_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                self.stats['loans']['transformed'] += len(batch)
                print(f"  Inserted {self.stats['loans']['transformed']}/{total_rows} loans...")
            except mysql.connector.Error as e:
                if "Duplicate entry" not in str(e): print(f"Error inserting loans: {e}")
        
        self.transformed_connection.commit()
        
        tcursor.execute("SELECT * FROM transformed_loans")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'loans', ['loan_id', 'customer_id'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['loans']['transformed']}")
        print(f"  Duplicates: {self.stats['loans']['duplicates']}, NULLs: {self.stats['loans']['nulls']}")

    def transform_transactions(self):
        """Transform transactions (Vectorized)"""
        print("\nTRANSFORMING TRANSACTIONS")
        self.staging_connection.ping(reconnect=True)
        self.transformed_connection.ping(reconnect=True)
        
        tcursor = self.transformed_connection.cursor()
        try:
            tcursor.execute("SELECT customer_id FROM transformed_customers")
            valid_custs = set(r[0] for r in tcursor.fetchall())
        except mysql.connector.Error as e:
            print(f"  Failed to get valid customers: {e}")
            tcursor.close()
            return
        
        scursor = self.staging_connection.cursor()
        try:
            scursor.execute("SELECT * FROM staging_transactions LIMIT 1")
            columns = [c[0] for c in scursor.description]
            scursor.fetchall()
            scursor.execute("SELECT COUNT(*) FROM staging_transactions")
            total_count = scursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error: {e}")
            scursor.close()
            tcursor.close()
            return

        all_data = self.fetch_data_in_batches(scursor, "staging_transactions", "transaction_id")
        scursor.close()
        if not all_data:
            print("  No data found")
            tcursor.close()
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        self.stats['transactions']['processed'] = len(df)
        
        df_clean = df.drop_duplicates(subset=['transaction_id'], keep='first').copy()
        duplicates_removed = len(df) - len(df_clean)
        self.stats['transactions']['duplicates'] = duplicates_removed
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate transactions")
        
        # --- Vectorized Transformations ---
        print("  Applying transformations...")
        df_clean['customer_id'] = df_clean['customer_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['amount'] = df_clean['amount'].apply(lambda x: self.safe_num(x, 0))
        df_clean['transaction_date'] = df_clean['transaction_date'].apply(lambda x: self.safe_date(x, return_string_na=False))
        
        # Filter out invalid rows FIRST
        df_clean = df_clean[df_clean['customer_id'].isin(valid_custs)]
        df_clean = df_clean[df_clean['amount'] > 0]
        df_clean = df_clean[pd.notna(df_clean['transaction_date'])]
        
        df_clean['transaction_id'] = df_clean['transaction_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['transaction_type'] = df_clean['transaction_type'].apply(lambda x: self.safe_val(x, 'NA', upper=True))
        df_clean['balance_after'] = df_clean['balance_after'].apply(lambda x: self.safe_num(x, 0))
        
        # Vectorized fraud flag
        fraud_map = {'true': True, '1': True, 'yes': True, 'y': True}
        df_clean['fraud_flag'] = df_clean['fraud_flag'].astype(str).str.lower().map(fraud_map).fillna(False)
        
        # Vectorized category
        conditions = [
            (df_clean['amount'] > 10000),
            (df_clean['amount'] > 1000)
        ]
        choices = ['Large', 'Medium']
        df_clean['transaction_category'] = np.select(conditions, choices, default='Small')
        
        df_clean['outlier_flag'] = False
        
        self.stats['transactions']['nulls'] = (df_clean['balance_after'] == 0).sum()
        
        # Handle NaT for SQL (convert to None)
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        
        # --- Batch Insert ---
        print("  Starting batch inserts...")
        cols_to_insert = [
            'transaction_id', 'customer_id', 'transaction_date', 'transaction_type', 
            'amount', 'balance_after', 'fraud_flag', 'transaction_category', 'outlier_flag'
        ]
        
        batch_data = [tuple(x) for x in df_clean[cols_to_insert].to_numpy()]
        
        total_rows = len(batch_data)
        for i in range(0, total_rows, self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            try:
                tcursor.executemany("""
                    INSERT INTO transformed_transactions 
                    (transaction_id, customer_id, transaction_date, transaction_type, 
                     amount, balance_after, fraud_flag, transaction_category, outlier_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                self.stats['transactions']['transformed'] += len(batch)
                print(f"  Inserted {self.stats['transactions']['transformed']}/{total_rows} transactions...")
            except mysql.connector.Error as e:
                if "Duplicate entry" not in str(e): print(f"Error inserting transactions: {e}")

        self.transformed_connection.commit()
        
        tcursor.execute("SELECT * FROM transformed_transactions")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'transactions', ['transaction_id', 'customer_id'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['transactions']['transformed']}")
        print(f"  Duplicates: {self.stats['transactions']['duplicates']}, NULLs: {self.stats['transactions']['nulls']}")

    def export_csv(self):
        """Export to CSV (Vectorized)"""
        print("\nEXPORTING CSV")
        files = []
        
        for table in ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']:
            print(f"  Exporting {table}...")
            self.transformed_connection.ping(reconnect=True)
            
            cursor = self.transformed_connection.cursor()
            
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                columns = [c[0] for c in cursor.description]
                cursor.fetchall()
            except mysql.connector.Error as e:
                print(f"  Error getting columns for {table}: {e}")
                cursor.close()
                continue

            all_data = self.fetch_data_in_batches(cursor, table, "display_id")
            cursor.close()
            
            if not all_data:
                print(f"  No data found in {table} to export.")
                continue

            df = pd.DataFrame(all_data, columns=columns)
            
            # Vectorized NA replacement
            for col in df.columns:
                df[col] = df[col].astype(str).replace([
                    'None', 'NaN', 'nan', 'NaT', '<NA>', 'NULL', 'null', '', 'NoneType'
                ], 'NA')
            
            filename = f"transformed_{table.replace('transformed_', '')}.csv"
            filepath = self.exports_dir / filename
            df.to_csv(filepath, index=False)
            files.append(filename)
            print(f"  {filename}: {len(df)} records")
        
        return files

    # ... (print_summary and run_transformation are unchanged) ...
    def print_summary(self):
        """Print summary"""
        print("\n" + "="*60)
        print("TRANSFORMATION SUMMARY")
        print("="*60)
        
        for table in ['branches', 'customers', 'loans', 'transactions']:
            s = self.stats[table]
            q = self.quality.get(table, {})
            
            print(f"\n{table.upper()}:")
            print(f"  Processed: {s['processed']}, Transformed: {s['transformed']}")
            print(f"  Duplicates: {s['duplicates']}, NULLs replaced: {s['nulls']}")
            if 'outliers' in s:
                print(f"  Outliers: {s['outliers']}")
            if q:
                print(f"  Quality: {q['completeness']:.1f}% complete, {q['accuracy']:.1f}% accurate")
    
    def run_transformation(self):
        """Run pipeline"""
        try:
            print("\n" + "="*60)
            print("STARTING TRANSFORMATION")
            print("="*60)
            
            self.connect_databases()
            self.create_transformed_tables()
            
            start_time = time.time()
            
            self.transform_branches()
            self.transform_customers()
            self.transform_loans()
            self.transform_transactions()
            
            files = self.export_csv()
            self.print_summary()
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            print(f"\nExported {len(files)} files to {self.exports_dir}/")
            print(f"Total execution time: {execution_time:.2f} seconds")
            print("TRANSFORMATION COMPLETED\n")
            
            return {'stats': self.stats, 'quality': self.quality, 'files': files, 'execution_time': execution_time}
            
        except Exception as e:
            print(f"\nTRANSFORMATION FAILED: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            if self.staging_connection:
                self.staging_connection.close()
            if self.transformed_connection:
                self.transformed_connection.close()

# ... (main function is unchanged) ...
def main():
    import sys
    import logging
    
    config = None
    
    try:
        from config.config import config as cfg
        config = cfg
        print("Config loaded from config.config")
    except ImportError: pass
    
    if config is None:
        try:
            import sys, os
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from config import config as cfg
            config = cfg
            print("Config loaded from config")
        except ImportError: pass
    
    if config is None:
        try:
            sys.path.insert(0, os.getcwd())
            from config.config import config as cfg
            config = cfg
            print("Config loaded from current directory")
        except ImportError: pass
    
    if config is None:
        print("Using fallback config - update with your database credentials")
        config = {
            'MYSQL_HOST': '127.0.0.1',
            'MYSQL_USER': 'root',
            'MYSQL_PASSWORD': 'Mysql@1234',
            'MYSQL_DATABASE': 'stagging',
            'MYSQL_PORT': 3306
        }
    
    required_keys = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE', 'MYSQL_PORT']
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        print(f"Missing required config keys: {missing_keys}")
        print("Please check your config.py file")
        sys.exit(1)
    
    print(f"Database: {config['MYSQL_USER']}@{config['MYSQL_HOST']}:{config['MYSQL_PORT']}")
    print(f"Staging DB: {config['MYSQL_DATABASE']}")
    print(f"Transform DB: transformed")
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    try:
        transformer = DataTransformer(config)
        transformer.run_transformation()
    except Exception as e:
        print(f"\nTransformation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()