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
            'customers': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0},
            'loans': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0},
            'transactions': {'processed': 0, 'transformed': 0, 'skipped': 0, 'duplicates': 0, 'nulls': 0}
        }
    
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
        """Create tables - REMOVED outlier_flag and optimized storage"""
        self.transformed_connection.ping(reconnect=True)
        cursor = self.transformed_connection.cursor()
        
        # Branches
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_branches (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                branch_id VARCHAR(20) UNIQUE,
                branch_name VARCHAR(100) DEFAULT 'NA',
                city VARCHAR(50) DEFAULT 'NA',
                state VARCHAR(50) DEFAULT 'NA',
                manager_name VARCHAR(100) DEFAULT 'NA',
                region VARCHAR(20) DEFAULT 'NA'
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        # Customers - Reduced Gender to CHAR(1) and removed outlier_flag
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_customers (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id VARCHAR(20) UNIQUE,
                branch_id VARCHAR(20) DEFAULT 'NA',
                first_name VARCHAR(50) DEFAULT 'NA',
                last_name VARCHAR(50) DEFAULT 'NA',
                dob DATE,
                age INT DEFAULT 0,
                gender CHAR(1) DEFAULT 'N',
                email VARCHAR(100) DEFAULT 'NA',
                phone VARCHAR(20) DEFAULT 'NA',
                address TEXT,
                account_open_date DATE,
                customer_tenure_days INT DEFAULT 0,
                customer_segment VARCHAR(20) DEFAULT 'NA'
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        # Loans - Removed outlier_flag
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_loans (
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
                risk_category VARCHAR(20) DEFAULT 'NA'
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        # Transactions - Removed outlier_flag
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_transactions (
                display_id INT AUTO_INCREMENT PRIMARY KEY,
                transaction_id VARCHAR(20) UNIQUE,
                customer_id VARCHAR(20) DEFAULT 'NA',
                transaction_date DATE,
                transaction_type VARCHAR(50) DEFAULT 'NA',
                amount DECIMAL(15,2),
                balance_after DECIMAL(15,2) DEFAULT 0,
                fraud_flag BOOLEAN DEFAULT FALSE,
                transaction_category VARCHAR(20) DEFAULT 'NA'
            ) ENGINE=InnoDB AUTO_INCREMENT=1
        """)
        
        self.transformed_connection.commit()
        cursor.close()

    def fetch_data_in_batches(self, cursor, table_name, primary_key):
        offset = 0
        all_data = []
        while True:
            try:
                query = f"SELECT * FROM {table_name} ORDER BY {primary_key} LIMIT {self.batch_size} OFFSET {offset}"
                cursor.execute(query)
                batch = cursor.fetchall()
                if not batch: break
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
        if pd.isna(val) or str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a']:
            return pd.NaT if not return_string_na else 'NA'
        try:
            val_str = str(val).strip()
            if re.match(r'^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$', val_str) or re.match(r'^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$', val_str):
                try:
                    parsed = pd.to_datetime(val_str, errors='coerce').date()
                    if pd.notna(parsed) and 1900 <= parsed.year <= date.today().year: return parsed
                except Exception: pass
            two_digit_match = re.match(r'^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2})$', val_str)
            if two_digit_match:
                day, month, year_2digit = map(int, two_digit_match.groups())
                if day > 12: pass
                elif month > 12: day, month = month, day
                current_year_2digit = date.today().year % 100
                if year_2digit > current_year_2digit: year_4digit = 1900 + year_2digit
                else: year_4digit = 2000 + year_2digit
                try:
                    parsed = date(year_4digit, month, day)
                    if 1900 <= parsed.year <= date.today().year: return parsed
                except ValueError: pass 
            try:
                parsed = pd.to_datetime(val_str, errors='coerce', dayfirst=True).date()
                if pd.notna(parsed) and 1900 <= parsed.year <= date.today().year: return parsed
            except Exception: pass
            return pd.NaT if not return_string_na else 'NA'
        except Exception: return pd.NaT if not return_string_na else 'NA'

    def safe_num(self, val, default=0):
        try: 
            cleaned = str(val).replace('₹','').replace('$','').replace(',','').replace(' ','').strip()
            return float(cleaned) if cleaned else default
        except: return default
    
    def calc_age(self, dob):
        if pd.isna(dob) or dob > date.today(): return 0
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

    # --- INCREMENTAL TRANSFORMATION FUNCTIONS ---

    def transform_branches(self):
        print("Transforming Branches (Incremental)...")
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
        
        df_clean = df.drop_duplicates(subset=['branch_id'], keep='first').copy()
        
        df_clean['branch_name'] = df_clean['branch_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['city'] = df_clean['city'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['state'] = df_clean['state'].apply(lambda x: self.safe_val(x, 'NA', upper=True))
        df_clean['manager_name'] = df_clean['manager_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        north, west, south, east = ['DELHI','PUNJAB','UP'], ['MAHARASHTRA','GUJARAT'], ['KARNATAKA','TAMIL'], ['BENGAL','BIHAR']
        def map_region(state):
            if any(x in state for x in north): return 'North'
            if any(x in state for x in west): return 'West'
            if any(x in state for x in south): return 'South'
            if any(x in state for x in east): return 'East'
            return 'NA'
        df_clean['region'] = df_clean['state'].apply(map_region)

        tcursor = self.transformed_connection.cursor()
        cols = ['branch_id', 'branch_name', 'city', 'state', 'manager_name', 'region']
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        if batch_data:
            tcursor.executemany("""
                INSERT INTO transformed_branches (branch_id, branch_name, city, state, manager_name, region)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    branch_name = VALUES(branch_name),
                    city = VALUES(city),
                    state = VALUES(state),
                    manager_name = VALUES(manager_name),
                    region = VALUES(region)
            """, batch_data)
            self.transformed_connection.commit()
            self.stats['branches']['transformed'] = len(batch_data)
            print(f"  Successfully upserted {len(batch_data)} branches.")
        tcursor.close()

    def transform_customers(self):
        print("Transforming Customers (Incremental)...")
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

        df_clean = df.drop_duplicates(subset=['customer_id'], keep='first').copy()
        
        df_clean['first_name'] = df_clean['first_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['last_name'] = df_clean['last_name'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['branch_id'] = df_clean['branch_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['dob'] = df_clean['dob'].apply(lambda x: self.safe_date(x, return_string_na=False))
        df_clean['account_open_date'] = df_clean['account_open_date'].apply(lambda x: self.safe_date(x, return_string_na=False))
        df_clean['age'] = df_clean['dob'].apply(self.calc_age)
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            today = pd.to_datetime(date.today())
            df_clean['customer_tenure_days'] = (today - pd.to_datetime(df_clean['account_open_date'])).dt.days.fillna(0).astype(int)
            df_clean.loc[df_clean['customer_tenure_days'] < 0, 'customer_tenure_days'] = 0
        
        conditions = [(df_clean['customer_tenure_days'] >= 730), (df_clean['customer_tenure_days'] >= 180), (df_clean['customer_tenure_days'] > 0)]
        df_clean['customer_segment'] = np.select(conditions, ['VIP', 'Regular', 'New'], default='NA')
        
        df_clean['email'] = df_clean['email'].apply(lambda x: self.safe_val(x, 'NA', lower=True))
        df_clean['phone'] = df_clean['phone'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['address'] = df_clean['address'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        
        # --- FIXED GENDER LOGIC: Mapped to 'M' or 'F' ---
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gender_map = {'m': 'M', 'f': 'F', 'male': 'M', 'female': 'F'}
            df_clean['gender'] = df_clean['gender'].apply(lambda x: self.safe_val(x, 'N')).str.lower().map(gender_map).fillna('N')
        
        # --- REMOVED OUTLIER COLUMN LOGIC ---
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)

        tcursor = self.transformed_connection.cursor()
        cols = ['customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 'age', 'gender', 'email', 'phone', 'address', 'account_open_date', 'customer_tenure_days', 'customer_segment']
        
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        
        for i in range(0, len(batch_data), self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            tcursor.executemany("""
                INSERT INTO transformed_customers 
                (customer_id, branch_id, first_name, last_name, dob, age, gender, email, phone, address, account_open_date, customer_tenure_days, customer_segment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    branch_id = VALUES(branch_id),
                    first_name = VALUES(first_name),
                    last_name = VALUES(last_name),
                    dob = VALUES(dob),
                    age = VALUES(age),
                    gender = VALUES(gender),
                    email = VALUES(email),
                    phone = VALUES(phone),
                    address = VALUES(address),
                    account_open_date = VALUES(account_open_date),
                    customer_tenure_days = VALUES(customer_tenure_days),
                    customer_segment = VALUES(customer_segment)
            """, batch)
        
        self.transformed_connection.commit()
        self.stats['customers']['transformed'] = len(batch_data)
        print(f"  Successfully upserted {len(batch_data)} customers.")
        tcursor.close()

    def transform_loans(self):
        print("Transforming Loans (Incremental)...")
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

        if not all_data: return

        df = pd.DataFrame(all_data, columns=columns)
        self.stats['loans']['processed'] = len(df)
        
        df_clean = df.drop_duplicates(subset=['loan_id'], keep='first').copy()
        
        df_clean['customer_id'] = df_clean['customer_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['loan_amount'] = df_clean['loan_amount'].apply(lambda x: self.safe_num(x, 0))
        df_clean['loan_type'] = df_clean['loan_type'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['loan_status'] = df_clean['loan_status'].apply(lambda x: self.safe_val(x, 'NA', title=True))
        df_clean['interest_rate'] = df_clean['interest_rate'].apply(lambda x: self.safe_num(x, 0))
        df_clean['start_date'] = df_clean['start_date'].apply(lambda x: self.safe_date(x))
        df_clean['end_date'] = df_clean['end_date'].apply(lambda x: self.safe_date(x))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_clean['loan_duration_months'] = (pd.to_datetime(df_clean['end_date']).dt.to_period('M') - pd.to_datetime(df_clean['start_date']).dt.to_period('M')).apply(lambda x: x.n if pd.notna(x) else 0)
            df_clean.loc[df_clean['loan_duration_months'] < 0, 'loan_duration_months'] = 0

        conditions = [(df_clean['loan_amount'] > 500000), (df_clean['loan_amount'] > 100000)]
        df_clean['risk_category'] = np.select(conditions, ['High', 'Medium'], default='Low')
        
        # --- REMOVED OUTLIER LOGIC ---
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)

        tcursor = self.transformed_connection.cursor()
        cols = ['loan_id', 'customer_id', 'loan_type', 'loan_amount', 'interest_rate', 'start_date', 'end_date', 'loan_status', 'loan_duration_months', 'risk_category']
        
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        for i in range(0, len(batch_data), self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            tcursor.executemany("""
                INSERT INTO transformed_loans 
                (loan_id, customer_id, loan_type, loan_amount, interest_rate, start_date, end_date, loan_status, loan_duration_months, risk_category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    customer_id = VALUES(customer_id),
                    loan_type = VALUES(loan_type),
                    loan_amount = VALUES(loan_amount),
                    interest_rate = VALUES(interest_rate),
                    start_date = VALUES(start_date),
                    end_date = VALUES(end_date),
                    loan_status = VALUES(loan_status),
                    loan_duration_months = VALUES(loan_duration_months),
                    risk_category = VALUES(risk_category)
            """, batch)
        
        self.transformed_connection.commit()
        self.stats['loans']['transformed'] = len(batch_data)
        print(f"  Successfully upserted {len(batch_data)} new loans.")
        tcursor.close()

    def transform_transactions(self):
        print("Transforming Transactions (Incremental)...")
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

        if not all_data: return

        df = pd.DataFrame(all_data, columns=columns)
        self.stats['transactions']['processed'] = len(df)
        
        df_clean = df.drop_duplicates(subset=['transaction_id'], keep='first').copy()
          
        df_clean['customer_id'] = df_clean['customer_id'].apply(lambda x: self.safe_val(x, 'NA'))
        df_clean['amount'] = df_clean['amount'].apply(lambda x: self.safe_num(x, 0))
        df_clean['transaction_date'] = df_clean['transaction_date'].apply(lambda x: self.safe_date(x))
        df_clean['transaction_type'] = df_clean['transaction_type'].apply(lambda x: self.safe_val(x, 'NA', upper=True))
        df_clean['balance_after'] = df_clean['balance_after'].apply(lambda x: self.safe_num(x, 0))
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fraud_map = {'true': True, '1': True, 'yes': True, 'y': True}
            df_clean['fraud_flag'] = df_clean['fraud_flag'].astype(str).str.lower().map(fraud_map).fillna(False).astype(bool)
        
        conditions = [(df_clean['amount'] > 10000), (df_clean['amount'] > 1000)]
        df_clean['transaction_category'] = np.select(conditions, ['Large', 'Medium'], default='Small')
        
        # --- REMOVED OUTLIER LOGIC ---
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)

        tcursor = self.transformed_connection.cursor()
        cols = ['transaction_id', 'customer_id', 'transaction_date', 'transaction_type', 'amount', 'balance_after', 'fraud_flag', 'transaction_category']
        
        batch_data = [tuple(x) for x in df_clean[cols].to_numpy()]
        for i in range(0, len(batch_data), self.batch_size):
            batch = batch_data[i:i + self.batch_size]
            tcursor.executemany("""
                INSERT INTO transformed_transactions 
                (transaction_id, customer_id, transaction_date, transaction_type, amount, balance_after, fraud_flag, transaction_category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    customer_id = VALUES(customer_id),
                    transaction_date = VALUES(transaction_date),
                    transaction_type = VALUES(transaction_type),
                    amount = VALUES(amount),
                    balance_after = VALUES(balance_after),
                    fraud_flag = VALUES(fraud_flag),
                    transaction_category = VALUES(transaction_category)
            """, batch)
        
        self.transformed_connection.commit()
        self.stats['transactions']['transformed'] = len(batch_data)
        print(f"  Successfully upserted {len(batch_data)} new transactions.")
        tcursor.close()

    def export_csv(self):
        print("\nExporting transformed data to CSV...")
        files = []
        for table in ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']:
            self.transformed_connection.ping(reconnect=True)
            cursor = self.transformed_connection.cursor()
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                columns = [c[0] for c in cursor.description]
                cursor.fetchall()
                all_data = self.fetch_data_in_batches(cursor, table, "display_id")
                if not all_data: continue

                df = pd.DataFrame(all_data, columns=columns)
                df = df.fillna('NA')
                
                filename = f"transformed_{table.replace('transformed_', '')}.csv"
                filepath = self.exports_dir / filename
                df.to_csv(filepath, index=False)
                files.append(filename)
            finally:
                cursor.close()
        return files

    def print_summary(self):
        print("\n" + "-"*40)
        print("TRANSFORMATION PHASE COMPLETE")
        print("-"*40)
        total_trans = 0
        for table, s in self.stats.items():
            print(f"{table.title()}: {s['transformed']} records processed (Upserted).")
            total_trans += s['transformed']
        print(f"Total records processed: {total_trans}")

    def run_transformation(self):
        try:
            print("\nStarting Transformation Phase...")
            self.connect_databases()
            self.create_transformed_tables()
            
            start_time = time.time()
            self.transform_branches()
            self.transform_customers()
            self.transform_loans()
            self.transform_transactions()
            
            files = self.export_csv()
            self.print_summary()
            
            return {'stats': self.stats, 'files': files}
            
        except Exception as e:
            print(f"Transformation Error: {e}")
            raise
        finally:
            if self.staging_connection: self.staging_connection.close()
            if self.transformed_connection: self.transformed_connection.close()

def main():
    # --- HARDCODED CONFIG FOR STAGGING DATABASE ---
    config = {
        'MYSQL_HOST': '127.0.0.1',
        'MYSQL_USER': 'root',
        'MYSQL_PASSWORD': 'Mysql@1234',
        'MYSQL_DATABASE': 'stagging',
        'MYSQL_PORT': 3306
    }
    transformer = DataTransformer(config)
    transformer.run_transformation()

if __name__ == "__main__":
    main()