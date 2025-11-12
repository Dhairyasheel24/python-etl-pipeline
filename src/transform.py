import pandas as pd
import mysql.connector
from datetime import date, datetime, timedelta
import re
import logging
import sys
import os
from pathlib import Path
import time  # Added time for execution tracking

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataTransformer:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.staging_connection = None
        self.transformed_connection = None
        self.exports_dir = Path("exports")
        self.exports_dir.mkdir(exist_ok=True)
        
        # --- NEW: Batch processing configuration ---
        self.batch_size = 1000  # Process 1000 records at a time
        self.query_timeout = 300  # 5 minutes timeout
        
        # Simple stats tracking
        self.stats = {
            'branches': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0},
            'customers': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0},
            'loans': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0},
            'transactions': {'processed': 0, 'transformed': 0, 'duplicates': 0, 'nulls': 0, 'outliers': 0}
        }
        
        self.quality = {}
    
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
                # --- NEW: Added connection timeout ---
                connection_timeout=self.query_timeout
            )
            
            temp_conn = mysql.connector.connect(
                host=self.config['MYSQL_HOST'],
                user=self.config['MYSQL_USER'],
                password=self.config['MYSQL_PASSWORD'],
                port=self.config['MYSQL_PORT'],
                # --- NEW: Added connection timeout ---
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
                # --- NEW: Added connection timeout ---
                connection_timeout=self.query_timeout
            )
            
            print("Connected to databases")
            
        except mysql.connector.Error as e:
            self.logger.error(f"Connection error: {e}")
            raise
    
    def create_transformed_tables(self):
        """Create tables with sequential display IDs"""
        cursor = self.transformed_connection.cursor()
        
        tables = ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
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
    
    # --- NEW: Batch fetching helper function ---
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
                print(f"  Fetched batch: {len(batch)} records (total: {len(all_data)})")
                
            except mysql.connector.Error as e:
                print(f"  Error fetching batch for {table_name}: {e}")
                break
        
        return all_data

    # Utility functions (Unchanged)
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
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return 'NA' if return_string_na else None
        try:
            val_str = str(val).strip()
            if not val_str: return 'NA' if return_string_na else None
            date_formats_4digit = ['%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d.%m.%Y', '%m.%d.%Y', '%Y.%m.%d']
            for fmt in date_formats_4digit:
                try:
                    parsed = datetime.strptime(val_str, fmt).date()
                    current_year = date.today().year
                    if (current_year - 120) <= parsed.year <= (current_year + 50):
                        return parsed
                except ValueError: continue
            two_digit_match = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$', val_str)
            if two_digit_match:
                day, month, year = two_digit_match.groups()
                day, month, year_2digit = int(day), int(month), int(year)
                year_4digit = 2000 + year_2digit if year_2digit <= 49 else 1900 + year_2digit
                if 1 <= month <= 12 and 1 <= day <= 31:
                    try:
                        if month == 12: last_day = date(year_4digit + 1, 1, 1) - timedelta(days=1)
                        else: last_day = date(year_4digit, month + 1, 1) - timedelta(days=1)
                        adjusted_day = min(day, last_day.day)
                        parsed_date = date(year_4digit, month, adjusted_day)
                        current_year = date.today().year
                        if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                            return parsed_date
                    except ValueError:
                        try:
                            parsed_date = date(year_4digit, month, 1)
                            current_year = date.today().year
                            if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                                return parsed_date
                        except ValueError: pass
            date_formats_2digit = ['%d-%m-%y', '%m-%d-%y', '%y-%m-%d', '%d/%m/%y', '%m/%d/%y', '%y/%m/%d']
            for fmt in date_formats_2digit:
                try:
                    parsed = datetime.strptime(val_str, fmt).date()
                    current_year = date.today().year
                    if (current_year - 120) <= parsed.year <= (current_year + 50):
                        return parsed
                except ValueError: continue
            try:
                parsed = pd.to_datetime(val_str, errors='coerce', dayfirst=True)
                if pd.notna(parsed):
                    parsed_date = parsed.date()
                    current_year = date.today().year
                    if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                        return parsed_date
            except Exception: pass
            return 'NA' if return_string_na else None
        except Exception as e:
            return 'NA' if return_string_na else None

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
    
    def detect_outliers(self, series):
        if len(series) < 4: return set()
        Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
        IQR = Q3 - Q1
        if IQR == 0: return set()
        return set(series[(series < Q1-1.5*IQR) | (series > Q3+1.5*IQR)].index)
    
    def calc_age(self, dob):
        if not dob or dob == 'NA' or dob > date.today(): return 0
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age if 0 <= age <= 120 else 0
    
    def clean_email(self, email):
        return self.safe_val(email, 'NA', lower=True)
    
    def clean_phone(self, phone):
        return self.safe_val(phone, 'NA')
    
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

    # --- MODIFIED: Uses batch fetching AND includes the fix ---
    def transform_branches(self):
        """Transform branches"""
        print("\nTRANSFORMING BRANCHES")
        cursor = self.staging_connection.cursor()
        
        # Get column names first, and clear the buffer
        try:
            cursor.execute("SELECT * FROM staging_branches LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall() # <-- FIX IS HERE
        except mysql.connector.Error as e:
            print(f"  Error getting columns for staging_branches: {e}")
            cursor.close()
            return

        # Get total count
        try:
            cursor.execute("SELECT COUNT(*) FROM staging_branches")
            total_count = cursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error counting staging_branches: {e}")
            cursor.close()
            return
        
        # --- NEW: Fetch data in batches ---
        # Assuming 'branch_id' is a good key to order by
        all_data = self.fetch_data_in_batches(cursor, "staging_branches", "branch_id")
        cursor.close() # Close the staging cursor
        
        if not all_data:
            print("  No data found in staging_branches")
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        
        self.stats['branches']['processed'] = len(df)
        
        # Remove duplicates
        initial = len(df)
        df_clean = df.drop_duplicates(subset=['branch_id'], keep='first')
        duplicates_removed = initial - len(df_clean)
        self.stats['branches']['duplicates'] = duplicates_removed
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate branches")
        
        tcursor = self.transformed_connection.cursor()
        nulls = 0
        
        for _, row in df_clean.iterrows():
            branch_id = self.safe_val(row.get('branch_id'), 'NA')
            branch_name = self.safe_val(row.get('branch_name'), 'NA', title=True)
            city = self.safe_val(row.get('city'), 'NA', title=True)
            state = self.safe_val(row.get('state'), 'NA', upper=True)
            manager = self.safe_val(row.get('manager_name'), 'NA', title=True)
            
            if any(field == 'NA' for field in [branch_name, city, state, manager]):
                nulls += 1
            
            region = 'NA'
            if state != 'NA':
                sl = state.lower()
                if any(x in sl for x in ['delhi','punjab','haryana','up','uttar']): region = 'North'
                elif any(x in sl for x in ['maharashtra','gujarat','goa']): region = 'West'
                elif any(x in sl for x in ['karnataka','tamil','kerala','andhra','telangana']): region = 'South'
                elif any(x in sl for x in ['bengal','bihar','odisha','assam','meghalaya','chhattisgarh','arunachal','manipur']): region = 'East'
            
            try:
                tcursor.execute("""
                    INSERT INTO transformed_branches 
                    (branch_id, branch_name, city, state, manager_name, region)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (branch_id, branch_name, city, state, manager, region))
                self.stats['branches']['transformed'] += 1
            except mysql.connector.Error as e:
                if "Duplicate entry" in str(e): continue
                else: raise
        
        self.transformed_connection.commit()
        self.stats['branches']['nulls'] = nulls
        
        tcursor.execute("SELECT * FROM transformed_branches")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'branches', ['branch_id', 'branch_name'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['branches']['transformed']}")
        print(f"  Duplicates: {self.stats['branches']['duplicates']}, NULLs: {nulls}")

    # --- MODIFIED: Uses batch fetching AND includes the fix ---
    def transform_customers(self):
        """Transform customers"""
        print("\nTRANSFORMING CUSTOMERS")
        cursor = self.staging_connection.cursor()

        # Get column names first, and clear the buffer
        try:
            cursor.execute("SELECT * FROM staging_customers LIMIT 1")
            columns = [c[0] for c in cursor.description]
            cursor.fetchall() # <-- FIX IS HERE
        except mysql.connector.Error as e:
            print(f"  Error getting columns for staging_customers: {e}")
            cursor.close()
            return

        # Get total count
        try:
            cursor.execute("SELECT COUNT(*) FROM staging_customers")
            total_count = cursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error counting staging_customers: {e}")
            cursor.close()
            return

        # --- NEW: Fetch data in batches ---
        # Assuming 'customer_id' is a good key to order by
        all_data = self.fetch_data_in_batches(cursor, "staging_customers", "customer_id")
        cursor.close() # Close the staging cursor
        
        if not all_data:
            print("  No data found in staging_customers")
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        
        self.stats['customers']['processed'] = len(df)
        
        # Remove duplicates
        initial = len(df)
        df_clean = df.drop_duplicates(subset=['customer_id'], keep='first')
        duplicates_removed = initial - len(df_clean)
        self.stats['customers']['duplicates'] = duplicates_removed
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate customers")
        
        tcursor = self.transformed_connection.cursor()
        nulls = 0
        date_warnings = 0
        max_warnings = 5
        
        for idx, row in df_clean.iterrows():
            cid = self.safe_val(row.get('customer_id'), 'NA')
            fname = self.safe_val(row.get('first_name'), 'NA', title=True)
            lname = self.safe_val(row.get('last_name'), 'NA', title=True)
            branch_id = self.safe_val(row.get('branch_id'), 'NA')
            
            raw_dob = row.get('dob')
            dob_result = self.safe_date(raw_dob, return_string_na=True)
            
            if dob_result == 'NA':
                dob_sql = None
                nulls += 1
                if (raw_dob and 
                    str(raw_dob).strip().lower() not in ['nan', 'none', '', 'n/a', 'null'] and
                    date_warnings < max_warnings):
                    date_warnings += 1
                    print(f"  WARNING: Invalid date '{raw_dob}' for customer {cid}")
            else:
                dob_sql = dob_result
            
            raw_acc_date = row.get('account_open_date')
            acc_date_result = self.safe_date(raw_acc_date, return_string_na=True)
            if acc_date_result == 'NA':
                acc_date_sql = None
                nulls += 1
            else:
                acc_date_sql = acc_date_result
            
            age = self.calc_age(dob_result) if dob_result != 'NA' else 0
            tenure = max(0, (date.today() - acc_date_result).days) if acc_date_result != 'NA' else 0
            
            if tenure >= 730: segment = 'VIP'
            elif tenure >= 180: segment = 'Regular'
            elif tenure > 0: segment = 'New'
            else: segment = 'NA'
            
            email = self.clean_email(row.get('email'))
            phone = self.clean_phone(row.get('phone'))
            
            gender = self.safe_val(row.get('gender'), 'NA', title=True)
            if gender.lower() in ['m', 'male']: gender = 'Male'
            elif gender.lower() in ['f', 'female']: gender = 'Female'
            else: gender = 'NA'
            
            address = self.safe_val(row.get('address'), 'NA', title=True)
            
            if any(field == 'NA' for field in [email, phone, gender]):
                nulls += 1
            
            try:
                tcursor.execute("""
                    INSERT INTO transformed_customers 
                    (customer_id, branch_id, first_name, last_name, dob, age, gender, 
                     email, phone, address, account_open_date, customer_tenure_days, 
                     customer_segment, outlier_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (cid, branch_id, fname, lname, dob_sql, age, gender,
                      email, phone, address, acc_date_sql, tenure, segment, False))
                self.stats['customers']['transformed'] += 1
            except mysql.connector.Error as e:
                if "Duplicate entry" in str(e): continue
                else: raise
        
        self.transformed_connection.commit()
        self.stats['customers']['nulls'] = nulls
        
        tcursor.execute("SELECT * FROM transformed_customers")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'customers', ['customer_id', 'first_name'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['customers']['transformed']}")
        print(f"  Duplicates: {self.stats['customers']['duplicates']}, NULLs: {nulls}")
        if date_warnings >= max_warnings:
            print(f"  Additional date warnings suppressed")

    # --- MODIFIED: Uses batch fetching AND includes the fix ---
    def transform_loans(self):
        """Transform loans"""
        print("\nTRANSFORMING LOANS")
        
        tcursor = self.transformed_connection.cursor()
        tcursor.execute("SELECT customer_id FROM transformed_customers")
        valid_custs = set(r[0] for r in tcursor.fetchall())
        
        scursor = self.staging_connection.cursor()
        
        # Get column names first, and clear the buffer
        try:
            scursor.execute("SELECT * FROM staging_loans LIMIT 1")
            columns = [c[0] for c in scursor.description]
            scursor.fetchall() # <-- FIX IS HERE
        except mysql.connector.Error as e:
            print(f"  Error getting columns for staging_loans: {e}")
            scursor.close()
            tcursor.close()
            return

        # Get total count
        try:
            scursor.execute("SELECT COUNT(*) FROM staging_loans")
            total_count = scursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error counting staging_loans: {e}")
            scursor.close()
            tcursor.close()
            return

        # --- NEW: Fetch data in batches ---
        # Assuming 'loan_id' is a good key to order by
        all_data = self.fetch_data_in_batches(scursor, "staging_loans", "loan_id")
        scursor.close() # Close the staging cursor
        
        if not all_data:
            print("  No data found in staging_loans")
            tcursor.close()
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        
        self.stats['loans']['processed'] = len(df)
        
        # Remove duplicates
        initial = len(df)
        df_clean = df.drop_duplicates(subset=['loan_id'], keep='first')
        duplicates_removed = initial - len(df_clean)
        self.stats['loans']['duplicates'] = duplicates_removed
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate loans")
        
        nulls = 0
        
        for idx, row in df_clean.iterrows():
            cid = self.safe_val(row.get('customer_id'), 'NA')
            if cid not in valid_custs: continue
            
            amt = self.safe_num(row.get('loan_amount'), 0)
            if amt <= 0: continue
            
            rate = self.safe_num(row.get('interest_rate'), 0)
            
            start_result = self.safe_date(row.get('start_date'), return_string_na=True)
            if start_result == 'NA':
                start_sql = None
                continue
            else:
                start_sql = start_result
            
            end_result = self.safe_date(row.get('end_date'), return_string_na=True)
            if end_result == 'NA':
                end_sql = None
                nulls += 1
            else:
                end_sql = end_result
            
            duration = 0
            if end_sql and end_sql >= start_sql:
                duration = ((end_sql.year - start_sql.year) * 12 + (end_sql.month - start_sql.month))
            
            if amt > 500000: risk = 'High'
            elif amt > 100000: risk = 'Medium'
            else: risk = 'Low'
            
            loan_id = self.safe_val(row.get('loan_id'), 'NA')
            loan_type = self.safe_val(row.get('loan_type'), 'NA', title=True)
            loan_status = self.safe_val(row.get('loan_status'), 'NA', title=True)
            
            try:
                tcursor.execute("""
                    INSERT INTO transformed_loans 
                    (loan_id, customer_id, loan_type, loan_amount, interest_rate, 
                     start_date, end_date, loan_status, loan_duration_months, 
                     risk_category, outlier_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (loan_id, cid, loan_type, amt, rate, start_sql, end_sql, 
                      loan_status, duration, risk, False))
                self.stats['loans']['transformed'] += 1
            except mysql.connector.Error as e:
                if "Duplicate entry" in str(e): continue
                else: raise
        
        self.transformed_connection.commit()
        self.stats['loans']['nulls'] = nulls
        
        tcursor.execute("SELECT * FROM transformed_loans")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'loans', ['loan_id', 'customer_id'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['loans']['transformed']}")
        print(f"  Duplicates: {self.stats['loans']['duplicates']}, NULLs: {nulls}")

    # --- MODIFIED: Uses batch fetching AND includes the fix ---
    def transform_transactions(self):
        """Transform transactions"""
        print("\nTRANSFORMING TRANSACTIONS")
        
        tcursor = self.transformed_connection.cursor()
        tcursor.execute("SELECT customer_id FROM transformed_customers")
        valid_custs = set(r[0] for r in tcursor.fetchall())
        
        scursor = self.staging_connection.cursor()
        
        # Get column names first, and clear the buffer
        try:
            scursor.execute("SELECT * FROM staging_transactions LIMIT 1")
            columns = [c[0] for c in scursor.description]
            scursor.fetchall() # <-- FIX IS HERE
        except mysql.connector.Error as e:
            print(f"  Error getting columns for staging_transactions: {e}")
            scursor.close()
            tcursor.close()
            return

        # Get total count
        try:
            scursor.execute("SELECT COUNT(*) FROM staging_transactions")
            total_count = scursor.fetchone()[0]
            print(f"  Total records: {total_count}")
        except mysql.connector.Error as e:
            print(f"  Error counting staging_transactions: {e}")
            scursor.close()
            tcursor.close()
            return

        # --- NEW: Fetch data in batches ---
        # This is the one that likely timed out before
        # Assuming 'transaction_id' is a good key to order by
        all_data = self.fetch_data_in_batches(scursor, "staging_transactions", "transaction_id")
        scursor.close() # Close the staging cursor
        
        if not all_data:
            print("  No data found in staging_transactions")
            tcursor.close()
            return
            
        df = pd.DataFrame(all_data, columns=columns)
        
        self.stats['transactions']['processed'] = len(df)
        
        # Remove duplicates
        initial = len(df)
        df_clean = df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial - len(df_clean)
        self.stats['transactions']['duplicates'] = duplicates_removed
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate transactions")
        
        nulls = 0
        
        for idx, row in df_clean.iterrows():
            cid = self.safe_val(row.get('customer_id'), 'NA')
            if cid not in valid_custs: continue
            
            amt = self.safe_num(row.get('amount'), 0)
            if amt <= 0: continue
            
            tdate_result = self.safe_date(row.get('transaction_date'), return_string_na=True)
            if tdate_result == 'NA':
                tdate_sql = None
                continue
            else:
                tdate_sql = tdate_result
            
            bal = self.safe_num(row.get('balance_after'), 0)
            if bal == 0: nulls += 1
            
            fraud_flag = str(row.get('fraud_flag', 'false')).lower()
            fraud = fraud_flag in ['true', '1', 'yes', 'y']
            
            if amt > 10000: cat = 'Large'
            elif amt > 1000: cat = 'Medium'
            else: cat = 'Small'
            
            transaction_id = self.safe_val(row.get('transaction_id'), 'NA')
            transaction_type = self.safe_val(row.get('transaction_type'), 'NA', upper=True)
            
            try:
                tcursor.execute("""
                    INSERT INTO transformed_transactions 
                    (transaction_id, customer_id, transaction_date, transaction_type, 
                     amount, balance_after, fraud_flag, transaction_category, outlier_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (transaction_id, cid, tdate_sql, transaction_type,
                      amt, bal, fraud, cat, False))
                self.stats['transactions']['transformed'] += 1
            except mysql.connector.Error as e:
                if "Duplicate entry" in str(e): continue
                else: raise
        
        self.transformed_connection.commit()
        self.stats['transactions']['nulls'] = nulls
        
        tcursor.execute("SELECT * FROM transformed_transactions")
        result_df = pd.DataFrame(tcursor.fetchall(), columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'transactions', ['transaction_id', 'customer_id'])
        tcursor.close()
        
        print(f"  Transformed: {self.stats['transactions']['transformed']}")
        print(f"  Duplicates: {self.stats['transactions']['duplicates']}, NULLs: {nulls}")

    # --- MODIFIED: Uses batch fetching AND includes the fix ---
    def export_csv(self):
        """Export to CSV"""
        print("\nEXPORTING CSV")
        files = []
        
        for table in ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']:
            print(f"  Exporting {table}...")
            cursor = self.transformed_connection.cursor()
            
            # Get column names first, and clear the buffer
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                columns = [c[0] for c in cursor.description]
                cursor.fetchall() # <-- FIX IS HERE
            except mysql.connector.Error as e:
                print(f"  Error getting columns for {table}: {e}")
                cursor.close()
                continue

            # --- NEW: Fetch data in batches ---
            # Use 'display_id' as the key to order by for transformed tables
            all_data = self.fetch_data_in_batches(cursor, table, "display_id")
            cursor.close()
            
            df = pd.DataFrame(all_data, columns=columns)
            
            # Convert ALL columns to string and replace NULL values
            for col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].apply(lambda x: 'NA' if x in [
                    'None', 'NaN', 'nan', 'NaT', '<NA>', 'NULL', 'null', '', 'NoneType'
                ] else x)
                df[col] = df[col].apply(lambda x: 'NA' if 'NaT' in str(x) else x)
                df[col] = df[col].apply(lambda x: 'NA' if x == 'None' else x)
            
            filename = f"transformed_{table.replace('transformed_', '')}.csv"
            filepath = self.exports_dir / filename
            df.to_csv(filepath, index=False)
            files.append(filename)
            print(f"  {filename}: {len(df)} records")
        
        return files

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
            
            start_time = time.time()  # --- NEW: Track execution time ---
            
            self.transform_branches()
            self.transform_customers()
            self.transform_loans()
            self.transform_transactions()
            
            files = self.export_csv()
            self.print_summary()
            
            end_time = time.time()  # --- NEW: Track execution time ---
            execution_time = end_time - start_time
            
            print(f"\nExported {len(files)} files to {self.exports_dir}/")
            print(f"Total execution time: {execution_time:.2f} seconds") # --- NEW ---
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