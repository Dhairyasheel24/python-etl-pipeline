import pandas as pd
import mysql.connector
from datetime import date, datetime, timedelta
import re
import logging
import sys
import os
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataTransformer:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.staging_connection = None
        self.transformed_connection = None
        self.exports_dir = Path("exports")
        self.exports_dir.mkdir(exist_ok=True)
        
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
                autocommit=False
            )
            
            temp_conn = mysql.connector.connect(
                host=self.config['MYSQL_HOST'],
                user=self.config['MYSQL_USER'],
                password=self.config['MYSQL_PASSWORD'],
                port=self.config['MYSQL_PORT']
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
                autocommit=False
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
        
        # Branches - FIXED: All columns use 'NA' default
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
        
        # Customers - FIXED: All string columns use 'NA' default
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
        
        # Loans - FIXED: All string columns use 'NA' default
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
        
        # Transactions - FIXED: All string columns use 'NA' default
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
    
    # Utility functions
    def safe_val(self, val, default='NA', title=False, upper=False, lower=False):
        """Safe value conversion with COMPLETE NULL handling"""
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return default
        
        result = str(val).strip()
        if not result:
            return default
            
        if title: 
            result = result.title()
        if upper: 
            result = result.upper()
        if lower: 
            result = result.lower()
            
        return result

    def safe_date(self, val, return_string_na=False):
        """Safe date conversion - returns 'NA' as string if return_string_na=True, otherwise None"""
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return 'NA' if return_string_na else None
        
        try:
            val_str = str(val).strip()
            if not val_str:
                return 'NA' if return_string_na else None
            
            # FIXED: First try four-digit year formats before two-digit
            date_formats_4digit = [
                '%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d',  # DD-MM-YYYY, MM-DD-YYYY, YYYY-MM-DD
                '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d',  # Same with slashes
                '%d.%m.%Y', '%m.%d.%Y', '%Y.%m.%d'   # Same with dots
            ]
            
            # Try four-digit year formats first (most common)
            for fmt in date_formats_4digit:
                try:
                    parsed = datetime.strptime(val_str, fmt).date()
                    # Validate the date is reasonable
                    current_year = date.today().year
                    if (current_year - 120) <= parsed.year <= (current_year + 50):  # Extended future range to 50 years
                        return parsed
                except ValueError:
                    continue
            
            # Handle two-digit year dates like '13-04-58' (DD-MM-YY) ONLY if no 4-digit match
            two_digit_match = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$', val_str)
            if two_digit_match:
                day, month, year = two_digit_match.groups()
                day = int(day)
                month = int(month)
                year_2digit = int(year)
                
                # FIXED: Updated logic for two-digit year conversion
                # Years 00-49 → 2000-2049 (future dates like 2027)
                # Years 50-99 → 1950-1999 (past dates like 1958)
                if year_2digit <= 49:
                    year_4digit = 2000 + year_2digit
                else:
                    year_4digit = 1900 + year_2digit
                
                # Validate and create date
                if 1 <= month <= 12 and 1 <= day <= 31:
                    try:
                        # Check if date is valid (handles cases like Feb 30, April 31, etc.)
                        test_date = date(year_4digit, month, 1)
                        # Get last day of month
                        if month == 12:
                            last_day = date(year_4digit + 1, 1, 1) - timedelta(days=1)
                        else:
                            last_day = date(year_4digit, month + 1, 1) - timedelta(days=1)
                        
                        # Adjust day if it's beyond the last day of month
                        adjusted_day = min(day, last_day.day)
                        parsed_date = date(year_4digit, month, adjusted_day)
                        
                        # Validate reasonable date range
                        current_year = date.today().year
                        if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                            return parsed_date
                    except ValueError:
                        # If still invalid, try with first day of month
                        try:
                            parsed_date = date(year_4digit, month, 1)
                            current_year = date.today().year
                            if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                                return parsed_date
                        except ValueError:
                            pass
            
            # Two-digit year formats (only if 4-digit failed)
            date_formats_2digit = [
                '%d-%m-%y', '%m-%d-%y', '%y-%m-%d',
                '%d/%m/%y', '%m/%d/%y', '%y/%m/%d'
            ]
            
            for fmt in date_formats_2digit:
                try:
                    parsed = datetime.strptime(val_str, fmt).date()
                    current_year = date.today().year
                    if (current_year - 120) <= parsed.year <= (current_year + 50):
                        return parsed
                except ValueError:
                    continue
            
            # Final fallback: pandas parsing with dayfirst=True
            try:
                parsed = pd.to_datetime(val_str, errors='coerce', dayfirst=True)
                if pd.notna(parsed):
                    parsed_date = parsed.date()
                    current_year = date.today().year
                    if (current_year - 120) <= parsed_date.year <= (current_year + 50):
                        return parsed_date
            except Exception:
                pass
            
            return 'NA' if return_string_na else None
            
        except Exception as e:
            return 'NA' if return_string_na else None

    def safe_num(self, val, default=0):
        """Safe numeric conversion with complete NULL handling"""
        if (val is None or 
            pd.isna(val) or 
            str(val).strip() in ['', 'None', 'NaN', 'nan', 'NULL', 'null', 'N/A', 'n/a'] or
            str(val).strip().lower() in ['nan', 'none', '', 'n/a', 'null']):
            return default
        
        try: 
            # Remove currency symbols and commas
            cleaned = str(val).replace('₹','').replace('$','').replace(',','').replace(' ','').strip()
            if not cleaned:
                return default
            return float(cleaned)
        except: 
            return default
    
    def detect_outliers(self, series):
        """IQR outlier detection"""
        if len(series) < 4: 
            return set()
        Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
        IQR = Q3 - Q1
        if IQR == 0:  # No variation
            return set()
        return set(series[(series < Q1-1.5*IQR) | (series > Q3+1.5*IQR)].index)
    
    def calc_age(self, dob):
        """Calculate age"""
        if not dob or dob == 'NA' or dob > date.today(): 
            return 0
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age if 0 <= age <= 120 else 0
    
    def clean_email(self, email):
        """Clean email - KEEP ALL, just standardize"""
        return self.safe_val(email, 'NA', lower=True)
    
    def clean_phone(self, phone):
        """Clean phone - KEEP ALL phones, just standardize format"""
        return self.safe_val(phone, 'NA')
    
    def calc_quality(self, df, table, required_cols):
        """Calculate quality metrics"""
        total = len(df)
        if total == 0:
            completeness = 0
            accuracy = 0
            dup_rate = 0
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

    def transform_branches(self):
        """Transform branches"""
        print("\nTRANSFORMING BRANCHES")
        cursor = self.staging_connection.cursor()
        cursor.execute("SELECT * FROM staging_branches")
        df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])
        
        self.stats['branches']['processed'] = len(df)
        
        if df.empty:
            print("  No data found in staging_branches")
            return
        
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
            # Use safe_val for ALL fields to ensure 'NA' for nulls
            branch_id = self.safe_val(row.get('branch_id'), 'NA')
            branch_name = self.safe_val(row.get('branch_name'), 'NA', title=True)
            city = self.safe_val(row.get('city'), 'NA', title=True)
            state = self.safe_val(row.get('state'), 'NA', upper=True)
            manager = self.safe_val(row.get('manager_name'), 'NA', title=True)
            
            # Count nulls for critical fields
            if any(field == 'NA' for field in [branch_name, city, state, manager]):
                nulls += 1
            
            # Determine region
            region = 'NA'
            if state != 'NA':
                sl = state.lower()
                if any(x in sl for x in ['delhi','punjab','haryana','up','uttar']): 
                    region = 'North'
                elif any(x in sl for x in ['maharashtra','gujarat','goa']): 
                    region = 'West'
                elif any(x in sl for x in ['karnataka','tamil','kerala','andhra','telangana']): 
                    region = 'South'
                elif any(x in sl for x in ['bengal','bihar','odisha','assam','meghalaya','chhattisgarh','arunachal','manipur']): 
                    region = 'East'
            
            try:
                tcursor.execute("""
                    INSERT INTO transformed_branches 
                    (branch_id, branch_name, city, state, manager_name, region)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (branch_id, branch_name, city, state, manager, region))
                self.stats['branches']['transformed'] += 1
            except mysql.connector.Error as e:
                if "Duplicate entry" in str(e):
                    continue
                else:
                    raise
        
        self.transformed_connection.commit()
        self.stats['branches']['nulls'] = nulls
        
        # Calculate quality metrics
        tcursor.execute("SELECT * FROM transformed_branches")
        result_df = pd.DataFrame(tcursor.fetchall(), 
                               columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'branches', ['branch_id', 'branch_name'])
        
        print(f"  Transformed: {self.stats['branches']['transformed']}")
        print(f"  Duplicates: {self.stats['branches']['duplicates']}, NULLs: {nulls}")

    def transform_customers(self):
        """Transform customers - FIXED: Date columns use 'NA' string"""
        print("\nTRANSFORMING CUSTOMERS")
        cursor = self.staging_connection.cursor()
        cursor.execute("SELECT * FROM staging_customers")
        df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])
        
        self.stats['customers']['processed'] = len(df)
        
        if df.empty:
            print("  No data found in staging_customers")
            return
        
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
        max_warnings = 5  # Limit warnings
        
        for idx, row in df_clean.iterrows():
            # Use safe_val for ALL string fields
            cid = self.safe_val(row.get('customer_id'), 'NA')
            fname = self.safe_val(row.get('first_name'), 'NA', title=True)
            lname = self.safe_val(row.get('last_name'), 'NA', title=True)
            branch_id = self.safe_val(row.get('branch_id'), 'NA')
            
            # FIXED: Use return_string_na=True to get 'NA' string for dates
            raw_dob = row.get('dob')
            dob_result = self.safe_date(raw_dob, return_string_na=True)
            
            # Handle date conversion
            if dob_result == 'NA':
                dob_sql = None  # Use NULL in database for dates
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
            
            # Calculate derived fields
            age = self.calc_age(dob_result) if dob_result != 'NA' else 0
            tenure = max(0, (date.today() - acc_date_result).days) if acc_date_result != 'NA' else 0
            
            # Customer segmentation
            if tenure >= 730:
                segment = 'VIP'
            elif tenure >= 180:
                segment = 'Regular'
            elif tenure > 0:
                segment = 'New'
            else:
                segment = 'NA'
            
            # Clean contact information
            email = self.clean_email(row.get('email'))
            phone = self.clean_phone(row.get('phone'))
            
            # Gender standardization
            gender = self.safe_val(row.get('gender'), 'NA', title=True)
            if gender.lower() in ['m', 'male']: 
                gender = 'Male'
            elif gender.lower() in ['f', 'female']: 
                gender = 'Female'
            else: 
                gender = 'NA'
            
            address = self.safe_val(row.get('address'), 'NA', title=True)
            
            # Count nulls for critical fields
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
                if "Duplicate entry" in str(e):
                    continue
                else:
                    raise
        
        self.transformed_connection.commit()
        self.stats['customers']['nulls'] = nulls
        
        # Calculate quality metrics
        tcursor.execute("SELECT * FROM transformed_customers")
        result_df = pd.DataFrame(tcursor.fetchall(), 
                               columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'customers', ['customer_id', 'first_name'])
        
        print(f"  Transformed: {self.stats['customers']['transformed']}")
        print(f"  Duplicates: {self.stats['customers']['duplicates']}, NULLs: {nulls}")
        if date_warnings >= max_warnings:
            print(f"  Additional date warnings suppressed")

    def transform_loans(self):
        """Transform loans"""
        print("\nTRANSFORMING LOANS")
        
        # Get valid customers
        cursor = self.transformed_connection.cursor()
        cursor.execute("SELECT customer_id FROM transformed_customers")
        valid_custs = set(r[0] for r in cursor.fetchall())
        
        scursor = self.staging_connection.cursor()
        scursor.execute("SELECT * FROM staging_loans")
        df = pd.DataFrame(scursor.fetchall(), columns=[c[0] for c in scursor.description])
        
        self.stats['loans']['processed'] = len(df)
        
        if df.empty:
            print("  No data found in staging_loans")
            return
        
        # Remove duplicates
        initial = len(df)
        df_clean = df.drop_duplicates(subset=['loan_id'], keep='first')
        duplicates_removed = initial - len(df_clean)
        self.stats['loans']['duplicates'] = duplicates_removed
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate loans")
        
        tcursor = self.transformed_connection.cursor()
        nulls = 0
        
        for idx, row in df_clean.iterrows():
            cid = self.safe_val(row.get('customer_id'), 'NA')
            if cid not in valid_custs: 
                continue
            
            amt = self.safe_num(row.get('loan_amount'), 0)
            if amt <= 0: 
                continue
            
            rate = self.safe_num(row.get('interest_rate'), 0)
            
            # FIXED: Use return_string_na=True for dates
            start_result = self.safe_date(row.get('start_date'), return_string_na=True)
            if start_result == 'NA':
                start_sql = None
                continue  # Skip if start date is invalid
            else:
                start_sql = start_result
            
            end_result = self.safe_date(row.get('end_date'), return_string_na=True)
            if end_result == 'NA':
                end_sql = None
                nulls += 1
            else:
                end_sql = end_result
            
            # Calculate loan duration
            duration = 0
            if end_sql and end_sql >= start_sql:
                duration = ((end_sql.year - start_sql.year) * 12 + (end_sql.month - start_sql.month))
            
            # Risk categorization
            if amt > 500000:
                risk = 'High'
            elif amt > 100000:
                risk = 'Medium'
            else:
                risk = 'Low'
            
            # Use safe_val for all string fields
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
                if "Duplicate entry" in str(e):
                    continue
                else:
                    raise
        
        self.transformed_connection.commit()
        self.stats['loans']['nulls'] = nulls
        
        # Calculate quality metrics
        tcursor.execute("SELECT * FROM transformed_loans")
        result_df = pd.DataFrame(tcursor.fetchall(), 
                               columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'loans', ['loan_id', 'customer_id'])
        
        print(f"  Transformed: {self.stats['loans']['transformed']}")
        print(f"  Duplicates: {self.stats['loans']['duplicates']}, NULLs: {nulls}")

    def transform_transactions(self):
        """Transform transactions"""
        print("\nTRANSFORMING TRANSACTIONS")
        
        # Get valid customers
        cursor = self.transformed_connection.cursor()
        cursor.execute("SELECT customer_id FROM transformed_customers")
        valid_custs = set(r[0] for r in cursor.fetchall())
        
        scursor = self.staging_connection.cursor()
        scursor.execute("SELECT * FROM staging_transactions")
        df = pd.DataFrame(scursor.fetchall(), columns=[c[0] for c in scursor.description])
        
        self.stats['transactions']['processed'] = len(df)
        
        if df.empty:
            print("  No data found in staging_transactions")
            return
        
        # Remove duplicates
        initial = len(df)
        df_clean = df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial - len(df_clean)
        self.stats['transactions']['duplicates'] = duplicates_removed
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicate transactions")
        
        tcursor = self.transformed_connection.cursor()
        nulls = 0
        
        for idx, row in df_clean.iterrows():
            cid = self.safe_val(row.get('customer_id'), 'NA')
            if cid not in valid_custs: 
                continue
            
            amt = self.safe_num(row.get('amount'), 0)
            if amt <= 0: 
                continue
            
            # FIXED: Use return_string_na=True for dates
            tdate_result = self.safe_date(row.get('transaction_date'), return_string_na=True)
            if tdate_result == 'NA':
                tdate_sql = None
                continue  # Skip if transaction date is invalid
            else:
                tdate_sql = tdate_result
            
            bal = self.safe_num(row.get('balance_after'), 0)
            if bal == 0: 
                nulls += 1
            
            # Fraud flag conversion
            fraud_flag = str(row.get('fraud_flag', 'false')).lower()
            fraud = fraud_flag in ['true', '1', 'yes', 'y']
            
            # Transaction categorization
            if amt > 10000:
                cat = 'Large'
            elif amt > 1000:
                cat = 'Medium'
            else:
                cat = 'Small'
            
            # Use safe_val for all string fields
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
                if "Duplicate entry" in str(e):
                    continue
                else:
                    raise
        
        self.transformed_connection.commit()
        self.stats['transactions']['nulls'] = nulls
        
        # Calculate quality metrics
        tcursor.execute("SELECT * FROM transformed_transactions")
        result_df = pd.DataFrame(tcursor.fetchall(), 
                               columns=[c[0] for c in tcursor.description])
        self.calc_quality(result_df, 'transactions', ['transaction_id', 'customer_id'])
        
        print(f"  Transformed: {self.stats['transactions']['transformed']}")
        print(f"  Duplicates: {self.stats['transactions']['duplicates']}, NULLs: {nulls}")

    def export_csv(self):
        """Export to CSV - FIXED: Convert ALL NULL values including dates to 'NA'"""
        print("\nEXPORTING CSV")
        files = []
        
        for table in ['transformed_branches', 'transformed_customers', 'transformed_loans', 'transformed_transactions']:
            cursor = self.transformed_connection.cursor()
            cursor.execute(f"SELECT * FROM {table} ORDER BY display_id")
            
            # Get column names and data
            columns = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # FIXED: Convert ALL columns to string and replace NULL values
            for col in df.columns:
                df[col] = df[col].astype(str)
                # Replace all NULL-like values with 'NA'
                df[col] = df[col].apply(lambda x: 'NA' if x in [
                    'None', 'NaN', 'nan', 'NaT', '<NA>', 'NULL', 'null', '', 'NoneType'
                ] else x)
                # Handle special case for date NaT
                df[col] = df[col].apply(lambda x: 'NA' if 'NaT' in str(x) else x)
                # Handle MySQL NULL representation
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
            
            self.transform_branches()
            self.transform_customers()
            self.transform_loans()
            self.transform_transactions()
            
            files = self.export_csv()
            self.print_summary()
            
            print(f"\nExported {len(files)} files to {self.exports_dir}/")
            print("TRANSFORMATION COMPLETED\n")
            
            return {'stats': self.stats, 'quality': self.quality, 'files': files}
            
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
    
    # Try multiple import methods to handle different project structures
    config = None
    
    # Method 1: Try from config.config
    try:
        from config.config import config as cfg
        config = cfg
        print("Config loaded from config.config")
    except ImportError:
        pass
    
    # Method 2: Try direct from config folder
    if config is None:
        try:
            import sys
            import os
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from config import config as cfg
            config = cfg
            print("Config loaded from config")
        except ImportError:
            pass
    
    # Method 3: Try from current directory config
    if config is None:
        try:
            sys.path.insert(0, os.getcwd())
            from config.config import config as cfg
            config = cfg
            print("Config loaded from current directory")
        except ImportError:
            pass
    
    # Method 4: Fallback to manual config
    if config is None:
        print("Using fallback config - update with your database credentials")
        config = {
            'MYSQL_HOST': '127.0.0.1',
            'MYSQL_USER': 'root',
            'MYSQL_PASSWORD': 'Mysql@1234',
            'MYSQL_DATABASE': 'stagging',
            'MYSQL_PORT': 3306
        }
    
    # Ensure the config has required keys
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