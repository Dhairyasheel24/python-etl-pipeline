"""
Configuration module for ETL pipeline
"""
import os
from dotenv import load_dotenv

# Load environment variables from specific path
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

# MySQL Configuration (Staging Database)
MYSQL_STAGING_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '26.9.242.172'),
    'user': os.getenv('MYSQL_USER', 'TeamETL'),
    'password': os.getenv('MYSQL_PASSWORD', 'TeamETL@123'),
    'database': os.getenv('MYSQL_STAGING_DATABASE', 'stagging'),
    'port': int(os.getenv('MYSQL_PORT', 3306))
}

# MySQL Configuration (Transformed Database)
MYSQL_TRANSFORMED_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '26.9.242.172'),
    'user': os.getenv('MYSQL_USER', 'TeamETL'),
    'password': os.getenv('MYSQL_PASSWORD', 'TeamETL@123'),
    'database': os.getenv('MYSQL_TRANSFORMED_DATABASE', 'transformed'),
    'port': int(os.getenv('MYSQL_PORT', 3306))
}

# PostgreSQL Configuration (Production Database)
POSTGRESQL_CONFIG = {
    'host': os.getenv('POSTGRESQL_HOST', '26.9.242.172'),
    'user': os.getenv('POSTGRESQL_USER', 'bank_app_user'),
    'password': os.getenv('POSTGRESQL_PASSWORD', 'TeamETL@123'),
    'database': os.getenv('POSTGRESQL_DATABASE', 'bank_production'),  # Use lowercase
    'port': int(os.getenv('POSTGRESQL_PORT', 5432))
}

# Legacy config dictionary for backward compatibility
config = {
    'MYSQL_HOST': os.getenv('MYSQL_HOST', '26.9.242.172'),
    'MYSQL_USER': os.getenv('MYSQL_USER', 'TeamETL'),
    'MYSQL_PASSWORD': os.getenv('MYSQL_PASSWORD', 'TeamETL@123'),
    'MYSQL_DATABASE': os.getenv('MYSQL_STAGING_DATABASE', 'stagging'),
    'MYSQL_PORT': int(os.getenv('MYSQL_PORT', 3306)),

    'MYSQL_TRANSFORMED_HOST': os.getenv('MYSQL_HOST', '26.9.242.172'),
    'MYSQL_TRANSFORMED_USER': os.getenv('MYSQL_USER', 'TeamETL'),
    'MYSQL_TRANSFORMED_PASSWORD': os.getenv('MYSQL_PASSWORD', 'TeamETL@123'),
    'MYSQL_TRANSFORMED_DATABASE': os.getenv('MYSQL_TRANSFORMED_DATABASE', 'transformed'),
    'MYSQL_TRANSFORMED_PORT': int(os.getenv('MYSQL_PORT', 3306)),
    
    # PostgreSQL settings
    'POSTGRESQL_HOST': os.getenv('POSTGRESQL_HOST', '26.9.242.172'),
    'POSTGRESQL_USER': os.getenv('POSTGRESQL_USER', 'bank_app_user'),
    'POSTGRESQL_PASSWORD': os.getenv('POSTGRESQL_PASSWORD', 'TeamETL@123'),
    'POSTGRESQL_DATABASE': os.getenv('POSTGRESQL_DATABASE', 'bank_production'),  # Use lowercase
    'POSTGRESQL_PORT': int(os.getenv('POSTGRESQL_PORT', 5432)),
    
    # Other settings
    'CSV_DATA_PATH': os.getenv('CSV_DATA_PATH', './data'),
    'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO'),
    'BATCH_SIZE': int(os.getenv('BATCH_SIZE', 1000)),
}

# ETL Configuration
CSV_DATA_PATH = os.getenv('CSV_DATA_PATH', './data')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))

# Production Table Schemas for PostgreSQL
PRODUCTION_TABLE_SCHEMAS = {
    'branches': {
        'table_name': 'branches',
        'columns': ['branch_id', 'branch_name', 'city', 'state', 'manager_name', 'region'],
        'mysql_columns': ['display_id', 'branch_id', 'branch_name', 'city', 'state', 'manager_name', 'region'],
        'primary_key': 'branch_id',
        'date_columns': [],
        'sql_types': {
            'branch_id': 'SERIAL',
            'branch_name': 'VARCHAR(255) NOT NULL',
            'city': 'VARCHAR(100)',
            'state': 'VARCHAR(100)',
            'manager_name': 'VARCHAR(255)',
            'region': 'VARCHAR(100)'
        }
    },
    'customers': {
        'table_name': 'customers',
        'columns': ['customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 'age',
                   'gender', 'email', 'phone', 'address', 'account_open_date', 'customer_tenure_days',
                   'customer_segment', 'outlier_flag'],
        'mysql_columns': ['display_id', 'customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 'age',
                         'gender', 'email', 'phone', 'address', 'account_open_date', 'customer_tenure_days',
                         'customer_segment', 'outlier_flag'],
        'primary_key': 'customer_id',
        'date_columns': ['dob', 'account_open_date'],
        'sql_types': {
            'customer_id': 'SERIAL',
            'branch_id': 'VARCHAR(50)',  # Changed from INTEGER to VARCHAR
            'first_name': 'VARCHAR(100)',
            'last_name': 'VARCHAR(100)',
            'dob': 'DATE',
            'age': 'INTEGER',
            'gender': 'VARCHAR(10)',
            'email': 'VARCHAR(255)',
            'phone': 'VARCHAR(20)',
            'address': 'TEXT',
            'account_open_date': 'DATE',
            'customer_tenure_days': 'INTEGER',
            'customer_segment': 'VARCHAR(50)',
            'outlier_flag': 'BOOLEAN'
        }
    },
    'loans': {
        'table_name': 'loans',
        'columns': ['loan_id', 'customer_id', 'loan_type', 'loan_amount', 'interest_rate',
                   'start_date', 'end_date', 'loan_status', 'loan_duration_months', 'risk_category', 'outlier_flag'],
        'mysql_columns': ['display_id', 'loan_id', 'customer_id', 'loan_type', 'loan_amount', 'interest_rate',
                         'start_date', 'end_date', 'loan_status', 'loan_duration_months', 'risk_category', 'outlier_flag'],
        'primary_key': 'loan_id',
        'date_columns': ['start_date', 'end_date'],
        'sql_types': {
            'loan_id': 'SERIAL',
            'customer_id': 'VARCHAR (50)',
            'loan_type': 'VARCHAR(50)',
            'loan_amount': 'DECIMAL(15, 2)',
            'interest_rate': 'DECIMAL(5, 2)',
            'start_date': 'DATE',
            'end_date': 'DATE',
            'loan_status': 'VARCHAR(50)',
            'loan_duration_months': 'INTEGER',
            'risk_category': 'VARCHAR(50)',
            'outlier_flag': 'BOOLEAN'
        }
    },
    'transactions': {
        'table_name': 'transactions',
        'columns': ['transaction_id', 'customer_id', 'transaction_date', 'transaction_type',
                   'amount', 'balance_after', 'fraud_flag', 'transaction_category', 'outlier_flag'],
        'mysql_columns': ['display_id', 'transaction_id', 'customer_id', 'transaction_date', 'transaction_type',
                         'amount', 'balance_after', 'fraud_flag', 'transaction_category', 'outlier_flag'],
        'primary_key': 'transaction_id',
        'date_columns': ['transaction_date'],
        'large_table': True,
        'sql_types': {
            'transaction_id': 'SERIAL',
            'customer_id': 'INTEGER',
            'transaction_date': 'TIMESTAMP',
            'transaction_type': 'VARCHAR(50)',
            'amount': 'DECIMAL(15, 2)',
            'balance_after': 'DECIMAL(15, 2)',
            'fraud_flag': 'BOOLEAN',
            'transaction_category': 'VARCHAR(50)',
            'outlier_flag': 'BOOLEAN'
        }
    }
}

# Staging Table Schemas
STAGING_TABLE_SCHEMAS = {
    'branches': {
        'table_name': 'staging_branches',
        'columns': ['branch_id', 'branch_name', 'city', 'state', 'manager_name'],
        'primary_key': 'branch_id',
        'date_columns': []
    },
    'customers': {
        'table_name': 'staging_customers',
        'columns': ['customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 
                   'gender', 'email', 'phone', 'address', 'account_open_date'],
        'primary_key': 'customer_id',
        'date_columns': ['dob', 'account_open_date']
    },
    'loans': {
        'table_name': 'staging_loans',
        'columns': ['loan_id', 'customer_id', 'loan_type', 'loan_amount', 
                   'interest_rate', 'start_date', 'end_date', 'loan_status'],
        'primary_key': 'loan_id',
        'date_columns': ['start_date', 'end_date']
    },
    'transactions': {
        'table_name': 'staging_transactions',
        'columns': ['transaction_id', 'customer_id', 'transaction_date', 
                   'transaction_type', 'amount', 'balance_after', 'fraud_flag'],
        'primary_key': 'transaction_id',
        'date_columns': ['transaction_date'],
        'large_table': True
    }
}

# Transformed Table Schemas
TRANSFORMED_TABLE_SCHEMAS = {
    'branches': {
        'table_name': 'transformed_branches',
        'columns': ['display_id', 'branch_id', 'branch_name', 'city', 'state', 'manager_name', 'region'],
        'primary_key': 'branch_id',
        'date_columns': []
    },
    'customers': {
        'table_name': 'transformed_customers',
        'columns': ['display_id', 'customer_id', 'branch_id', 'first_name', 'last_name', 'dob', 'age',
                   'gender', 'email', 'phone', 'address', 'account_open_date', 'customer_tenure_days',
                   'customer_segment', 'outlier_flag'],
        'primary_key': 'customer_id',
        'date_columns': ['dob', 'account_open_date']
    },
    'loans': {
        'table_name': 'transformed_loans',
        'columns': ['display_id', 'loan_id', 'customer_id', 'loan_type', 'loan_amount', 'interest_rate',
                   'start_date', 'end_date', 'loan_status', 'loan_duration_months', 'risk_category', 'outlier_flag'],
        'primary_key': 'loan_id',
        'date_columns': ['start_date', 'end_date']
    },
    'transactions': {
        'table_name': 'transformed_transactions',
        'columns': ['display_id', 'transaction_id', 'customer_id', 'transaction_date', 'transaction_type',
                   'amount', 'balance_after', 'fraud_flag', 'transaction_category', 'outlier_flag'],
        'primary_key': 'transaction_id',
        'date_columns': ['transaction_date'],
        'large_table': True
    }
}

# Legacy table schemas for backward compatibility
TABLE_SCHEMAS = STAGING_TABLE_SCHEMAS

# Export directory settings
EXPORT_DIR = os.getenv('EXPORT_DIR', './exports')
LOG_DIR = os.getenv('LOG_DIR', './logs')

# Create directories if they don't exist
for directory in [CSV_DATA_PATH, EXPORT_DIR, LOG_DIR]:
    if not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception as e:
            print(f"Warning: Could not create directory {directory}: {e}")

# Validation function
def validate_config():
    """Validate configuration settings"""
    errors = []
    
    # Check MySQL staging connection
    required_mysql = ['host', 'user', 'password', 'database']
    for setting in required_mysql:
        if not MYSQL_STAGING_CONFIG.get(setting):
            errors.append(f"Missing required MySQL staging setting: {setting}")
    
    # Check PostgreSQL connection
    required_postgresql = ['host', 'user', 'password', 'database']
    for setting in required_postgresql:
        if not POSTGRESQL_CONFIG.get(setting):
            errors.append(f"Missing required PostgreSQL setting: {setting}")
    
    if errors:
        raise ValueError(f"Configuration errors: {'; '.join(errors)}")
    
    return True

# Database connection functions
def get_staging_connection():
    """Get MySQL staging database connection parameters"""
    return MYSQL_STAGING_CONFIG

def get_transformed_connection():
    """Get MySQL transformed database connection parameters"""
    return MYSQL_TRANSFORMED_CONFIG

def get_postgresql_connection():
    """Get PostgreSQL database connection parameters"""
    return POSTGRESQL_CONFIG

def get_production_schemas():
    """Get production table schemas for PostgreSQL"""
    return PRODUCTION_TABLE_SCHEMAS

# Validate configuration on import
try:
    validate_config()
    print("✓ Configuration validated successfully")
except ValueError as e:
    print(f"Configuration Warning: {e}")

# Export main configurations
__all__ = [
    'config', 
    'MYSQL_STAGING_CONFIG', 
    'MYSQL_TRANSFORMED_CONFIG', 
    'POSTGRESQL_CONFIG',
    'STAGING_TABLE_SCHEMAS', 
    'TRANSFORMED_TABLE_SCHEMAS',
    'PRODUCTION_TABLE_SCHEMAS',
    'TABLE_SCHEMAS',
    'get_staging_connection',
    'get_transformed_connection', 
    'get_postgresql_connection',
    'get_production_schemas',
    'validate_config'
]