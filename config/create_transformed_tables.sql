USE transformed;

-- Drop tables if they exist
DROP TABLE IF EXISTS transformed_transactions;
DROP TABLE IF EXISTS transformed_loans;
DROP TABLE IF EXISTS transformed_customers;
DROP TABLE IF EXISTS transformed_branches;

-- Create transformed_branches table
CREATE TABLE transformed_branches (
    branch_id VARCHAR(10) PRIMARY KEY,
    branch_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    manager_first_name VARCHAR(50),
    manager_last_name VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create transformed_customers table
CREATE TABLE transformed_customers (
    customer_id VARCHAR(15) PRIMARY KEY,
    branch_id VARCHAR(10),
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    dob DATE,
    age INT,
    gender VARCHAR(10),
    email VARCHAR(100),
    phone VARCHAR(15),
    address TEXT,
    account_open_date DATE,
    customer_tenure_days INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create transformed_loans table
CREATE TABLE transformed_loans (
    loan_id VARCHAR(15) PRIMARY KEY,
    customer_id VARCHAR(15),
    loan_type VARCHAR(50),
    loan_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    loan_status VARCHAR(20),
    loan_duration_months INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create transformed_transactions table
CREATE TABLE transformed_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(15),
    transaction_date DATE,
    transaction_type VARCHAR(20),
    amount DECIMAL(15,2),
    balance_after DECIMAL(15,2),
    fraud_flag BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);