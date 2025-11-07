-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS stagging;
USE stagging;

-- Create staging_branches table (only if it doesn't exist)
CREATE TABLE IF NOT EXISTS staging_branches (
    branch_id VARCHAR(50) PRIMARY KEY,
    branch_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(100),
    manager_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create staging_customers table (only if it doesn't exist)
CREATE TABLE IF NOT EXISTS staging_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    branch_id VARCHAR(50),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    dob DATE,
    gender VARCHAR(10),
    email VARCHAR(255),
    phone VARCHAR(20),
    address TEXT,
    account_open_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_branch_id (branch_id),
    INDEX idx_email (email)
);

-- Create staging_loans table (only if it doesn't exist)
CREATE TABLE IF NOT EXISTS staging_loans (
    loan_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    loan_type VARCHAR(100),
    loan_amount DECIMAL(15, 2),
    interest_rate DECIMAL(5, 2),
    start_date DATE,
    end_date DATE,
    loan_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer_id (customer_id),
    INDEX idx_loan_type (loan_type),
    INDEX idx_loan_status (loan_status)
);

-- Create staging_transactions table (only if it doesn't exist)
CREATE TABLE IF NOT EXISTS staging_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    transaction_date DATE,
    transaction_type VARCHAR(50),
    amount DECIMAL(15, 2),
    balance_after DECIMAL(15, 2),
    fraud_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer_id (customer_id),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_transaction_type (transaction_type),
    INDEX idx_fraud_flag (fraud_flag)
);

-- Show created tables
SHOW TABLES;