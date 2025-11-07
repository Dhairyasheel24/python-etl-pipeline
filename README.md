Bank ETL Pipeline

This is a complete, 3-stage ETL (Extract, Transform, Load) pipeline built with Python. It is designed to read raw banking data from CSV files, clean and enrich it using a multi-tier database architecture, and load it into a final production-ready data warehouse.

The entire pipeline is automated by a built-in scheduler, making it a "set it and forget it" solution for continuous data processing.

🚀 Features

3-Tier Architecture: Uses separate databases (Staging, Transform, Production) for resilience, auditing, and separation of concerns.

Automated Scheduling: Runs the entire pipeline automatically on a timer (e.g., every 1 minute) using the --mode schedule command.

Incremental Loading: The final load step is high-performance, loading only new records, not the entire dataset.

Data Cleaning & Enrichment: Robustly cleans messy data (nulls, bad formats) and derives new business logic (e.g., age, risk_category, customer_segment).

Resilience: The extract phase tracks processed files (with hashes) to prevent duplicate loads.

Full Orchestration: A single main.py entry point manages all pipeline operations.

Setup Utility: setup.py acts as a "health check" to install dependencies and test all database connections.

🏛️ Architecture Flow

+-----------+     +-----------------+     +-------------------+     +-----------------+
|           |     |                 |     |                   |     |                 |
| CSV Files |-(1)->|  MySQL Staging  |-(2)->|  MySQL Transform  |-(3)->|   PostgreSQL    |
|  (data/)  |     |   (DB: stagging)  |     | (DB: transformed) |     | (DB: production)|
|           |     | (Raw, Unchanged)  |     | (Clean, Enriched) |     |  (Query-Ready)  |
+-----------+     +-----------------+     +-------------------+     +-----------------+


Extract: src/extract.py reads CSVs from /data into stagging as raw VARCHAR data.

Transform: src/transform.py reads from stagging, applies cleaning and business logic, and saves to transformed with correct data types.

Load: src/load.py reads from transformed and incrementally loads only new rows into the final bank_production PostgreSQL database.

🛠️ Tech Stack

Language: Python 3.x

Databases: MySQL (for Staging/Transform) & PostgreSQL (for Production)

Core Libraries: pandas, mysql-connector-python, pymysql, psycopg2-binary

Automation: schedule

Configuration: python-dotenv

Networking: Radmin VPN (for shared database access)

🏃 How to Run

1. Initial Setup

First, run the setup.py script. This will install all requirements, create necessary folders, and test all database connections.

# Clone the repository (if you haven't already)
git clone [your-repo-url]
cd python-etl-pipeline

# Create your .env file
# (Copy .env.template and fill in your passwords)
cp .env.template .env

# Run the setup script
python setup.py


2. Run the Full Pipeline (Manually)

To run the entire ETL process one time:

python main.py --mode full


3. Start the Automated Scheduler

To run the pipeline immediately and then again every 1 minute:

python main.py --mode schedule


To change the interval to 5 minutes:

python main.py --mode schedule --interval 5


4. Run Individual Phases

You can run any phase individually for debugging or development:

# Run only the Extract phase
python main.py --mode extract

# Run only the Transform phase
python main.py --mode transform

# Run only the Load phase
python main.py --mode load


5. Check System Health

Run test.py at any time to get a full report on database connections and row counts for all tables.

python test.py
