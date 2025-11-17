"""
Incremental Load: Append only NEW rows from MySQL to PostgreSQL
Skips existing rows, only adds new ones
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
import pymysql
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Set

from config.config import (
    get_transformed_connection, 
    get_postgresql_connection,
    TRANSFORMED_TABLE_SCHEMAS,
    PRODUCTION_TABLE_SCHEMAS,
    BATCH_SIZE
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IncrementalLoader:
    """Load only NEW rows from MySQL to PostgreSQL"""
    
    def __init__(self):
        self.mysql_config = get_transformed_connection()
        self.postgresql_config = get_postgresql_connection()
        self.mysql_conn = None
        self.pg_conn = None
        self.batch_size = BATCH_SIZE
        
    def connect_mysql(self):
        """Connect to MySQL transformed database"""
        try:
            self.mysql_conn = pymysql.connect(**self.mysql_config)
            logger.info("✓ Connected to MySQL transformed database")
            return True
        except pymysql.Error as e:
            logger.error(f"✗ MySQL connection failed: {e}")
            return False
    
    def connect_postgresql(self):
        """Connect to PostgreSQL production database"""
        try:
            self.pg_conn = psycopg2.connect(**self.postgresql_config)
            self.pg_conn.autocommit = False
            logger.info("✓ Connected to PostgreSQL production database")
            return True
        except psycopg2.Error as e:
            logger.error(f"✗ PostgreSQL connection failed: {e}")
            return False
    
    def create_production_tables(self):
        """Create production tables in PostgreSQL if they don't exist"""
        if not self.pg_conn:
            logger.error("✗ No PostgreSQL connection available")
            return False
        
        cursor = self.pg_conn.cursor()
        
        try:
            for entity, schema in PRODUCTION_TABLE_SCHEMAS.items():
                table_name = schema['table_name']
                sql_types = schema.get('sql_types', {})
                primary_key = schema.get('primary_key')
                
                # Build CREATE TABLE statement
                columns_def = []
                for col in schema['columns']:
                    col_type = sql_types.get(col, 'TEXT')
                    columns_def.append(f"{col} {col_type}")
                
                # Add PRIMARY KEY constraint
                if primary_key:
                    columns_def.append(f"PRIMARY KEY ({primary_key})")
                
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {', '.join(columns_def)}
                    )
                """
                
                cursor.execute(create_sql)
                logger.info(f"✓ Created/verified table: {table_name}")
            
            # Create indexes
            self._create_indexes(cursor)
            
            self.pg_conn.commit()
            logger.info("✓ All production tables ready")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"✗ Failed to create production tables: {e}")
            self.pg_conn.rollback()
            return False
        finally:
            cursor.close()
    
    def _create_indexes(self, cursor):
        """Create indexes on foreign keys"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_branch ON customers(branch_id)",
            "CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)",
            "CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(loan_status)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)",
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except psycopg2.Error:
                pass
    
    def get_existing_ids(self, table_name: str, id_column: str) -> Set[int]:
        """Get set of existing IDs in PostgreSQL table"""
        if not self.pg_conn:
            return set()
        
        cursor = self.pg_conn.cursor()
        
        try:
            # Get max ID to know how many rows exist
            cursor.execute(f"SELECT MAX({id_column}) FROM {table_name}")
            max_id = cursor.fetchone()[0]
            
            if max_id is None:
                logger.info(f"  📊 Table {table_name} is empty, will load all rows")
                return set()
            
            logger.info(f"  📊 Table {table_name} has {max_id} rows")
            
            # For incremental, we assume sequential IDs
            # Return set of IDs from 1 to max_id
            return set(range(1, max_id + 1))
            
        except psycopg2.Error as e:
            logger.error(f"✗ Failed to get existing IDs: {e}")
            return set()
        finally:
            cursor.close()
    
    def get_new_rows_from_mysql(self, mysql_table: str, mysql_columns: List[str], 
                                primary_key: str, existing_count: int) -> List[Tuple]:
        """Get only NEW rows from MySQL that don't exist in PostgreSQL"""
        if not self.mysql_conn:
            return []
        
        cursor = self.mysql_conn.cursor()
        
        try:
            # Check what columns exist in MySQL table
            cursor.execute(f"DESCRIBE {mysql_table}")
            existing_columns = [row[0] for row in cursor.fetchall()]
            
            # Filter valid columns (skip display_id and primary_key)
            skip_columns = {'display_id', primary_key}
            valid_columns = [col for col in mysql_columns if col in existing_columns and col not in skip_columns]
            
            if not valid_columns:
                logger.error(f"✗ No valid columns found in {mysql_table}")
                return []
            
            # Count total rows in MySQL
            cursor.execute(f"SELECT COUNT(*) FROM {mysql_table}")
            total_mysql_rows = cursor.fetchone()[0]
            
            logger.info(f"  📊 MySQL has {total_mysql_rows} rows")
            
            if total_mysql_rows <= existing_count:
                logger.info(f"  ✓ No new rows to load")
                return []
            
            # Calculate how many new rows
            new_rows_count = total_mysql_rows - existing_count
            logger.info(f"  📥 Found {new_rows_count} new rows to load")
            
            # Get new rows using LIMIT and OFFSET
            query = f"""
                SELECT {', '.join(valid_columns)} 
                FROM {mysql_table} 
                ORDER BY display_id
                LIMIT {new_rows_count} OFFSET {existing_count}
            """
            
            cursor.execute(query)
            data = cursor.fetchall()
            
            logger.info(f"✓ Fetched {len(data)} new rows from {mysql_table}")
            return data
            
        except pymysql.Error as e:
            logger.error(f"✗ Failed to fetch new rows: {e}")
            return []
        finally:
            cursor.close()
    
    def insert_new_rows(self, table_name: str, columns: List[str], data: List[Tuple]) -> bool:
        """Insert only NEW rows into PostgreSQL"""
        if not self.pg_conn or not data:
            return False
        
        cursor = self.pg_conn.cursor()
        
        try:
            # Prepare INSERT statement (no ON CONFLICT, just append)
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            # Convert boolean columns
            converted_data = []
            for row in data:
                converted_row = list(row)
                boolean_columns = ['fraud_flag', 'outlier_flag']
                for bool_col in boolean_columns:
                    if bool_col in columns:
                        bool_idx = columns.index(bool_col)
                        if bool_idx < len(converted_row) and converted_row[bool_idx] is not None:
                            converted_row[bool_idx] = bool(int(converted_row[bool_idx]))
                converted_data.append(tuple(converted_row))
            
            # Insert in batches
            total_rows = len(converted_data)
            inserted = 0
            
            for i in range(0, total_rows, self.batch_size):
                batch = converted_data[i:i + self.batch_size]
                cursor.executemany(insert_sql, batch)
                self.pg_conn.commit()
                inserted += len(batch)
                
                progress = min(i + self.batch_size, total_rows)
                logger.info(f"  → Inserted {progress}/{total_rows} new rows")
            
            logger.info(f"✓ Successfully added {inserted} new rows to {table_name}")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"✗ Failed to insert new rows: {e}")
            self.pg_conn.rollback()
            return False
        finally:
            cursor.close()
    
    def load_entity_incremental(self, entity: str) -> bool:
        """Load only NEW rows for a specific entity"""
        transformed_schema = TRANSFORMED_TABLE_SCHEMAS.get(entity)
        production_schema = PRODUCTION_TABLE_SCHEMAS.get(entity)
        
        if not transformed_schema or not production_schema:
            logger.error(f"✗ Schema not found for entity: {entity}")
            return False
        
        mysql_table = transformed_schema['table_name']
        pg_table = production_schema['table_name']
        pg_columns = production_schema['columns']
        mysql_columns = production_schema.get('mysql_columns', pg_columns)
        primary_key = production_schema['primary_key']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📥 Loading NEW rows for: {entity}")
        logger.info(f"Source: {mysql_table} → Target: {pg_table}")
        logger.info(f"{'='*60}")
        
        # Get count of existing rows in PostgreSQL
        existing_ids = self.get_existing_ids(pg_table, primary_key)
        existing_count = len(existing_ids)
        
        # Get only NEW rows from MySQL
        new_data = self.get_new_rows_from_mysql(mysql_table, mysql_columns, primary_key, existing_count)
        
        if not new_data:
            logger.info(f"  ✓ No new rows to add for {entity}")
            return True
        
        # Prepare columns (without primary key, it auto-generates)
        insert_columns = [col for col in pg_columns if col != primary_key]
        actual_columns = insert_columns[:len(new_data[0])]
        
        # Insert new rows
        return self.insert_new_rows(pg_table, actual_columns, new_data)
    
    def load_all_entities_incremental(self) -> Dict[str, bool]:
        """Load NEW rows for all entities"""
        results = {}
        
        # Create tables if they don't exist
        if not self.create_production_tables():
            logger.error("✗ Failed to create production tables")
            return results
        
        # Load entities in order
        load_order = ['branches', 'customers', 'loans', 'transactions']
        
        for entity in load_order:
            try:
                success = self.load_entity_incremental(entity)
                results[entity] = success
            except Exception as e:
                logger.error(f"✗ Error loading {entity}: {e}")
                results[entity] = False
        
        return results
    
    def verify_counts(self) -> Dict[str, Dict[str, int]]:
        """Compare row counts between MySQL and PostgreSQL"""
        if not self.mysql_conn or not self.pg_conn:
            return {}
        
        counts = {}
        
        logger.info("\n📊 Verifying row counts:")
        logger.info("=" * 70)
        logger.info(f"{'Table':<20} {'MySQL':<15} {'PostgreSQL':<15} {'Status':<10}")
        logger.info("-" * 70)
        
        for entity, schema in PRODUCTION_TABLE_SCHEMAS.items():
            pg_table = schema['table_name']
            mysql_table = TRANSFORMED_TABLE_SCHEMAS[entity]['table_name']
            
            try:
                # Get MySQL count
                mysql_cursor = self.mysql_conn.cursor()
                mysql_cursor.execute(f"SELECT COUNT(*) FROM {mysql_table}")
                mysql_count = mysql_cursor.fetchone()[0]
                mysql_cursor.close()
                
                # Get PostgreSQL count
                pg_cursor = self.pg_conn.cursor()
                pg_cursor.execute(f"SELECT COUNT(*) FROM {pg_table}")
                pg_count = pg_cursor.fetchone()[0]
                pg_cursor.close()
                
                status = "✓ Synced" if mysql_count == pg_count else "⚠️ Diff"
                
                counts[entity] = {
                    'mysql': mysql_count,
                    'postgresql': pg_count,
                    'synced': mysql_count == pg_count
                }
                
                logger.info(f"{pg_table:<20} {mysql_count:<15,} {pg_count:<15,} {status:<10}")
                
            except Exception as e:
                logger.error(f"Failed to verify {entity}: {e}")
        
        logger.info("=" * 70)
        return counts
    
    def close_connections(self):
        """Close database connections"""
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("✓ MySQL connection closed")
        
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("✓ PostgreSQL connection closed")


def main():
    """Main execution function"""
    start_time = datetime.now()
    logger.info("\n" + "="*80)
    logger.info("🚀 Starting Incremental Load (Append New Rows Only)")
    logger.info("="*80)
    
    loader = IncrementalLoader()
    
    try:
        # Connect to databases
        if not loader.connect_mysql():
            logger.error("✗ Failed to connect to MySQL")
            return
        
        if not loader.connect_postgresql():
            logger.error("✗ Failed to connect to PostgreSQL")
            return
        
        # Load only NEW rows
        results = loader.load_all_entities_incremental()
        
        # Verify counts
        counts = loader.verify_counts()
        
        # Summary
        logger.info("\n" + "="*80)
        logger.info("📋 Load Summary")
        logger.info("="*80)
        
        for entity, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED"
            logger.info(f"  {entity}: {status}")
        
        total_success = sum(1 for s in results.values() if s)
        total_entities = len(results)
        logger.info(f"\nTotal: {total_success}/{total_entities} entities processed")
        
        # Show sync status
        all_synced = all(count['synced'] for count in counts.values())
        if all_synced:
            logger.info("\n✓ All tables are fully synchronized!")
        else:
            logger.info("\n⚠️  Some tables have differences - run again to sync")
        
        # Execution time
        duration = datetime.now() - start_time
        logger.info(f"\n⏱️  Total execution time: {duration}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"✗ Load failed: {e}")
        raise
    
    finally:
        loader.close_connections()


if __name__ == "__main__":
    main()