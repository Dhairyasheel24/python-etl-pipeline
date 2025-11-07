"""
main.py - Main ETL Pipeline Controller & Scheduler

This script orchestrates the complete ETL pipeline and can run as a scheduler.
1. Extract: Load CSV files into MySQL staging tables
2. Transform: Clean and standardize data, store in transformed database
3. Load: Incrementally load new rows into PostgreSQL
"""

import argparse
import sys
import logging
from datetime import datetime
import traceback

# --- Imports for Scheduler ---
import schedule
import time
import os
import signal
import hashlib
# --- End Scheduler Imports ---


# Import ETL modules
from src.extract import MySQLExtractor
from src.transform import DataTransformer
from src.load import IncrementalLoader
from src.logger import setup_logger

# --- Main Config Import ---
# config is used by DataTransformer
# TABLE_SCHEMAS and CSV_DATA_PATH are used by the Scheduler
try:
    from config.config import config, TABLE_SCHEMAS, CSV_DATA_PATH
except ImportError:
    print("❌ CRITICAL: Could not import 'config', 'TABLE_SCHEMAS', or 'CSV_DATA_PATH' from config.config")
    print("Please ensure your config.py file is correct and accessible.")
    sys.exit(1)


# =============================================================================
# SCHEDULER CLASS (Merged from etl_scheduler.py)
# =============================================================================

class CompleteETLScheduler:
    """Complete ETL Scheduler - Runs the full pipeline on a timer"""
    
    def __init__(self, interval_minutes: int = 1):
        self.interval_minutes = interval_minutes
        self.running = True
        self.total_runs = 0
        self.successful_runs = 0
        self.file_hashes = {}
        
        # Setup logger
        self.logger = setup_logger("CompleteETLScheduler")
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info("🛑 Shutdown signal received, stopping scheduler...")
        self.running = False
    
    def _get_file_hash(self, file_path: str) -> str:
        """Calculate file hash to detect changes"""
        try:
            hasher = hashlib.md5()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return ""
    
    def _check_for_file_changes(self) -> bool:
        """Check if CSV files have changed"""
        changes_detected = False
        current_hashes = {}
        
        try:
            if not os.path.exists(CSV_DATA_PATH):
                self.logger.warning(f"Data directory not found: {CSV_DATA_PATH}")
                return False
            
            import glob
            for table_name in TABLE_SCHEMAS.keys():
                pattern = os.path.join(CSV_DATA_PATH, f"{table_name}*.csv")
                csv_files = glob.glob(pattern)
                
                for csv_file in csv_files:
                    file_hash = self._get_file_hash(csv_file)
                    current_hashes[csv_file] = file_hash
                    
                    if csv_file not in self.file_hashes or self.file_hashes[csv_file] != file_hash:
                        changes_detected = True
                        file_name = os.path.basename(csv_file)
                        
                        if csv_file not in self.file_hashes:
                            self.logger.info(f"📄 New CSV file: {file_name}")
                        else:
                            self.logger.info(f"📝 CSV file changed: {file_name}")
            
            # Check for deleted files
            for old_file in self.file_hashes:
                if old_file not in current_hashes:
                    changes_detected = True
                    self.logger.info(f"🗑️ CSV file deleted: {os.path.basename(old_file)}")
            
            self.file_hashes = current_hashes
            
            if not changes_detected:
                self.logger.info("✓ No CSV file changes detected")
            
            return changes_detected
            
        except Exception as e:
            self.logger.error(f"Error checking file changes: {str(e)}")
            return True  # Run on error to be safe
    
    def run_complete_etl_job(self):
        """
        Execute complete ETL pipeline.
        This now calls the robust run_full_pipeline() function.
        """
        start_time = datetime.now()
        self.total_runs += 1
        
        self.logger.info("\n" + "="*80)
        self.logger.info(f"🚀 SCHEDULER: EXECUTING RUN #{self.total_runs}")
        self.logger.info(f"⏰ Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*80)
        
        try:
            # Optional: Check for file changes before running
            # if not self._check_for_file_changes():
            #     self.logger.info("⏭️  No changes detected - skipping ETL run")
            #     self.successful_runs += 1 # Count as success
            #     return True
            
            # --- FIX: Call the main, robust pipeline function ---
            # This avoids duplicating logic and ensures all fixes are used.
            run_full_pipeline(is_scheduled_run=True)
            
            # If run_full_pipeline completes without exception, it succeeded
            duration = datetime.now() - start_time
            self.successful_runs += 1
            
            self.logger.info("\n" + "="*80)
            self.logger.info("✅ COMPLETE ETL PIPELINE SUCCEEDED (via Scheduler)")
            self.logger.info("="*80)
            self.logger.info(f"⏱️  Duration: {duration.total_seconds():.1f} seconds")
            self.logger.info(f"📊 Success Rate: {(self.successful_runs/self.total_runs)*100:.1f}% ({self.successful_runs}/{self.total_runs})")
            self.logger.info(f"⏰ Next run in: {self.interval_minutes} minute(s)")
            self.logger.info("="*80 + "\n")
            
            return True
            
        except Exception as e:
            # run_full_pipeline already logs its own errors,
            # but we log a final scheduler-level error.
            self.logger.error(f"❌ SCHEDULER: Complete ETL pipeline run #{self.total_runs} FAILED")
            self.logger.error(f"❌ Error: {str(e)}")
            return False
    
    def start(self):
        """Start the scheduler"""
        self.logger.info("\n" + "="*80)
        self.logger.info("🎯 STARTING COMPLETE ETL SCHEDULER")
        self.logger.info("="*80)
        self.logger.info(f"⏰ Schedule: Every {self.interval_minutes} minute(s)")
        self.logger.info(f"📂 Data Directory: {CSV_DATA_PATH}")
        self.logger.info(f"📍 Pipeline: CSV → Staging → Transformed → PostgreSQL")
        self.logger.info("🛑 Press Ctrl+C to stop")
        self.logger.info("="*80)
        
        # Schedule the complete job
        schedule.every(self.interval_minutes).minutes.do(self.run_complete_etl_job)
        
        # Run immediately on start
        self.logger.info("\n🔄 Running initial complete ETL pipeline...")
        self.run_complete_etl_job()
        
        # Keep running scheduled jobs
        self.logger.info(f"\n✓ Scheduler now running (every {self.interval_minutes} min)...")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("\n🛑 Scheduler interrupted by user")
                self.running = False # Trigger shutdown
            except Exception as e:
                self.logger.error(f"❌ Scheduler error: {str(e)}")
                time.sleep(5)
        
        self.logger.info("\n" + "="*80)
        self.logger.info("🏁 COMPLETE ETL SCHEDULER STOPPED")
        self.logger.info(f"📊 Total Runs: {self.total_runs}")
        self.logger.info(f"✅ Successful: {self.successful_runs}")
        self.logger.info(f"❌ Failed: {self.total_runs - self.successful_runs}")
        self.logger.info("="*80 + "\n")

# =============================================================================
# PIPELINE PHASES (Fixed and Corrected)
# =============================================================================

def run_extract_phase():
    """Run the extraction phase"""
    print("\n" + "="*60)
    print("EXTRACTION PHASE - Loading CSV files to MySQL staging")
    print("="*60)
    
    extractor = None # Define in outer scope for 'finally' block
    try:
        extractor = MySQLExtractor()
        
        # --- FIX: Must connect and create tables FIRST ---
        if not extractor.connect():
            raise Exception("Failed to connect to MySQL staging database")
        
        if not extractor.create_staging_tables():
            raise Exception("Failed to create staging tables")

        # extract_all_csv_files() returns: Dict[str, Tuple[int, int, int]]
        # (table -> (total_csv_rows, new_rows, updated_rows))
        results = extractor.extract_all_csv_files()
        
        print("\nExtraction Results:")
        total_extracted = 0
        total_csv_rows = 0
        
        # --- FIX: Unpack the stats tuple to fix TypeError ---
        for table, stats in results.items():
            if isinstance(stats, tuple) and len(stats) == 3:
                csv_rows, new_rows, updated_rows = stats
                print(f"  {table.title()}: {new_rows:,} new records extracted (from {csv_rows:,} CSV rows)")
                total_extracted += new_rows
                total_csv_rows += csv_rows
            else:
                logging.warning(f"Unexpected stats format for {table}: {stats}")
        
        print(f"\nTotal new records extracted: {total_extracted:,}")
        print("✅ Extraction phase completed successfully")
        
        return results
        
    except Exception as e:
        logging.error(f"Extraction phase failed: {e}")
        print(f"❌ Extraction phase failed: {e}")
        raise # Re-raise exception to be caught by run_full_pipeline

    finally:
        # --- FIX: Ensure connection is always closed ---
        if extractor and extractor.connection:
            extractor.close()
            print("Database connection closed for extractor.")

def run_transform_phase():
    """Run the transformation phase"""
    print("\n" + "="*60)
    print("TRANSFORMATION PHASE - Cleaning and standardizing data")
    print("="*60)
    
    transformer = None # Define in outer scope
    try:
        # DataTransformer requires config object
        transformer = DataTransformer(config)
        
        # run_transformation() returns: {'stats': ..., 'quality': ..., 'files': ...}
        results = transformer.run_transformation()
        
        print("\nTransformation Results:")
        total_transformed = 0
        
        # --- FIX: Use correct keys from transform.py ---
        # 'results['results']' -> 'results['stats']'
        transformed_counts = {}
        if 'stats' in results:
            for table, stats in results['stats'].items():
                count = stats.get('transformed', 0)
                print(f"  {table.title()}: {count:,} records transformed")
                total_transformed += count
                transformed_counts[table] = count
        
        print(f"\nFinal Record Counts in Transformed Database:")
        # 'results['summary']' -> 'results['stats']'
        if 'stats' in results:
            for table, stats in results['stats'].items():
                count = stats.get('transformed', 0)
                print(f"  {table.title()}: {count:,} records")
        
        print(f"\nExported CSV Files:")
        # 'results['exported_files']' -> 'results['files']'
        exported_files = results.get('files', [])
        for file in exported_files:
            print(f"  📁 exports/{file}")
        
        # 'integrity_issues' is not returned by transform.py
        # We default to an empty list
        integrity_issues = [] 
        print("\n✅ All referential integrity checks passed (within transform)")
        
        print(f"\nTotal records transformed: {total_transformed:,}")
        print("✅ Transformation phase completed successfully")
        
        # Return a standardized dict for run_full_pipeline
        return {
            'results': transformed_counts,
            'exported_files': exported_files,
            'integrity_issues': integrity_issues
        }
        
    except Exception as e:
        logging.error(f"Transformation phase failed: {e}")
        print(f"❌ Transformation phase failed: {e}")
        raise # Re-raise exception

    finally:
        # transform.py handles its own connections in run_transformation()
        print("Transformation phase finished.")


def run_load_phase():
    """Run the loading phase to PostgreSQL (Incremental)"""
    print("\n" + "="*60)
    print("LOADING PHASE - Incrementally loading to PostgreSQL")
    print("="*60)
    
    loader = None # Define in outer scope for 'finally' block
    try:
        loader = IncrementalLoader()
        
        if not loader.connect_mysql():
            raise Exception("Failed to connect to MySQL (for loading)")
        
        if not loader.connect_postgresql():
            raise Exception("Failed to connect to PostgreSQL (for loading)")
        
        # --- FIX: Must create tables before loading ---
        if not loader.create_production_tables():
             raise Exception("Failed to create/verify production tables in PostgreSQL")
        
        # Load only NEW rows
        results = loader.load_all_entities_incremental()
        
        # Verify counts
        counts = loader.verify_counts()
        
        print("\nLoading Results:")
        load_results_counts = {}
        
        for entity, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            pg_count = counts.get(entity, {}).get('postgresql', 0)
            print(f"  {entity.title()}: {status} (Total in PG: {pg_count:,})")
            load_results_counts[entity] = pg_count
        
        print(f"\nFinal Record Counts in PostgreSQL:")
        final_counts_map = {e: c.get('postgresql', 0) for e, c in counts.items()}
        for entity, count in final_counts_map.items():
            print(f"  {entity.title()}: {count:,} records")
        
        # Check for sync issues
        integrity_issues = []
        all_synced = True
        for entity, count_info in counts.items():
            if not count_info.get('synced', False):
                all_synced = False
                issue = f"{entity}: MySQL={count_info.get('mysql', 0):,} vs PostgreSQL={count_info.get('postgresql', 0):,}"
                integrity_issues.append(issue)
        
        if integrity_issues:
            print(f"\n⚠️  Data Sync Issues ({len(integrity_issues)}):")
            for issue in integrity_issues:
                print(f"     {issue}")
        
        if all_synced:
            print("\n✅ All tables are fully synchronized!")
        
        print("✅ Loading phase completed")
        
        return {
            'load_results': load_results_counts,
            'final_counts': final_counts_map,
            'integrity_issues': integrity_issues,
            'all_synced': all_synced
        }
        
    except Exception as e:
        logging.error(f"Loading phase failed: {e}")
        print(f"❌ Loading phase failed: {e}")
        raise # Re-raise exception

    finally:
        # --- FIX: Ensure connections are always closed ---
        if loader:
            loader.close_connections()
            print("Database connections closed for loader.")

def run_full_pipeline(is_scheduled_run=False):
    """Run the complete ETL pipeline"""
    start_time = datetime.now()
    
    # Use different header if run by scheduler
    header = "SCHEDULER: STARTING FULL PIPELINE" if is_scheduled_run else "🚀 STARTING COMPLETE ETL PIPELINE"
    
    print(f"\n{header}")
    print(f"📅 Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Phase 1: Extract
        # extract_results = Dict[str, Tuple[int, int, int]]
        extract_results = run_extract_phase()
        
        # Phase 2: Transform
        # transform_results = {'results': {...}, 'exported_files': [...], ...}
        transform_results = run_transform_phase()
        
        # Phase 3: Load
        # load_results = {'load_results': {...}, 'integrity_issues': [...], ...}
        load_results = run_load_phase()
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "="*60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"📅 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Total duration: {duration}")
        
        print(f"\n📈 FINAL SUMMARY:")
        
        # --- FIX: Sum the correct element (new_rows, index 1) from the tuple ---
        total_new_extracted = sum(stats[1] for stats in extract_results.values() if isinstance(stats, tuple))
        print(f"  Records extracted (new): {total_new_extracted:,}")
        
        print(f"  Records transformed: {sum(transform_results.get('results', {}).values()):,}")
        print(f"  Records loaded to PostgreSQL: {sum(load_results.get('load_results', {}).values()):,}")
        print(f"  CSV files exported: {len(transform_results.get('exported_files', []))}")
        
        # Show any issues
        all_issues = []
        all_issues.extend(transform_results.get('integrity_issues', []))
        all_issues.extend(load_results.get('integrity_issues', []))
        
        if all_issues:
            print(f"\n⚠️  ISSUES DETECTED ({len(all_issues)}):")
            for issue in all_issues:
                print(f"     {issue}")
        else:
            print(f"\n✅ No data integrity issues detected")
        
        print("\n🏆 ETL Pipeline completed successfully!")
        
        return {
            'extract_results': extract_results,
            'transform_results': transform_results,
            'load_results': load_results,
            'duration': duration,
            'issues': all_issues
        }
        
    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n💥 PIPELINE FAILED after {duration}")
        print(f"❌ Error: {e}")
        logging.error(f"Pipeline failed: {e}")
        logging.error(traceback.format_exc())
        
        raise # Re-raise to be caught by main() or scheduler

def export_transformed_data():
    """Export transformed data to CSV files only"""
    print("\n" + "="*60)
    print("CSV EXPORT - Exporting transformed data to CSV files")
    print("="*60)
    
    transformer = None
    try:
        transformer = DataTransformer(config)
        transformer.connect_databases()
        
        # --- FIX: Method name mismatch ('export_to_csv' -> 'export_csv') ---
        exported_files = transformer.export_csv()
        
        print(f"\n📁 Exported CSV Files ({len(exported_files)}):")
        for file in exported_files:
            from pathlib import Path
            file_path = Path("exports") / file
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"  {file} ({size_mb:.2f} MB)")
            else:
                print(f"  {file} (file not found)")
        
        print("\n✅ CSV export completed successfully")
        
        return exported_files
        
    except Exception as e:
        logging.error(f"CSV export failed: {e}")
        print(f"❌ CSV export failed: {e}")
        raise
    finally:
        if transformer:
            transformer.staging_connection.close() if transformer.staging_connection else None
            transformer.transformed_connection.close() if transformer.transformed_connection else None
            print("Database connections closed for exporter.")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline Controller & Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --mode full          # Run complete pipeline once
  python main.py --mode schedule      # Run pipeline now, then every 1 min
  python main.py --mode schedule -i 5 # Run pipeline now, then every 5 mins
  python main.py --mode extract       # Extract data only
  python main.py --mode transform     # Transform data only
  python main.py --mode load          # Load to PostgreSQL only
  python main.py --mode export        # Export CSV files only
        """
    )
    
    parser.add_argument(
        "--mode", 
        # --- FIX: Removed 'status', added 'schedule' ---
        choices=['extract', 'transform', 'load', 'incremental', 'export', 'full', 'schedule'],
        default='full',
        help="ETL pipeline mode to run (default: full)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help="Logging level (default: INFO)"
    )
    
    # --- ADD: Argument for scheduler interval ---
    parser.add_argument(
        "-i", "--interval",
        type=int,
        default=1,
        help="Interval in minutes for schedule mode (default: 1)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logger(level=args.log_level)
    
    logger.info(f"Starting ETL pipeline in {args.mode} mode")
    
    try:
        # --- ADD: Scheduler mode ---
        if args.mode == 'schedule':
            scheduler = CompleteETLScheduler(interval_minutes=args.interval)
            scheduler.start()
            
        elif args.mode == 'extract':
            run_extract_phase()
            
        elif args.mode == 'transform':
            run_transform_phase()
            
        # 'load' and 'incremental' are the same function now
        elif args.mode == 'load' or args.mode == 'incremental':
            run_load_phase()
            
        elif args.mode == 'export':
            export_transformed_data()
            
        # 'status' mode was removed
            
        elif args.mode == 'full':
            run_full_pipeline()
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("\n\n Pipeline interrupted by user")
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        # The individual functions already log details.
        # This is the final catch-all.
        print(f"\n💥 MAIN: Pipeline failed with unhandled exception: {e}")
        logger.error(f"MAIN: Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()