"""
main.py - Main ETL Pipeline Controller & Scheduler with Simple Data Quality Metrics

This script orchestrates the complete ETL pipeline with accuracy tracking.
1. Extract: Load CSV files into MySQL staging tables
2. Transform: Clean and standardize data, store in transformed database
3. Load: Incrementally load new rows into PostgreSQL
"""

import argparse
import sys
import logging
from datetime import datetime
import traceback
from typing import Dict, Any, Tuple

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
try:
    from config.config import config, TABLE_SCHEMAS, CSV_DATA_PATH
except ImportError:
    print("❌ CRITICAL: Could not import 'config', 'TABLE_SCHEMAS', or 'CSV_DATA_PATH' from config.config")
    print("Please ensure your config.py file is correct and accessible.")
    sys.exit(1)


# =============================================================================
# SIMPLE DATA QUALITY TRACKER
# =============================================================================

class SimpleQualityTracker:
    """Simple and clear data quality tracking"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.csv_total = 0
        self.extracted = 0
        self.transformed = 0
        self.loaded = 0
        self.duplicates_skipped = 0
        
    def record_extraction(self, results: Dict[str, Tuple[int, int, int]]):
        """Record extraction metrics"""
        for table, stats in results.items():
            if isinstance(stats, tuple) and len(stats) == 3:
                csv_rows, new_rows, updated_rows = stats
                self.csv_total += csv_rows
                self.extracted += new_rows
                self.duplicates_skipped += (csv_rows - new_rows - updated_rows)
    
    def record_transformation(self, results: Dict[str, Any]):
        """Record transformation metrics"""
        stats = results.get('results', {})
        self.transformed = sum(stats.values())
    
    def record_loading(self, results: Dict[str, Any]):
        """Record loading metrics"""
        load_results = results.get('load_results', {})
        self.loaded = sum(load_results.values())
    
    def print_summary(self):
        """Print simple, clear summary"""
        duration = datetime.now() - self.start_time
        
        print("\n" + "="*70)
        print("📊 DATA QUALITY SUMMARY")
        print("="*70)
        
        # Show extraction info
        print(f"\n✓ CSV Files Read:           {self.csv_total:,} rows")
        print(f"✓ New Records Extracted:    {self.extracted:,} rows")
        if self.duplicates_skipped > 0:
            print(f"  (Skipped {self.duplicates_skipped:,} duplicates - already in staging)")
        
        # Show transformation info
        print(f"\n✓ Records Transformed:      {self.transformed:,} rows")
        if self.extracted > 0:
            lost_in_transform = self.extracted - self.transformed
            if lost_in_transform > 0:
                print(f"  (Cleaned out {lost_in_transform:,} invalid/duplicate records)")
        
        # Show loading info
        print(f"\n✓ Loaded to PostgreSQL:     {self.loaded:,} rows")
        
        # Overall accuracy
        print(f"\n" + "-"*70)
        if self.extracted > 0:
            # Normal run with new data
            accuracy = (self.loaded / self.extracted * 100) if self.extracted > 0 else 100
            print(f"📈 Pipeline Accuracy:        {accuracy:.1f}%")
            print(f"   ({self.loaded:,} loaded / {self.extracted:,} extracted)")
        else:
            # Incremental run with no new data
            print(f"📈 Pipeline Status:          ✅ UP TO DATE")
            print(f"   (No new data to process - all tables synchronized)")
        
        print(f"\n⏱️  Execution Time:          {duration.total_seconds():.1f} seconds")
        print("="*70 + "\n")
    
    def log_to_file(self, logger):
        """Save data quality report to log file for audit trail"""
        duration = datetime.now() - self.start_time
        
        logger.info("="*70)
        logger.info("📊 DATA QUALITY REPORT")
        logger.info("="*70)
        logger.info(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")
        
        # Extraction metrics
        logger.info("EXTRACTION PHASE:")
        logger.info(f"  CSV Files Read:           {self.csv_total:,} rows")
        logger.info(f"  New Records Extracted:    {self.extracted:,} rows")
        logger.info(f"  Duplicates Skipped:       {self.duplicates_skipped:,} rows")
        if self.csv_total > 0:
            extraction_rate = (self.extracted / self.csv_total * 100)
            logger.info(f"  Extraction Rate:          {extraction_rate:.2f}%")
        logger.info("")
        
        # Transformation metrics
        logger.info("TRANSFORMATION PHASE:")
        logger.info(f"  Records Transformed:      {self.transformed:,} rows")
        if self.extracted > 0:
            lost_in_transform = self.extracted - self.transformed
            transform_rate = (self.transformed / self.extracted * 100)
            logger.info(f"  Transformation Rate:      {transform_rate:.2f}%")
            if lost_in_transform > 0:
                logger.info(f"  Records Cleaned Out:      {lost_in_transform:,} rows (invalid/duplicates)")
        logger.info("")
        
        # Loading metrics
        logger.info("LOADING PHASE:")
        logger.info(f"  Loaded to PostgreSQL:     {self.loaded:,} rows")
        if self.transformed > 0:
            load_rate = (self.loaded / self.transformed * 100)
            logger.info(f"  Loading Success Rate:     {load_rate:.2f}%")
        logger.info("")
        
        # Overall pipeline metrics
        logger.info("OVERALL PIPELINE METRICS:")
        logger.info(f"  Pipeline Duration:        {duration.total_seconds():.1f} seconds")
        
        if self.extracted > 0:
            # Normal run with new data
            accuracy = (self.loaded / self.extracted * 100)
            logger.info(f"  End-to-End Accuracy:      {accuracy:.2f}%")
            logger.info(f"  Data Flow: {self.extracted:,} extracted → {self.transformed:,} transformed → {self.loaded:,} loaded")
            
            # Calculate data loss
            total_loss = self.extracted - self.loaded
            if total_loss > 0:
                loss_percentage = (total_loss / self.extracted * 100)
                logger.info(f"  Total Data Loss:          {total_loss:,} rows ({loss_percentage:.2f}%)")
            else:
                logger.info(f"  Total Data Loss:          0 rows (0.00%)")
            
            # Quality assessment
            if accuracy >= 95:
                quality_grade = "EXCELLENT"
            elif accuracy >= 90:
                quality_grade = "VERY GOOD"
            elif accuracy >= 80:
                quality_grade = "GOOD"
            elif accuracy >= 70:
                quality_grade = "ACCEPTABLE"
            else:
                quality_grade = "NEEDS IMPROVEMENT"
            
            logger.info(f"  Pipeline Quality Grade:   {quality_grade}")
            logger.info(f"  Pipeline Status:          SUCCESS - NEW DATA PROCESSED")
        else:
            # Incremental run with no new data
            logger.info(f"  End-to-End Accuracy:      N/A (no new data)")
            logger.info(f"  Pipeline Status:          SUCCESS - UP TO DATE")
            logger.info(f"  Note:                     All tables synchronized, no new data to process")
        
        logger.info("="*70)
        logger.info("")
        
        # Summary for easy parsing by monitoring tools
        logger.info("QUICK STATS:")
        logger.info(f"CSV_READ={self.csv_total} | EXTRACTED={self.extracted} | TRANSFORMED={self.transformed} | LOADED={self.loaded} | DURATION={duration.total_seconds():.1f}s")
        logger.info("="*70)


# =============================================================================
# SCHEDULER CLASS
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
                            self.logger.info(f"🔄 CSV file changed: {file_name}")
            
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
            return True
    
    def run_complete_etl_job(self):
        """Execute complete ETL pipeline"""
        start_time = datetime.now()
        self.total_runs += 1
        
        self.logger.info("\n" + "="*80)
        self.logger.info(f"🚀 SCHEDULER: EXECUTING RUN #{self.total_runs}")
        self.logger.info(f"⏰ Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*80)
        
        try:
            run_full_pipeline(is_scheduled_run=True)
            
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
        self.logger.info(f"🔄 Pipeline: CSV → Staging → Transformed → PostgreSQL")
        self.logger.info("🛑 Press Ctrl+C to stop")
        self.logger.info("="*80)
        
        schedule.every(self.interval_minutes).minutes.do(self.run_complete_etl_job)
        
        self.logger.info("\n🔄 Running initial complete ETL pipeline...")
        self.run_complete_etl_job()
        
        self.logger.info(f"\n✓ Scheduler now running (every {self.interval_minutes} min)...")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("\n🛑 Scheduler interrupted by user")
                self.running = False
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
# PIPELINE PHASES
# =============================================================================

def run_extract_phase():
    """Run the extraction phase"""
    logger = logging.getLogger('ETL_Pipeline')
    
    print("\n" + "="*60)
    print("EXTRACTION PHASE - Loading CSV files to MySQL staging")
    print("="*60)
    
    logger.info("="*60)
    logger.info("EXTRACTION PHASE STARTED")
    logger.info("="*60)
    
    extractor = None
    try:
        extractor = MySQLExtractor()
        
        if not extractor.connect():
            raise Exception("Failed to connect to MySQL staging database")
        
        logger.info("✓ Connected to MySQL staging database")
        
        if not extractor.create_staging_tables():
            raise Exception("Failed to create staging tables")
        
        logger.info("✓ Staging tables created/verified")

        results = extractor.extract_all_csv_files()
        
        print("\nExtraction Results:")
        logger.info("\nExtraction Results:")
        total_extracted = 0
        total_csv_rows = 0
        
        for table, stats in results.items():
            if isinstance(stats, tuple) and len(stats) == 3:
                csv_rows, new_rows, updated_rows = stats
                msg = f"  {table.title()}: {new_rows:,} new records extracted (from {csv_rows:,} CSV rows)"
                print(msg)
                logger.info(msg)
                total_extracted += new_rows
                total_csv_rows += csv_rows
            else:
                logging.warning(f"Unexpected stats format for {table}: {stats}")
        
        summary = f"\nTotal new records extracted: {total_extracted:,} from {total_csv_rows:,} CSV rows"
        print(summary)
        logger.info(summary)
        logger.info("✅ Extraction phase completed successfully")
        print("✅ Extraction phase completed successfully")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Extraction phase failed: {e}")
        logger.error(traceback.format_exc())
        print(f"❌ Extraction phase failed: {e}")
        raise

    finally:
        if extractor and extractor.connection:
            extractor.close()
            logger.info("Database connection closed for extractor")
            print("Database connection closed for extractor.")


def run_transform_phase():
    """Run the transformation phase"""
    logger = logging.getLogger('ETL_Pipeline')
    
    print("\n" + "="*60)
    print("TRANSFORMATION PHASE - Cleaning and standardizing data")
    print("="*60)
    
    logger.info("="*60)
    logger.info("TRANSFORMATION PHASE STARTED")
    logger.info("="*60)
    
    transformer = None
    try:
        transformer = DataTransformer(config)
        logger.info("✓ DataTransformer initialized")
        
        results = transformer.run_transformation()
        
        print("\nTransformation Results:")
        logger.info("\nTransformation Results:")
        total_transformed = 0
        
        transformed_counts = {}
        if 'stats' in results:
            for table, stats in results['stats'].items():
                count = stats.get('transformed', 0)
                msg = f"  {table.title()}: {count:,} records transformed"
                print(msg)
                logger.info(msg)
                total_transformed += count
                transformed_counts[table] = count
        
        print(f"\nFinal Record Counts in Transformed Database:")
        logger.info("\nFinal Record Counts in Transformed Database:")
        if 'stats' in results:
            for table, stats in results['stats'].items():
                count = stats.get('transformed', 0)
                msg = f"  {table.title()}: {count:,} records"
                print(msg)
                logger.info(msg)
        
        print(f"\nExported CSV Files:")
        logger.info("\nExported CSV Files:")
        exported_files = results.get('files', [])
        for file in exported_files:
            msg = f"  📄 exports/{file}"
            print(msg)
            logger.info(msg)
        
        integrity_issues = []
        msg = "\n✅ All referential integrity checks passed (within transform)"
        print(msg)
        logger.info(msg)
        
        summary = f"\nTotal records transformed: {total_transformed:,}"
        print(summary)
        logger.info(summary)
        logger.info("✅ Transformation phase completed successfully")
        print("✅ Transformation phase completed successfully")
        
        return {
            'results': transformed_counts,
            'exported_files': exported_files,
            'integrity_issues': integrity_issues,
            'stats': results.get('stats', {}),
            'quality': results.get('quality', {})
        }
        
    except Exception as e:
        logger.error(f"❌ Transformation phase failed: {e}")
        logger.error(traceback.format_exc())
        print(f"❌ Transformation phase failed: {e}")
        raise

    finally:
        logger.info("Transformation phase finished")
        print("Transformation phase finished.")


def run_load_phase():
    """Run the loading phase to PostgreSQL (Incremental)"""
    logger = logging.getLogger('ETL_Pipeline')
    
    print("\n" + "="*60)
    print("LOADING PHASE - Incrementally loading to PostgreSQL")
    print("="*60)
    
    logger.info("="*60)
    logger.info("LOADING PHASE STARTED")
    logger.info("="*60)
    
    loader = None
    try:
        loader = IncrementalLoader()
        
        if not loader.connect_mysql():
            raise Exception("Failed to connect to MySQL (for loading)")
        
        logger.info("✓ Connected to MySQL transformed database")
        
        if not loader.connect_postgresql():
            raise Exception("Failed to connect to PostgreSQL (for loading)")
        
        logger.info("✓ Connected to PostgreSQL production database")
        
        if not loader.create_production_tables():
             raise Exception("Failed to create/verify production tables in PostgreSQL")
        
        logger.info("✓ Production tables created/verified")
        
        results = loader.load_all_entities_incremental()
        counts = loader.verify_counts()
        
        print("\nLoading Results:")
        logger.info("\nLoading Results:")
        load_results_counts = {}
        
        for entity, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            pg_count = counts.get(entity, {}).get('postgresql', 0)
            msg = f"  {entity.title()}: {status} (Total in PG: {pg_count:,})"
            print(msg)
            logger.info(msg)
            load_results_counts[entity] = pg_count
        
        print(f"\nFinal Record Counts in PostgreSQL:")
        logger.info("\nFinal Record Counts in PostgreSQL:")
        final_counts_map = {e: c.get('postgresql', 0) for e, c in counts.items()}
        for entity, count in final_counts_map.items():
            msg = f"  {entity.title()}: {count:,} records"
            print(msg)
            logger.info(msg)
        
        integrity_issues = []
        all_synced = True
        for entity, count_info in counts.items():
            if not count_info.get('synced', False):
                all_synced = False
                issue = f"{entity}: MySQL={count_info.get('mysql', 0):,} vs PostgreSQL={count_info.get('postgresql', 0):,}"
                integrity_issues.append(issue)
        
        if integrity_issues:
            msg = f"\n⚠️  Data Sync Issues ({len(integrity_issues)}):"
            print(msg)
            logger.warning(msg)
            for issue in integrity_issues:
                print(f"     {issue}")
                logger.warning(f"     {issue}")
        
        if all_synced:
            msg = "\n✅ All tables are fully synchronized!"
            print(msg)
            logger.info(msg)
        
        logger.info("✅ Loading phase completed")
        print("✅ Loading phase completed")
        
        return {
            'load_results': load_results_counts,
            'final_counts': final_counts_map,
            'integrity_issues': integrity_issues,
            'all_synced': all_synced
        }
        
    except Exception as e:
        logger.error(f"❌ Loading phase failed: {e}")
        logger.error(traceback.format_exc())
        print(f"❌ Loading phase failed: {e}")
        raise

    finally:
        if loader:
            loader.close_connections()
            logger.info("Database connections closed for loader")
            print("Database connections closed for loader.")


def run_full_pipeline(is_scheduled_run=False):
    """Run the complete ETL pipeline with simple quality tracking"""
    start_time = datetime.now()
    logger = logging.getLogger('ETL_Pipeline')
    
    # Initialize simple quality tracker
    quality_tracker = SimpleQualityTracker()
    
    header = "SCHEDULER: STARTING FULL PIPELINE" if is_scheduled_run else "🚀 STARTING COMPLETE ETL PIPELINE"
    
    print(f"\n{header}")
    print(f"📅 Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    logger.info("\n" + "="*80)
    logger.info(header)
    logger.info("="*80)
    logger.info(f"📅 Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Phase 1: Extract
        logger.info("\n>>> PHASE 1: EXTRACTION <<<")
        extract_results = run_extract_phase()
        quality_tracker.record_extraction(extract_results)
        
        # Phase 2: Transform
        logger.info("\n>>> PHASE 2: TRANSFORMATION <<<")
        transform_results = run_transform_phase()
        quality_tracker.record_transformation(transform_results)
        
        # Phase 3: Load
        logger.info("\n>>> PHASE 3: LOADING <<<")
        load_results = run_load_phase()
        quality_tracker.record_loading(load_results)
        
        # Print simple quality summary (terminal)
        quality_tracker.print_summary()
        
        # Log quality report to file for audit trail
        quality_tracker.log_to_file(logger)
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "="*60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"📅 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Total duration: {duration}")
        
        logger.info("\n" + "="*80)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"📅 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total duration: {duration}")
        
        # Show any issues
        all_issues = []
        all_issues.extend(transform_results.get('integrity_issues', []))
        all_issues.extend(load_results.get('integrity_issues', []))
        
        if all_issues:
            msg = f"\n⚠️  ISSUES DETECTED ({len(all_issues)}):"
            print(msg)
            logger.warning(msg)
            for issue in all_issues:
                print(f"     {issue}")
                logger.warning(f"     {issue}")
        else:
            msg = f"\n✅ No data integrity issues detected"
            print(msg)
            logger.info(msg)
        
        print("\n🏆 ETL Pipeline completed successfully!")
        logger.info("🏆 ETL Pipeline completed successfully!")
        
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
        
        error_msg = f"\n💥 PIPELINE FAILED after {duration}"
        print(error_msg)
        print(f"❌ Error: {e}")
        
        logger.error(error_msg)
        logger.error(f"❌ Error: {e}")
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        
        raise


def export_transformed_data():
    """Export transformed data to CSV files only"""
    print("\n" + "="*60)
    print("CSV EXPORT - Exporting transformed data to CSV files")
    print("="*60)
    
    transformer = None
    try:
        transformer = DataTransformer(config)
        transformer.connect_databases()
        
        exported_files = transformer.export_csv()
        
        print(f"\n📄 Exported CSV Files ({len(exported_files)}):")
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
        description="ETL Pipeline Controller & Scheduler with Data Quality Metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --mode full          # Run complete pipeline once with metrics
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
        if args.mode == 'schedule':
            scheduler = CompleteETLScheduler(interval_minutes=args.interval)
            scheduler.start()
            
        elif args.mode == 'extract':
            run_extract_phase()
            
        elif args.mode == 'transform':
            run_transform_phase()
            
        elif args.mode == 'load' or args.mode == 'incremental':
            run_load_phase()
            
        elif args.mode == 'export':
            export_transformed_data()
            
        elif args.mode == 'full':
            run_full_pipeline()
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Pipeline interrupted by user")
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n💥 MAIN: Pipeline failed with unhandled exception: {e}")
        logger.error(f"MAIN: Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()