"""
main.py - Main ETL Pipeline Controller & Scheduler (Banking Standard)

This script orchestrates the complete ETL pipeline.
Supported Modes:
1. Daily (EOD Batch)
2. Twice Daily (12-Hour Cycle - Mid-Day & EOD)
3. Bi-Weekly (Low Volume / Archival)
"""

import argparse
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple

# --- Imports for Scheduler ---
import schedule
import time

import signal

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
    print("❌ CRITICAL: Could not import config. Check config.py")
    sys.exit(1)


# =============================================================================
# DATA QUALITY METRICS TRACKER
# =============================================================================

class DataQualityMetrics:
    """Tracks data quality and accuracy metrics throughout the ETL pipeline"""
    
    def __init__(self):
        self.metrics = {
            'extraction': {},
            'transformation': {},
            'loading': {},
            'overall': {}
        }
        self.start_time = datetime.now()
        
    def record_extraction_metrics(self, results: Dict[str, Tuple[int, int, int]]):
        total_csv_rows = 0
        total_extracted = 0
        table_metrics = {}
        
        for table, stats in results.items():
            if isinstance(stats, tuple) and len(stats) == 3:
                csv_rows, new_rows, updated_rows = stats
                total_csv_rows += csv_rows
                total_extracted += new_rows
                
                table_metrics[table] = {
                    'csv_rows': csv_rows,
                    'extracted_rows': new_rows,
                    'extraction_rate': (new_rows / csv_rows * 100) if csv_rows > 0 else 0
                }
        
        self.metrics['extraction'] = {
            'total_csv_rows': total_csv_rows,
            'total_extracted': total_extracted,
            'tables': table_metrics
        }
        
    def record_transformation_metrics(self, results: Dict[str, Any]):
        total_transformed = 0
        stats = results.get('stats', {})
        
        for table, table_stats in stats.items():
            total_transformed += table_stats.get('transformed', 0)
            
        self.metrics['transformation'] = {
            'total_transformed': total_transformed,
            'quality_summary': results.get('quality', {}),
            'stats': stats
        }
        
    def record_loading_metrics(self, results: Dict[str, Any]):
        load_results = results.get('load_results', {})
        total_loaded = sum(load_results.values())
        
        self.metrics['loading'] = {
            'total_loaded': total_loaded,
            'integrity_issues': results.get('integrity_issues', []),
            'all_synced': results.get('all_synced', False)
        }
        
    def calculate_overall_metrics(self):
        duration = datetime.now() - self.start_time
        
        total_csv = self.metrics['extraction'].get('total_csv_rows', 0)
        total_loaded = self.metrics['loading'].get('total_loaded', 0)
        
        accuracy = (total_loaded / total_csv * 100) if total_csv > 0 else 0
        
        self.metrics['overall'] = {
            'duration': str(duration),
            'end_to_end_accuracy': round(accuracy, 2),
            'status': 'SUCCESS' if self.metrics['loading'].get('all_synced') else 'PARTIAL'
        }

    def print_summary(self):
        print("\n" + "="*80)
        print("📊 BANKING DATA QUALITY REPORT")
        print("="*80)
        
        ext = self.metrics['extraction']
        trans = self.metrics['transformation']
        load = self.metrics['loading']
        ovr = self.metrics['overall']
        
        print(f"  Duration:              {ovr.get('duration')}")
        print(f"  End-to-End Accuracy:   {ovr.get('end_to_end_accuracy')}%")
        print("-" * 80)
        print(f"  Rows Read (CSV):       {ext.get('total_csv_rows', 0):,}")
        print(f"  Rows Extracted:        {ext.get('total_extracted', 0):,}")
        print(f"  Rows Transformed:      {trans.get('total_transformed', 0):,}")
        print(f"  Rows Loaded (PG):      {load.get('total_loaded', 0):,}")
        
        issues = load.get('integrity_issues', [])
        if issues:
            print(f"\n  ⚠️  Issues ({len(issues)}):")
            for i in issues:
                print(f"     - {i}")
        else:
            print("\n  ✅ No Integrity Issues Detected")
            
        print("="*80 + "\n")

    def export_metrics_to_log(self, logger):
        logger.info(f"Final Metrics: {self.metrics}")


# =============================================================================
# PIPELINE PHASES (ACTUAL WORKERS)
# =============================================================================

def run_extract_phase():
    """Run the extraction phase"""
    print("\n" + "="*60)
    print("EXTRACTION PHASE - Loading CSV files to MySQL staging")
    print("="*60)
    
    extractor = None
    try:
        extractor = MySQLExtractor()
        
        if not extractor.connect():
            raise Exception("Failed to connect to MySQL staging database")
        
        if not extractor.create_staging_tables():
            raise Exception("Failed to create staging tables")

        # Actual extraction work
        results = extractor.extract_all_csv_files()
        
        print("\nExtraction Results:")
        total_extracted = 0
        
        for table, stats in results.items():
            if isinstance(stats, tuple) and len(stats) == 3:
                csv_rows, new_rows, updated_rows = stats
                print(f"  {table.title()}: {new_rows:,} new records extracted")
                total_extracted += new_rows
        
        print(f"\nTotal new records extracted: {total_extracted:,}")
        print("✅ Extraction phase completed successfully")
        return results
        
    except Exception as e:
        logging.error(f"Extraction phase failed: {e}")
        print(f"❌ Extraction phase failed: {e}")
        raise

    finally:
        if extractor and extractor.connection:
            extractor.close()

def run_transform_phase():
    """Run the transformation phase"""
    print("\n" + "="*60)
    print("TRANSFORMATION PHASE - Cleaning and standardizing data")
    print("="*60)
    
    transformer = None
    try:
        transformer = DataTransformer(config)
        # Actual transformation work
        results = transformer.run_transformation()
        
        print("\nTransformation Results:")
        if 'stats' in results:
            for table, stats in results['stats'].items():
                count = stats.get('transformed', 0)
                print(f"  {table.title()}: {count:,} records transformed")
        
        print("✅ Transformation phase completed successfully")
        return results
        
    except Exception as e:
        logging.error(f"Transformation phase failed: {e}")
        print(f"❌ Transformation phase failed: {e}")
        raise

    finally:
        pass # Connections handled inside DataTransformer

def run_load_phase():
    """Run the loading phase to PostgreSQL (Incremental)"""
    print("\n" + "="*60)
    print("LOADING PHASE - Incrementally loading to PostgreSQL")
    print("="*60)
    
    loader = None
    try:
        loader = IncrementalLoader()
        
        if not loader.connect_mysql():
            raise Exception("Failed to connect to MySQL")
        
        if not loader.connect_postgresql():
            raise Exception("Failed to connect to PostgreSQL")
        
        if not loader.create_production_tables():
             raise Exception("Failed to create/verify production tables")
        
        # Actual load work
        results = loader.load_all_entities_incremental()
        counts = loader.verify_counts()
        
        print("\nLoading Results:")
        load_results_counts = {}
        
        for entity, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            pg_count = counts.get(entity, {}).get('postgresql', 0)
            print(f"  {entity.title()}: {status} (Total in PG: {pg_count:,})")
            load_results_counts[entity] = pg_count
        
        # Sync Check
        integrity_issues = []
        all_synced = True
        for entity, count_info in counts.items():
            if not count_info.get('synced', False):
                all_synced = False
                issue = f"{entity}: MySQL={count_info.get('mysql', 0):,} vs PostgreSQL={count_info.get('postgresql', 0):,}"
                integrity_issues.append(issue)
        
        print("✅ Loading phase completed")
        return {
            'load_results': load_results_counts,
            'final_counts': counts,
            'integrity_issues': integrity_issues,
            'all_synced': all_synced
        }
        
    except Exception as e:
        logging.error(f"Loading phase failed: {e}")
        print(f"❌ Loading phase failed: {e}")
        raise

    finally:
        if loader:
            loader.close_connections()

def run_full_pipeline(is_scheduled_run=False):
    """Run the complete ETL pipeline"""
    start_time = datetime.now()
    metrics_tracker = DataQualityMetrics()
    
    header = "BATCH PROCESS: STARTING PIPELINE" if is_scheduled_run else "🚀 STARTING MANUAL PIPELINE"
    print(f"\n{header}")
    print(f"📅 Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Extract
        extract_results = run_extract_phase()
        metrics_tracker.record_extraction_metrics(extract_results)
        
        # 2. Transform
        transform_results = run_transform_phase()
        metrics_tracker.record_transformation_metrics(transform_results)
        
        # 3. Load
        load_results = run_load_phase()
        metrics_tracker.record_loading_metrics(load_results)
        
        # 4. Final Metrics
        metrics_tracker.calculate_overall_metrics()
        metrics_tracker.print_summary()
        
        return True
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


# =============================================================================
# BANKING STANDARD SCHEDULER
# =============================================================================

class BankingBatchScheduler:
    """
    Banking Standard Scheduler
    Executes ETL jobs during off-peak hours or 12h cycles.
    """
    
    def __init__(self, schedule_type: str = 'twice_daily', run_time: str = "01:00"):
        self.schedule_type = schedule_type
        self.run_time = run_time
        self.running = True
        self.is_processing = False # Lock to prevent overlap
        self.total_runs = 0
        self.successful_runs = 0
        
        self.logger = setup_logger("BankingScheduler")
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        self.logger.info("🛑 Shutdown signal received. Finishing current batch if running...")
        self.running = False
    
    def run_batch_job(self):
        """Wrapper to run the pipeline with locking"""
        
        if self.is_processing:
            self.logger.warning("⚠️ SKIPPING SCHEDULE: Previous batch is still processing.")
            return

        self.is_processing = True
        self.total_runs += 1
        
        # Log to file
        self.logger.info(f"🏦 STARTING BATCH RUN #{self.total_runs}")
        
        try:
            # THIS CALLS THE FULL PIPELINE (Extract -> Transform -> Load)
            run_full_pipeline(is_scheduled_run=True)
            
            self.successful_runs += 1
            self.logger.info("✅ Batch Run Completed Successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Batch Run Failed: {e}")
            
        finally:
            self.is_processing = False

    def _calculate_12h_offset(self, start_time_str):
        try:
            start_dt = datetime.strptime(start_time_str, "%H:%M")
            offset_dt = start_dt + timedelta(hours=12)
            return offset_dt.strftime("%H:%M")
        except ValueError:
            return "13:00"

    def start(self):
        """Configure the schedule AND run once immediately"""
        self.logger.info("\n" + "="*80)
        self.logger.info("📅 INITIALIZING BANKING BATCH SCHEDULER")
        self.logger.info(f"   Mode: {self.schedule_type.upper()}")
        self.logger.info("="*80)
        
        # 1. Setup Schedule
        if self.schedule_type == 'daily':
            self.logger.info(f"🗓️  Scheduled: Every Day at {self.run_time}")
            schedule.every().day.at(self.run_time).do(self.run_batch_job)

        elif self.schedule_type == 'twice_daily':
            second_slot = self._calculate_12h_offset(self.run_time)
            self.logger.info(f"🗓️  Scheduled: Twice Daily (Every 12 Hours)")
            self.logger.info(f"   ➤ Slot 1: {self.run_time}")
            self.logger.info(f"   ➤ Slot 2: {second_slot}")
            
            schedule.every().day.at(self.run_time).do(self.run_batch_job)
            schedule.every().day.at(second_slot).do(self.run_batch_job)
            
        elif self.schedule_type == 'biweekly':
            self.logger.info(f"🗓️  Scheduled: Wednesdays and Sundays at {self.run_time}")
            schedule.every().wednesday.at(self.run_time).do(self.run_batch_job)
            schedule.every().sunday.at(self.run_time).do(self.run_batch_job)

        # 2. IMMEDIATE RUN (Crucial Step for User Feedback)
        print("\n" + "!"*80)
        print("🚀 TRIGGERING INITIAL PIPELINE RUN (HEALTH CHECK)")
        print("   The pipeline will run ONCE now, then wait for the schedule.")
        print("!"*80)
        
        # --- THIS LINE WAS MISSING IN YOUR OUTPUT ---
        self.run_batch_job()  
        # ---------------------------------------------

        # 3. Enter Wait Loop
        self.logger.info("\n💤 Initial run complete. Scheduler is now waiting for next slot...")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60) 
            except KeyboardInterrupt:
                self.running = False
            except Exception as e:
                self.logger.error(f"Scheduler Error: {e}")
                time.sleep(60)

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Banking ETL Pipeline Controller")
    
    parser.add_argument(
        "--mode", 
        choices=['full', 'schedule', 'extract', 'transform', 'load'],
        default='full',
        help="Mode of operation"
    )
    
    parser.add_argument(
        "--schedule-type",
        choices=['daily', 'biweekly', 'twice_daily'],
        default='twice_daily',
        help="Schedule Type"
    )
    
    parser.add_argument(
        "--run-time",
        type=str,
        default="01:00",
        help="Primary execution time (HH:MM)"
    )
    
    args = parser.parse_args()
    logger = setup_logger()
    
    try:
        if args.mode == 'schedule':
            scheduler = BankingBatchScheduler(
                schedule_type=args.schedule_type,
                run_time=args.run_time
            )
            scheduler.start()
            
        elif args.mode == 'full':
            run_full_pipeline()
            
    except KeyboardInterrupt:
        print("\nStopped by user")

if __name__ == "__main__":
    main()