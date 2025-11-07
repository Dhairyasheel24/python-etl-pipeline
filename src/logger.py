import logging
import os
from datetime import datetime

# Add the project root to Python path
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import LOG_LEVEL

def setup_logger(name: str = "ETL_Pipeline", level: str = None) -> logging.Logger:
    """
    Set up logger with both file and console handlers
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Set log level
    if level is None:
        log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    else:
        log_level = getattr(logging, level.upper(), logging.INFO)
    
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with current date
    current_date = datetime.now().strftime("%Y%m%d")
    log_filename = f"{log_dir}/etl_pipeline_{current_date}.log"
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def log_extraction_stats(logger: logging.Logger, table_name: str, 
                        total_rows: int, new_rows: int, updated_rows: int):
    """
    Log extraction statistics
    
    Args:
        logger: Logger instance
        table_name: Name of the table
        total_rows: Total rows in CSV
        new_rows: New rows inserted
        updated_rows: Number of rows updated
    """
    logger.info(f"Extraction completed for {table_name}:")
    logger.info(f"  Total rows in CSV: {total_rows:,}")
    logger.info(f"  New rows inserted: {new_rows:,}")
    logger.info(f"  Rows updated: {updated_rows:,}")
    if total_rows > 0:
        success_rate = ((new_rows + updated_rows) / total_rows) * 100
        logger.info(f"  Success rate: {success_rate:.1f}%")

def log_transformation_stats(logger: logging.Logger, table_name: str,
                            processed: int, transformed: int, 
                            duplicates: int, nulls: int, outliers: int = 0):
    """
    Log transformation statistics
    
    Args:
        logger: Logger instance
        table_name: Name of the table
        processed: Total rows processed from staging
        transformed: Rows successfully transformed
        duplicates: Number of duplicates removed
        nulls: Number of NULL values replaced
        outliers: Number of outliers detected (optional)
    """
    logger.info(f"Transformation completed for {table_name}:")
    logger.info(f"  Processed: {processed:,} rows")
    logger.info(f"  Transformed: {transformed:,} rows")
    logger.info(f"  Duplicates removed: {duplicates:,}")
    logger.info(f"  NULL values replaced: {nulls:,}")
    if outliers > 0:
        logger.info(f"  Outliers detected: {outliers:,}")
    
    if processed > 0:
        success_rate = (transformed / processed) * 100
        data_quality = ((processed - duplicates - nulls) / processed) * 100
        logger.info(f"  Success rate: {success_rate:.1f}%")
        logger.info(f"  Data quality: {data_quality:.1f}%")

def log_loading_stats(logger: logging.Logger, table_name: str,
                     total_rows: int, loaded_rows: int, errors: int = 0,
                     incremental: bool = False):
    """
    Log loading statistics for PostgreSQL
    
    Args:
        logger: Logger instance
        table_name: Name of the table
        total_rows: Total rows processed
        loaded_rows: Rows successfully loaded
        errors: Number of errors encountered
        incremental: Whether this is an incremental load
    """
    load_type = "Incremental Load" if incremental else "Full Load"
    logger.info(f"{load_type} completed for {table_name}:")
    logger.info(f"  Total rows processed: {total_rows:,}")
    logger.info(f"  Rows loaded: {loaded_rows:,}")
    logger.info(f"  Errors: {errors:,}")
    
    if total_rows > 0:
        success_rate = (loaded_rows / total_rows) * 100
        logger.info(f"  Success rate: {success_rate:.1f}%")

def log_incremental_load_stats(logger: logging.Logger, table_name: str,
                              existing_rows: int, new_rows: int, 
                              total_rows: int, sync_status: bool):
    """
    Log incremental loading statistics
    
    Args:
        logger: Logger instance
        table_name: Name of the table
        existing_rows: Number of existing rows in target
        new_rows: Number of new rows added
        total_rows: Total rows after load
        sync_status: Whether tables are synchronized
    """
    logger.info(f"Incremental load completed for {table_name}:")
    logger.info(f"  Existing rows: {existing_rows:,}")
    logger.info(f"  New rows added: {new_rows:,}")
    logger.info(f"  Total rows after load: {total_rows:,}")
    
    status = "✅ Synchronized" if sync_status else "⚠️ Out of sync"
    logger.info(f"  Sync status: {status}")

def log_pipeline_summary(logger: logging.Logger, 
                        extraction_results: dict,
                        transformation_results: dict,
                        loading_results: dict = None,
                        duration_seconds: float = None):
    """
    Log complete pipeline summary
    
    Args:
        logger: Logger instance
        extraction_results: Dictionary with extraction stats per table
        transformation_results: Dictionary with transformation stats per table
        loading_results: Dictionary with loading stats per table (optional)
        duration_seconds: Total pipeline duration in seconds
    """
    logger.info("\n" + "="*70)
    logger.info("COMPLETE PIPELINE SUMMARY")
    logger.info("="*70)
    
    # Extraction summary
    logger.info("\nEXTRACTION PHASE:")
    total_extracted = 0
    for table, (csv_rows, new_rows, updated_rows) in extraction_results.items():
        total_extracted += new_rows
        logger.info(f"  {table}: {new_rows:,} new records")
    logger.info(f"  TOTAL EXTRACTED: {total_extracted:,} records")
    
    # Transformation summary
    logger.info("\nTRANSFORMATION PHASE:")
    total_transformed = 0
    total_duplicates = 0
    total_nulls = 0
    
    for table, stats in transformation_results['stats'].items():
        total_transformed += stats.get('transformed', 0)
        total_duplicates += stats.get('duplicates', 0)
        total_nulls += stats.get('nulls', 0)
        
        logger.info(f"  {table}:")
        logger.info(f"    Transformed: {stats.get('transformed', 0):,}")
        logger.info(f"    Duplicates: {stats.get('duplicates', 0):,}")
        logger.info(f"    NULLs: {stats.get('nulls', 0):,}")
    
    logger.info(f"  TOTAL TRANSFORMED: {total_transformed:,} records")
    logger.info(f"  TOTAL DUPLICATES REMOVED: {total_duplicates:,}")
    logger.info(f"  TOTAL NULLS REPLACED: {total_nulls:,}")
    
    # Loading summary (if provided)
    if loading_results:
        logger.info("\nLOADING PHASE:")
        total_loaded = 0
        total_errors = 0
        
        for table, stats in loading_results.items():
            loaded = stats.get('loaded', 0)
            errors = stats.get('errors', 0)
            total_loaded += loaded
            total_errors += errors
            
            logger.info(f"  {table}:")
            logger.info(f"    Loaded: {loaded:,}")
            logger.info(f"    Errors: {errors:,}")
        
        logger.info(f"  TOTAL LOADED: {total_loaded:,} records")
        logger.info(f"  TOTAL ERRORS: {total_errors:,}")
        
        if total_loaded > 0:
            overall_success_rate = (total_loaded / (total_loaded + total_errors)) * 100
            logger.info(f"  OVERALL SUCCESS RATE: {overall_success_rate:.1f}%")
    
    # Quality metrics
    if 'quality' in transformation_results:
        logger.info("\nQUALITY METRICS:")
        for table, metrics in transformation_results['quality'].items():
            logger.info(f"  {table}:")
            logger.info(f"    Completeness: {metrics.get('completeness', 0):.1f}%")
            logger.info(f"    Accuracy: {metrics.get('accuracy', 0):.1f}%")
    
    # Overall stats
    if duration_seconds:
        logger.info(f"\nPIPELINE DURATION: {duration_seconds:.1f} seconds")
    
    logger.info("="*70 + "\n")

def log_error(logger: logging.Logger, phase: str, error: Exception):
    """
    Log pipeline errors with context
    
    Args:
        logger: Logger instance
        phase: Phase where error occurred (extraction/transformation/loading)
        error: Exception object
    """
    logger.error(f"ERROR in {phase.upper()} phase:")
    logger.error(f"  {type(error).__name__}: {str(error)}")
    
    import traceback
    logger.error("Traceback:")
    for line in traceback.format_exc().split('\n'):
        if line.strip():
            logger.error(f"  {line}")

def log_data_quality_issues(logger: logging.Logger, issues: dict):
    """
    Log data quality issues found during transformation
    
    Args:
        logger: Logger instance
        issues: Dictionary of data quality issues by table
    """
    logger.warning("\n" + "⚠️ "*35)
    logger.warning("DATA QUALITY ISSUES DETECTED")
    logger.warning("⚠️ "*35)
    
    for table, table_issues in issues.items():
        if table_issues:
            logger.warning(f"\n{table.upper()}:")
            for issue_type, count in table_issues.items():
                if count > 0:
                    logger.warning(f"  {issue_type}: {count:,} records")
    
    logger.warning("⚠️ "*35 + "\n")

def log_incremental_sync_status(logger: logging.Logger, sync_results: dict):
    """
    Log incremental synchronization status
    
    Args:
        logger: Logger instance
        sync_results: Dictionary with sync status per table
    """
    logger.info("\n" + "🔄 "*20)
    logger.info("INCREMENTAL LOAD SYNC STATUS")
    logger.info("🔄 "*20)
    
    all_synced = True
    
    for table, status in sync_results.items():
        mysql_count = status.get('mysql', 0)
        postgresql_count = status.get('postgresql', 0)
        synced = status.get('synced', False)
        
        if synced:
            logger.info(f"✅ {table}: {mysql_count:,} rows (Synced)")
        else:
            logger.info(f"⚠️  {table}: MySQL={mysql_count:,}, PostgreSQL={postgresql_count:,} (Out of sync)")
            all_synced = False
    
    if all_synced:
        logger.info("\n🎉 ALL TABLES ARE FULLY SYNCHRONIZED!")
    else:
        logger.info("\n⚠️  Some tables require re-synchronization")
    
    logger.info("🔄 "*20 + "\n")