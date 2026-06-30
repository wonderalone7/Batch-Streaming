"""
PRODUCTION AIRFLOW DAG: Batch CSV Processing Pipeline
=======================================================

This DAG ORCHESTRATES your existing streaming_app.py functions.
It uses the business logic from streaming_app.py and manages:
  - When to run
  - Retry logic
  - Monitoring & logging
  - Historical tracking

ARCHITECTURE:
=============
streaming_app.py (Business Logic: Extract, Transform, Load)
        ↓
streaming_batch_dag.py (Orchestration: When, How many times, Monitoring)
        ↓
Airflow (Execution Engine: Run, Log, Alert)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import glob
import shutil
from pathlib import Path

# IMPORT functions from streaming_app.py
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from streaming_app import create_spark_session, process_batch

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_DIR = "c:/Users/Gnyandeep/Desktop/myfolder/PySpark/streaming_input"
OUTPUT_DIR = "c:/Users/Gnyandeep/Desktop/myfolder/PySpark/output"
PROCESSED_DIR = "c:/Users/Gnyandeep/Desktop/myfolder/PySpark/processed_files"

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2026, 6, 1),
}

# ============================================================================
# DAG
# ============================================================================

dag = DAG(
    dag_id='streaming_batch_processing_v2',
    default_args=default_args,
    description='Orchestrates batch processing of CSV files (uses streaming_app.py)',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['pyspark', 'batch-processing', 'etl', 'streaming'],
)

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def setup_directories():
    """
    TASK 1: Setup - Validate and create directories
    """
    print("="*80)
    print("TASK 1: SETUP - Validating directories")
    print("="*80)
    
    dirs = [INPUT_DIR, OUTPUT_DIR, PROCESSED_DIR]
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ {directory}")
    
    return True


def scan_for_files():
    """
    TASK 2: SCAN - Find all CSV files to process
    Returns: List of file paths
    """
    print("="*80)
    print("TASK 2: SCAN - Looking for CSV files")
    print("="*80)
    
    csv_files = sorted(glob.glob(f"{INPUT_DIR}/*.csv"))
    
    if not csv_files:
        print("⚠️  No files found")
        return []
    
    print(f"✓ Found {len(csv_files)} file(s):")
    for file in csv_files:
        print(f"  └─ {Path(file).name}")
    
    # Store files in XCom so next task can use them
    return csv_files


def process_all_files(**context):
    """
    TASK 3: PROCESS - Process ALL found files dynamically
    This is where the ACTUAL WORK happens (calls streaming_app.py functions)
    """
    print("="*80)
    print("TASK 3: PROCESS - Processing files using PySpark")
    print("="*80)
    
    # Get file list from previous task (stored in XCom)
    task_instance = context['task_instance']
    csv_files = task_instance.xcom_pull(task_ids='scan_for_files')
    
    if not csv_files:
        print("⚠️  No files to process")
        return False
    
    batch_num = 0
    processed_count = 0
    
    # Process EACH file
    for csv_file in csv_files:
        print(f"\n{'─'*80}")
        print(f"Processing: {Path(csv_file).name}")
        print(f"{'─'*80}")
        
        # CALL the function from streaming_app.py
        success = process_batch(csv_file, batch_num)
        
        if success:
            processed_count += 1
            batch_num += 1
        else:
            print(f"❌ Failed to process {csv_file}")
    
    print(f"\n✓ Processed {processed_count}/{len(csv_files)} files successfully")
    
    # Store result for next task
    task_instance.xcom_push(
        key='processed_files',
        value=csv_files[:processed_count]
    )
    
    return processed_count > 0


def archive_processed_files(**context):
    """
    TASK 4: ARCHIVE - Move processed files to archive folder
    """
    print("="*80)
    print("TASK 4: ARCHIVE - Moving files to processed_files")
    print("="*80)
    
    # Get processed files from previous task
    task_instance = context['task_instance']
    processed_files = task_instance.xcom_pull(task_ids='process_all_files')
    
    if not processed_files:
        print("⚠️  No files to archive")
        return 0
    
    archived_count = 0
    
    for file_path in processed_files:
        try:
            archived_path = os.path.join(PROCESSED_DIR, Path(file_path).name)
            shutil.move(file_path, archived_path)
            print(f"✓ Archived: {Path(file_path).name}")
            archived_count += 1
        except Exception as e:
            print(f"❌ Error archiving {file_path}: {e}")
    
    print(f"\n✓ Archived {archived_count} files")
    return archived_count


def send_summary_notification(**context):
    """
    TASK 5: NOTIFY - Send completion summary
    """
    print("="*80)
    print("TASK 5: NOTIFY - Batch processing completed")
    print("="*80)
    
    task_instance = context['task_instance']
    processed_count = task_instance.xcom_pull(task_ids='process_all_files')
    archived_count = task_instance.xcom_pull(task_ids='archive_processed_files')
    
    print(f"""
    ✓ PIPELINE COMPLETE
    ─────────────────────────────────────────
    Files processed: {processed_count if processed_count else 0}
    Files archived: {archived_count if archived_count else 0}
    
    Locations:
      Input:     {INPUT_DIR}
      Output:    {OUTPUT_DIR}
      Processed: {PROCESSED_DIR}
    
    Next run: Every hour (check Airflow UI)
    """)
    
    return True


# ============================================================================
# CREATE TASKS
# ============================================================================

task_1_setup = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

task_2_scan = PythonOperator(
    task_id='scan_for_files',
    python_callable=scan_for_files,
    dag=dag,
)

task_3_process = PythonOperator(
    task_id='process_all_files',
    python_callable=process_all_files,
    provide_context=True,
    dag=dag,
)

task_4_archive = PythonOperator(
    task_id='archive_processed_files',
    python_callable=archive_processed_files,
    provide_context=True,
    dag=dag,
)

task_5_notify = PythonOperator(
    task_id='send_summary_notification',
    python_callable=send_summary_notification,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# DEFINE EXECUTION FLOW
# ============================================================================

task_1_setup >> task_2_scan >> task_3_process >> task_4_archive >> task_5_notify


# ============================================================================
# DOCUMENTATION
# ============================================================================

"""
KEY DIFFERENCES FROM streaming_app.py:
======================================

STREAMING_APP.PY (Old - Standalone):
├─ Infinite while True loop
├─ Checks every 10 seconds
├─ Manual error handling
├─ No retry mechanism
├─ No monitoring/logging
└─ Must run manually or via cron

STREAMING_BATCH_DAG.PY (New - Airflow):
├─ No loop - Airflow schedules it
├─ Runs every hour (configurable)
├─ Automatic retry (2 times)
├─ Full logging & monitoring
├─ Web UI to see status
├─ History of all runs
└─ Auto-deployed & scheduled

EXECUTION FLOW:
===============
[Setup] → [Scan Files] → [Process All] → [Archive] → [Notify]

If "Scan Files" fails:
  → Retry 2 times (wait 2 min each)
  → If still fails, stop pipeline
  → "Process All" never runs
  → Airflow alerts you

DYNAMIC FILE PROCESSING:
========================
Unlike static approaches, this DAG:
1. Scans INPUT_DIR and finds ALL .csv files
2. Processes EACH file with process_batch() from streaming_app.py
3. Uses XCom to pass data between tasks
4. Archives all processed files

How XCom works:
  Task 2 finds: ['file1.csv', 'file2.csv', 'file3.csv']
  ↓
  Task 2 stores in XCom with key='processed_files'
  ↓
  Task 3 retrieves from XCom: xcom_pull(task_ids='scan_for_files')
  ↓
  Task 3 loops through each file

AIRFLOW SCHEDULING:
===================
schedule_interval='@hourly' means:
- 9:00 AM → Run DAG
- 10:00 AM → Run DAG
- 11:00 AM → Run DAG
- ... and so on

To change frequency:
- '@daily' → Once a day at midnight
- '@weekly' → Once a week (Sunday midnight)
- '0 2 * * *' → Every day at 2:00 AM (cron format)

MONITORING IN AIRFLOW UI:
=========================
When you run: airflow webserver
Visit: http://localhost:8080

You'll see:
- All past runs (history)
- Which tasks failed
- Task duration
- Logs for each task
- Retry count
- When next run happens
"""
