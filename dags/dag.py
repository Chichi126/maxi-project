from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import great_expectations as gx
from datetime import datetime
import pandas as pd
import logging

TABLES = ['stores', 'sale_transactions', 'inventory', 'customers', 'products', 'sales_managers']
BUCKET_NAME = 'maxi-sales-bucket002'

@dag(
    dag_id='simple_postgres_gcs_ge_dag',
    start_date=datetime(2025, 7,15),
    schedule=None,
    catchup=False,
)
def simple_pipeline():

    @task
    def simple_ge_validation(table_name: str):
        """Simple Great Expectations validation without complex setup"""
        logger = logging.getLogger(__name__)
        
        try:
            # Get data from PostgreSQL
            postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
            
            # Fetch data as pandas dataframe
            df = postgres_hook.get_pandas_df(f"SELECT * FROM {table_name}")
            
            if df.empty:
                raise ValueError(f"No data found in table {table_name}")
            
            logger.info(f"Loaded {len(df)} rows from {table_name}")
            
            # Create GE context and validator
            context = gx.get_context()
            validator = (context.sources.add_pandas("pandas_source")
                        .add_dataframe_asset(table_name, dataframe=df)
                        .build_batch_request()
                        .get_validator())
            
            # Define simple tests based on table
            tests = []
            
            # Common tests for all tables
            tests.extend([
                ("Table not empty", validator.expect_table_row_count_to_be_between(min_value=1)),
                ("ID column exists", validator.expect_column_to_exist(column="id")),
                ("ID values unique", validator.expect_column_values_to_be_unique(column="id")),
                ("No null IDs", validator.expect_column_values_to_not_be_null(column="id")),
            ])
            
          
            # Run tests
            passed = 0
            total = len(tests)
            
            for test_name, result in tests:
                if result.success:
                    passed += 1
                    logger.info(f"✅ {table_name} - {test_name}: PASSED")
                else:
                    logger.error(f"❌ {table_name} - {test_name}: FAILED")
            
            logger.info(f"{table_name}: {passed}/{total} tests passed")
            
            if passed != total:
                raise ValueError(f"Data quality validation failed for {table_name}: {passed}/{total} tests passed")
            
            return {
                "table_name": table_name,
                "tests_passed": passed,
                "tests_total": total,
                "row_count": len(df),
                "status": "validated"
            }
            
        except Exception as e:
            logger.error(f"Validation failed for {table_name}: {str(e)}")
            raise

    # Create tasks for each table
    for table in TABLES:
        # Export task
        export_task = PostgresToGCSOperator(
            task_id=f"export_{table}",
            postgres_conn_id='postgres_conn_id',
            gcp_conn_id='google_conn_id',
            sql=f"SELECT * FROM {table};",
            bucket=BUCKET_NAME,
            filename=f"{table}.parquet",
            export_format='parquet',
            gzip=False,
        )
        
        # Simple validation task
        validation_task = simple_ge_validation(table)
        
        # Set dependency (validation can run in parallel with export)
        # Or make it sequential: export_task >> validation_task
        
    @task
    def final_summary():
        """Final pipeline summary"""
        logger = logging.getLogger(__name__)
        logger.info("🎉 Pipeline completed successfully!")
        logger.info(f"✅ Processed {len(TABLES)} tables with Great Expectations validation")
        return "Pipeline completed"
    
    # Add final summary
    summary = final_summary()

simple_pipeline()