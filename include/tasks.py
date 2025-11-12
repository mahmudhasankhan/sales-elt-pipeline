import os
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions
from datetime import datetime, UTC
from pathlib import Path
import glob

# get latest excel file
def _get_latest_excel_file(excel_dir):
    """Get the most recent Excel file from the directory"""
    excel_files = glob.glob(os.path.join(excel_dir, "*.xlsx")) + \
                  glob.glob(os.path.join(excel_dir, "*.xls"))
    
    if not excel_files:
        raise FileNotFoundError("No Excel files found in directory")
    
    # Get the most recent file by modification time
    latest_file = max(excel_files, key=os.path.getmtime)
    print(f"Latest file found: {latest_file}")
    return latest_file

# Load service account key json file

def _extract_and_load(excel_dir, service_account):
    
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account 

    # get latest file
    file = _get_latest_excel_file(excel_dir=excel_dir)
    
    df = pd.read_excel(file)

    # variables
    SOURCE_FILE_NAME = Path(file).name
    BATCH_ID = datetime.now(UTC).strftime("batch_%Y_%m")
    LOADED_AT = datetime.now(UTC)
    TABLE_ID = "sales-datawarehouse.raw_data.sales"
    
    # rename columns following bigquery standards
    df.columns = (
            df.columns
            .str.strip() # removing leading/trailing spaces
            .str.lower() # making them lowercase
            .str.replace(r'[ /]+', '_', regex=True) # replacing any spaces and slashes with underscore
            .str.replace(r'[^0-9a-zA-Z_]', '', regex=True) # removing any other special chars
        )

    df['source_file_name'] = SOURCE_FILE_NAME 
    df['batch_id'] = BATCH_ID
    df['loaded_at'] = LOADED_AT

    # Construct a BigQuery client object.
    client = bigquery.Client()

    try:
        # Check if batch already loaded to prevent duplication
        check_query = f"""
            SELECT COUNT(1) as count
            FROM {TABLE_ID}
            WHERE batch_id = '{BATCH_ID}'

        """
        result = client.query(query=check_query).result()
        if list(result)[0].count > 0:
            print(f" Batch {BATCH_ID} already loaded! Skipping ...")
            return
    except exceptions.NotFound:
        print(f"Table {TABLE_ID} not found. Will create on first load.")
    
    # Proceed with loading the table 
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("order_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("customer_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("customer_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country_region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("salesperson", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shipped_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("shipper_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_country_region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payment_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("unit_price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("quantity", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("shipping_fee", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("revenue_bins", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("source_file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {TABLE_ID}")