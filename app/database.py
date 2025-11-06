from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions
import os
from dotenv import load_dotenv

load_dotenv()

class BigQueryClient:
    def __init__(self):
        # Validate required environment variables
        required_vars = ['BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET', 'BIGQUERY_TABLE']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Initialize BigQuery client with error handling
        try:
            self.client = bigquery.Client()
        except Exception as e:
            raise ConnectionError(f"Failed to initialize BigQuery client: {str(e)}")
        
        self.project_id = os.getenv('BIGQUERY_PROJECT_ID')
        self.dataset_id = os.getenv('BIGQUERY_DATASET')
        self.table_id = os.getenv('BIGQUERY_TABLE')
        
        # Auto-create dataset and table
        self._create_dataset_if_not_exists()
        self._create_table_if_not_exists()
        
    def get_full_table_id(self):
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"
    
    def _create_dataset_if_not_exists(self):
        """Create the dataset if it doesn't exist"""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set your preferred location
        
        try:
            self.client.create_dataset(dataset, exists_ok=True)
            print(f"✅ Dataset '{self.dataset_id}' is ready")
        except Exception as e:
            print(f"⚠️  Note: {e}")
    
    def _create_table_if_not_exists(self):
        """Create the tasks table if it doesn't exist"""
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id),
            self.table_id
        )
        table = bigquery.Table(table_ref, schema=schema)
        
        try:
            self.client.create_table(table, exists_ok=True)
            print(f"✅ Table '{self.table_id}' is ready")
        except Exception as e:
            print(f"❌ Error creating table: {e}")

# Create a single instance (this will auto-create dataset and table)
bigquery_client = BigQueryClient()