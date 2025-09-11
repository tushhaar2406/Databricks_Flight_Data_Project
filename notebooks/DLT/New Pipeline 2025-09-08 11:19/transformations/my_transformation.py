import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, Any, List
import os

silver_yaml = "/Workspace/Users/tushhaar24@gmail.com/Databricks_flight_project/Databricks_Flight_Data_Project/configs/silver_pipeline.yml"
class DLTConfigProcessor:
    """Class to process DLT pipeline configuration from YAML"""
    
    def __init__(self, config_path: str = silver_yaml):
        """Initialize with configuration file path"""
        self.config_path = config_path
        self.config = self._load_config()
        self.spark = spark  # Reference to Spark session
        
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Configuration file {self.config_path} not found. Using default configuration.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback default configuration if YAML file is not found"""
        return {
            'pipeline': {'name': 'flight_data_pipeline'},
            'sources': {
                'datasets': {
                    'bookings': {'path': '/Volumes/workspace/flight_bronze/bronzevolume/bookings/data', 'format': 'delta'},
                    'flights': {'path': '/Volumes/workspace/flight_bronze/bronzevolume/flights/data/', 'format': 'delta'},
                    'customers': {'path': '/Volumes/workspace/flight_bronze/bronzevolume/customers/data/', 'format': 'delta'},
                    'airports': {'path': '/Volumes/workspace/flight_bronze/bronzevolume/airports/data/', 'format': 'delta'}
                }
            },
            'tables': {}
        }
    
    def _apply_transformations(self, df, transformations: List[Dict]):
        """Apply transformations to DataFrame based on configuration"""
        for transform in transformations:
            column = transform.get('column')
            operation = transform.get('operation')
            
            if operation == 'cast':
                target_type = transform.get('target_type')
                if target_type == 'DoubleType':
                    df = df.withColumn(column, col(column).cast(DoubleType()))
                elif target_type == 'date':
                    df = df.withColumn(column, to_date(col(column)))
                    
            elif operation == 'add':
                value = transform.get('value')
                if value == 'current_timestamp()':
                    df = df.withColumn(column, current_timestamp())
                    
            elif operation == 'drop':
                df = df.drop(column)
                
        return df
    
    def get_source_path(self, dataset_name: str) -> str:
        """Get source path for a dataset"""
        return self.config['sources']['datasets'][dataset_name]['path']
    
    def get_source_format(self, dataset_name: str) -> str:
        """Get source format for a dataset"""
        return self.config['sources']['datasets'][dataset_name].get('format', 'delta')

# Initialize configuration processor
config_processor = DLTConfigProcessor()

# =======================================================================================
# BOOKING DATA PIPELINE
# =======================================================================================

@dlt.table(name="stage_bookings")
def stage_bookings():
    """Staging table for raw booking data"""
    source_path = config_processor.get_source_path('bookings')
    source_format = config_processor.get_source_format('bookings')
    
    df = spark.readStream.format(source_format).load(source_path)
    return df

@dlt.view(name="trans_bookings")
def trans_bookings():
    """Transformed booking data with type casting and date formatting"""
    df = spark.readStream.table("stage_bookings")
    
    # Apply transformations from config
    table_config = config_processor.config.get('tables', {}).get('trans_bookings', {})
    transformations = table_config.get('transformations', [])
    
    if transformations:
        df = config_processor._apply_transformations(df, transformations)
    else:
        # Fallback to hardcoded transformations if not in config
        df = df.withColumn("amount", col("amount").cast(DoubleType())) \
              .withColumn("modifiedDate", current_timestamp()) \
              .withColumn("booking_date", to_date(col("booking_date"))) \
              .drop("_rescued_data")
    
    return df

# Get data quality rules from config
booking_table_config = config_processor.config.get('tables', {}).get('silver_bookings', {})
quality_rules = booking_table_config.get('data_quality_rules', {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
})

@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(quality_rules)
def silver_bookings():
    """Silver layer booking data with data quality rules"""
    df = spark.readStream.table("trans_bookings")
    return df

# =======================================================================================
# FLIGHT DATA PIPELINE
# =======================================================================================

@dlt.view(name="trans_flights")
def trans_flights():
    """Transformed flight data"""
    source_path = config_processor.get_source_path('flights')
    source_format = config_processor.get_source_format('flights')
    
    df = spark.readStream.format(source_format).load(source_path)
    
    # Apply transformations from config
    table_config = config_processor.config.get('tables', {}).get('trans_flights', {})
    transformations = table_config.get('transformations', [])
    
    if transformations:
        df = config_processor._apply_transformations(df, transformations)
    else:
        # Fallback transformations
        df = df.withColumn("flight_date", to_date(col("flight_date"))) \
              .drop("_rescued_data") \
              .withColumn("modifiedDate", current_timestamp())
    
    return df

# Create streaming table and CDC flow for flights
dlt.create_streaming_table("silver_flights")

# Get CDC configuration from config
flight_cdc_config = config_processor.config.get('tables', {}).get('silver_flights', {}).get('cdc_configuration', {})
keys = flight_cdc_config.get('keys', ["flight_id"])
sequence_by = flight_cdc_config.get('sequence_by', 'modifiedDate')
scd_type = flight_cdc_config.get('scd_type', 1)

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=keys,
    sequence_by=col(sequence_by),
    stored_as_scd_type=scd_type
)

# =======================================================================================
# PASSENGER DATA PIPELINE
# =======================================================================================

@dlt.view(name="trans_passengers")
def trans_passengers():
    """Transformed passenger/customer data"""
    source_path = config_processor.get_source_path('customers')
    source_format = config_processor.get_source_format('customers')
    
    df = spark.readStream.format(source_format).load(source_path)
    
    # Apply transformations from config
    table_config = config_processor.config.get('tables', {}).get('trans_passengers', {})
    transformations = table_config.get('transformations', [])
    
    if transformations:
        df = config_processor._apply_transformations(df, transformations)
    else:
        # Fallback transformations
        df = df.drop("_rescued_data").withColumn("modifiedDate", current_timestamp())
    
    return df

# Create streaming table and CDC flow for passengers
dlt.create_streaming_table("silver_passengers")

passenger_cdc_config = config_processor.config.get('tables', {}).get('silver_passengers', {}).get('cdc_configuration', {})
passenger_keys = passenger_cdc_config.get('keys', ["passenger_id"])
passenger_sequence_by = passenger_cdc_config.get('sequence_by', 'modifiedDate')
passenger_scd_type = passenger_cdc_config.get('scd_type', 1)

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=passenger_keys,
    sequence_by=col(passenger_sequence_by),
    stored_as_scd_type=passenger_scd_type
)

# =======================================================================================
# AIRPORT DATA PIPELINE
# =======================================================================================

@dlt.view(name="trans_airports")
def trans_airports():
    """Transformed airport data"""
    source_path = config_processor.get_source_path('airports')
    source_format = config_processor.get_source_format('airports')
    
    df = spark.readStream.format(source_format).load(source_path)
    
    # Apply transformations from config
    table_config = config_processor.config.get('tables', {}).get('trans_airports', {})
    transformations = table_config.get('transformations', [])
    
    if transformations:
        df = config_processor._apply_transformations(df, transformations)
    else:
        # Fallback transformations
        df = df.drop("_rescued_data").withColumn("modifiedDate", current_timestamp())
    
    return df

# Create streaming table and CDC flow for airports
dlt.create_streaming_table("silver_airports")

airport_cdc_config = config_processor.config.get('tables', {}).get('silver_airports', {}).get('cdc_configuration', {})
airport_keys = airport_cdc_config.get('keys', ["airport_id"])
airport_sequence_by = airport_cdc_config.get('sequence_by', 'modifiedDate')
airport_scd_type = airport_cdc_config.get('scd_type', 1)

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=airport_keys,
    sequence_by=col(airport_sequence_by),
    stored_as_scd_type=airport_scd_type
)

# =======================================================================================
# BUSINESS VIEW
# =======================================================================================

@dlt.table(name="silver_business")
def silver_business():
    """Business view joining all silver layer tables"""
    
    # Get join configuration from config
    business_config = config_processor.config.get('tables', {}).get('silver_business', {})
    join_config = business_config.get('join_configuration', {})
    join_keys = join_config.get('join_keys', ["flight_id", "passenger_id", "airport_id"])
    
    # Start with bookings as base table
    df = dlt.readStream("silver_bookings")
    
    # Join with flights
    if "flight_id" in join_keys:
        df = df.join(dlt.readStream("silver_flights"), ["flight_id"])
    
    # Join with passengers
    if "passenger_id" in join_keys:
        df = df.join(dlt.readStream("silver_passengers"), ["passenger_id"])
    
    # Join with airports
    if "airport_id" in join_keys:
        df = df.join(dlt.readStream("silver_airports"), ["airport_id"])
    
    # Apply transformations from config (e.g., dropping columns)
    transformations = join_config.get('transformations', [])
    if transformations:
        df = config_processor._apply_transformations(df, transformations)
    else:
        df = df.drop("modifiedDate")
    
    return df

# =======================================================================================
# PIPELINE UTILITIES AND MONITORING
# =======================================================================================

def validate_pipeline_config():
    """Validate the pipeline configuration"""
    try:
        required_sections = ['pipeline', 'sources', 'tables']
        for section in required_sections:
            if section not in config_processor.config:
                print(f"Warning: Missing required section '{section}' in configuration")
        
        print(f"Pipeline configuration loaded successfully: {config_processor.config['pipeline']['name']}")
        return True
    except Exception as e:
        print(f"Error validating pipeline configuration: {str(e)}")
        return False

def get_pipeline_info():
    """Get pipeline information from configuration"""
    pipeline_info = config_processor.config.get('pipeline', {})
    print(f"Pipeline Name: {pipeline_info.get('name', 'Unknown')}")
    print(f"Description: {pipeline_info.get('description', 'No description available')}")
    
    # Print table dependencies
    dependencies = config_processor.config.get('dependencies', {})
    if dependencies:
        print("\nTable Dependencies:")
        for table, deps in dependencies.items():
            print(f"  {table}: {deps if deps else 'No dependencies'}")

# Initialize and validate configuration on import
if __name__ == "__main__":
    validate_pipeline_config()
    get_pipeline_info()