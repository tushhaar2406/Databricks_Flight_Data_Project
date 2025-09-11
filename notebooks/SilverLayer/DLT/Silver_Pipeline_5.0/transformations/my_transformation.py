import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import (
    DoubleType, DateType, StringType, IntegerType,
    LongType, FloatType, BooleanType, TimestampType
)

# -----------------------------------------------------------------------------------------
# Load YAML configuration
# -----------------------------------------------------------------------------------------
config_path = "/Workspace/Users/tushhaar24@gmail.com/Databricks_flight_project/Databricks_Flight_Data_Project/configs/silver_pipeline.yml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

sources = config["sources"]["datasets"]
tables = config["tables"]

# -----------------------------------------------------------------------------------------
# Helper: Map YAML types to PySpark types
# -----------------------------------------------------------------------------------------
type_mapping = {
    "DoubleType": DoubleType(),
    "DateType": DateType(),
    "date": DateType(),  # Added for your config
    "StringType": StringType(),
    "IntegerType": IntegerType(),
    "LongType": LongType(),
    "FloatType": FloatType(),
    "BooleanType": BooleanType(),
    "TimestampType": TimestampType()
}

def apply_transformations(df, transformations):
    """Apply transformations from YAML config to a DataFrame."""
    for t in transformations:
        if t["operation"] == "cast":
            target_type = type_mapping.get(t["target_type"], StringType())
            if t["column"] == "booking_date":
                # Special handling for date conversion
                df = df.withColumn(t["column"], to_date(col(t["column"])))
            else:
                df = df.withColumn(t["column"], col(t["column"]).cast(target_type))
        elif t["operation"] == "add" and t["value"] == "current_timestamp()":
            df = df.withColumn(t["column"], current_timestamp())
        elif t["operation"] == "drop":
            df = df.drop(t["column"])
    return df

# -----------------------------------------------------------------------------------------
# Booking Data
# -----------------------------------------------------------------------------------------
@dlt.table(
    name="stage_bookings",
    comment=tables["stage_bookings"]["description"]
)
def stage_bookings():
    dataset = sources["bookings"]
    return spark.readStream.format(dataset["format"]).load(dataset["path"])


@dlt.view(
    name="trans_bookings",
    comment=tables["trans_bookings"]["description"]
)
def trans_bookings():
    df = dlt.read_stream("stage_bookings")
    return apply_transformations(df, tables["trans_bookings"]["transformations"])


@dlt.table(
    name="silver_bookings",
    comment=tables["silver_bookings"]["description"]
)
@dlt.expect_all_or_drop(tables["silver_bookings"]["data_quality_rules"])
def silver_bookings():
    return dlt.read_stream("trans_bookings")

# -----------------------------------------------------------------------------------------
# Flight Data
# -----------------------------------------------------------------------------------------
@dlt.view(
    name="trans_flights",
    comment=tables["trans_flights"]["description"]
)
def trans_flights():
    dataset = sources["flights"]
    df = spark.readStream.format(dataset["format"]).load(dataset["path"])
    return apply_transformations(df, tables["trans_flights"]["transformations"])

# Create streaming table for flights
dlt.create_streaming_table(
    name="silver_flights",
    comment=tables["silver_flights"]["description"]
)

# Apply CDC for flights
dlt.apply_changes(
    target="silver_flights",
    source="trans_flights",
    keys=tables["silver_flights"]["cdc_configuration"]["keys"],
    sequence_by=tables["silver_flights"]["cdc_configuration"]["sequence_by"],
    stored_as_scd_type=tables["silver_flights"]["cdc_configuration"]["scd_type"]
)

# -----------------------------------------------------------------------------------------
# Passenger Data
# -----------------------------------------------------------------------------------------
@dlt.view(
    name="trans_passengers",
    comment=tables["trans_passengers"]["description"]
)
def trans_passengers():
    dataset = sources["customers"]
    df = spark.readStream.format(dataset["format"]).load(dataset["path"])
    return apply_transformations(df, tables["trans_passengers"]["transformations"])

# Create streaming table for passengers
dlt.create_streaming_table(
    name="silver_passengers",
    comment=tables["silver_passengers"]["description"]
)

# Apply CDC for passengers
dlt.apply_changes(
    target="silver_passengers",
    source="trans_passengers",
    keys=tables["silver_passengers"]["cdc_configuration"]["keys"],
    sequence_by=tables["silver_passengers"]["cdc_configuration"]["sequence_by"],
    stored_as_scd_type=tables["silver_passengers"]["cdc_configuration"]["scd_type"]
)

# -----------------------------------------------------------------------------------------
# Airport Data
# -----------------------------------------------------------------------------------------
@dlt.view(
    name="trans_airports",
    comment=tables["trans_airports"]["description"]
)
def trans_airports():
    dataset = sources["airports"]
    df = spark.readStream.format(dataset["format"]).load(dataset["path"])
    return apply_transformations(df, tables["trans_airports"]["transformations"])

# Create streaming table for airports
dlt.create_streaming_table(
    name="silver_airports",
    comment=tables["silver_airports"]["description"]
)

# Apply CDC for airports
dlt.apply_changes(
    target="silver_airports",
    source="trans_airports",
    keys=tables["silver_airports"]["cdc_configuration"]["keys"],
    sequence_by=tables["silver_airports"]["cdc_configuration"]["sequence_by"],
    stored_as_scd_type=tables["silver_airports"]["cdc_configuration"]["scd_type"]
)

# -----------------------------------------------------------------------------------------
# Silver Business View
# -----------------------------------------------------------------------------------------
@dlt.table(
    name="silver_business",
    comment=tables["silver_business"]["description"],
    table_properties={"pipelines.reset.allowed": "true"}
)
def silver_business():
    # Read from streaming silver tables
    bookings_df = dlt.read_stream("silver_bookings")
    flights_df = dlt.read_stream("silver_flights")
    passengers_df = dlt.read_stream("silver_passengers")
    airports_df = dlt.read_stream("silver_airports")
    
    # Join tables based on the configuration
    df = bookings_df \
        .join(flights_df, "flight_id", "inner") \
        .join(passengers_df, "passenger_id", "inner") \
        .join(airports_df, "airport_id", "inner")
    
    # Apply transformations if specified
    if "join_configuration" in tables["silver_business"] and "transformations" in tables["silver_business"]["join_configuration"]:
        df = apply_transformations(df, tables["silver_business"]["join_configuration"]["transformations"])
    
    return df