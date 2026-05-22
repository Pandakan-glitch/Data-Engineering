import re
import logging
import argparse

from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, trim, when, countDistinct
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    FloatType, DoubleType,
    DateType, NumericType
)
from functools import reduce as functools_reduce
parser = argparse.ArgumentParser()
parser.add_argument('--kafka-bootstrap-servers', default='kafka:9092')
parser.add_argument('--postgres-url', default='jdbc:postgresql://postgres:5432/Test_Tb')
parser.add_argument('--postgres-user', default='postgres')
parser.add_argument('--postgres-password', default='Mics0123')
parser.add_argument('--topic', default='sales_topic')
args = parser.parse_args()

# Create SparkSession (works in cluster mode too)
spark = (
    SparkSession.builder
    .appName("SalesStreamingETL")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3")
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark-apps/checkpoint")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# 3️⃣ DEFINE SCHEMAS
# =====================================================

raw_schema = StructType([
    StructField("Order_ID", StringType(), True),
    StructField("Customer_Name", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Unit_Price", StringType(), True),
    StructField("Total_Amount", StringType(), True),
    StructField("Payment_Method", StringType(), True)
])

expected_types = StructType([
    StructField("Order_ID", IntegerType(), True),
    StructField("Customer_Name", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Order_Date", DateType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Unit_Price", IntegerType(), True),
    StructField("Total_Amount", IntegerType(), True),
    StructField("Payment_Method", StringType(), True)
])

# =====================================================
# 4️⃣ CREATE STREAMING DATAFRAME
# =====================================================

kafka_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sales_topic")
    .option("startingOffsets", "earliest")
    .load()
)

json_df = kafka_raw_df.selectExpr(
    "CAST(value AS STRING) as json_value"
)

streaming_df = (
    json_df
    .withColumn(
        "data",
        from_json(col("json_value"), raw_schema)
    )
    .select("data.*")
)
# -----------------------------
# Regex pattern
# -----------------------------
ALLOWED_PATTERN = re.compile(
    r"^[a-zA-Z0-9\s.,;:()\-_/£$¥₱₩₦₭₮₨₹₡₢₫₲₴₵₸₺₼₾₿]+$"
)

# -----------------------------
# Cleaning functions
# -----------------------------
def strip_currency_symbols(val):
    if isinstance(val, str):
        cleaned = re.sub(r'[€£$¥₱₩₦₭₮₨₹₡₢₫₲₴₵₸₺₼₾₿,]', '', val)
        return cleaned.strip()
    return val

# UDF version for Spark
strip_currency_udf = F.udf(strip_currency_symbols, StringType())

# -----------------------------
# Detect ID column
# -----------------------------
def detect_id_column(df):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}

    if "order_id" in lower_map:
        return lower_map["order_id"]

    id_candidates = [c for c in cols if c.lower().endswith("_id")]

    if id_candidates:
        return id_candidates[0]

    raise ValueError("❌ No Order ID column detected")

# -----------------------------
# Kafka Source (REAL-TIME)
# -----------------------------
def get_kafka_stream(spark):

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "sales_topic")
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        df_raw
        .selectExpr("CAST(value AS STRING) as json_value")
        .withColumn(
            "data",
            from_json(col("json_value"), raw_schema)
        )
        .select("data.*")
    )
# -----------------------------
# Transformations (your logic reused)
# -----------------------------
def transform_stream(df):

    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                column,
                when(trim(col(column)) == "", None)
                .otherwise(trim(col(column)))
            )

    # Remove currency symbols
    df = df.withColumn("Unit_Price", strip_currency_udf(col("Unit_Price")))
    df = df.withColumn("Total_Amount", strip_currency_udf(col("Total_Amount")))

    # Cast numeric fields
    df = df.withColumn("Quantity", col("Quantity").cast("int"))
    df = df.withColumn("Unit_Price", col("Unit_Price").cast("int"))
    df = df.withColumn("Total_Amount", col("Total_Amount").cast("int"))

    return df

# -----------------------------
# Sink to PostgreSQL (foreachBatch)
# -----------------------------
def write_to_postgres(batch_df, batch_id):

    batch_df.write.jdbc(
        url="jdbc:postgresql://postgres:5432/Test_Tb",
        table="orders",
        mode="append",
        properties={
            "user": "postgres",
            "password": "Mics0123",
            "driver": "org.postgresql.Driver"
        }
    )

def log_null_values_to_kafka(batch_df, batch_id, kafka_bootstrap_servers, topic, source_file, project_name):

    expected_columns = expected_types.fieldNames()

    df = batch_df.select(*[c for c in expected_columns if c in batch_df.columns])

    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    df = df.select(*expected_columns)

    null_patterns = ["", "NULL", "null", "none", "NaN", "nan",
                     "Nan", "NA", "N/A", "n/a", "na"]

    for column, dtype in df.dtypes:

        if dtype == "string":

            df = df.withColumn(
                column,
                when(
                    trim(col(column)).isin(null_patterns),
                    None
                ).otherwise(
                    trim(col(column))
                )
            )

    # Add initial value:
    if expected_columns:
        null_condition = functools_reduce(
            lambda a, b: a | b,
            [col(c).isNull() for c in expected_columns]
        )
    else:
        null_condition = lit(False)

    null_rows = df.filter(null_condition)

    if null_rows.rdd.isEmpty():
        return

    null_columns_expr = array(*[
        when(col(c).isNull(), lit(c)).otherwise(None)
        for c in expected_columns
    ])

    null_rows = null_rows.withColumn("null_columns_raw", null_columns_expr)

    null_rows = null_rows.withColumn(
        "null_columns",
        expr("filter(null_columns_raw, x -> x is not null)")
    ).drop("null_columns_raw")

    null_rows = (
        null_rows
        .withColumn("issue", lit("Null values found"))
        .withColumn("source_file", lit(source_file))
        .withColumn("project_name", lit(project_name))
        .withColumn("logged_at", current_timestamp())
    )

    null_rows = null_rows.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast(StringType()) for c in expected_columns]),
            256
        )
    )

    null_rows = null_rows.dropDuplicates(["row_hash"])

    metadata_cols = [
        "issue",
        "null_columns",
        "source_file",
        "project_name",
        "logged_at",
        "row_hash"
    ]

    final_cols = metadata_cols + expected_columns
    null_rows = null_rows.select(*final_cols)

    # ✅ Convert row to JSON
    kafka_df = null_rows.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # ✅ Write to Kafka
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: log_null_values_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="dirty-null-values",
        source_file="file.csv",
        project_name="my_project"
    )) \
    .start()

def log_negative_values_to_kafka(batch_df, batch_id, kafka_bootstrap_servers, topic, source_file, project_name):

    expected_columns = expected_types.fieldNames()

    # Select expected columns
    df = batch_df.select(*[c for c in expected_columns if c in batch_df.columns])

    # Add missing columns
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder
    df = df.select(*expected_columns)

    # Detect numeric columns
    numeric_cols = [
        field.name
        for field in expected_types.fields
        if isinstance(field.dataType, NumericType)
    ]

    if not numeric_cols:
        return

    # Negative condition
    if numeric_cols:
        negative_condition = functools_reduce(  # ✅ Use 'functools_reduce'
            lambda a, b: a | b,
            [expr(f"try_cast({c} as double) <= 0") for c in numeric_cols]
        )
    else:
        negative_condition = lit(False)

    negative_rows = df.filter(negative_condition)

    # Streaming-safe empty check
    if negative_rows.rdd.isEmpty():
        return

    # Identify negative columns
    negative_columns_expr = array(*[
        when(expr(f"try_cast({c} as double) <= 0"), lit(c)).otherwise(None)
        for c in numeric_cols
    ])

    negative_rows = negative_rows.withColumn(
        "negative_columns_raw", negative_columns_expr
    )

    negative_rows = negative_rows.withColumn(
        "negative_columns",
        expr("filter(negative_columns_raw, x -> x is not null)")
    ).drop("negative_columns_raw")

    # Metadata
    negative_rows = (
        negative_rows
        .withColumn("issue", lit("Negative values found"))
        .withColumn("source_file", lit(source_file))
        .withColumn("project_name", lit(project_name))
        .withColumn("logged_at", current_timestamp())
    )

    # Row hash
    negative_rows = negative_rows.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast(StringType()) for c in expected_columns]),
            256
        )
    )

    negative_rows = negative_rows.dropDuplicates(["row_hash"])

    metadata_cols = [
        "issue",
        "negative_columns",
        "source_file",
        "project_name",
        "logged_at",
        "row_hash"
    ]

    final_cols = metadata_cols + expected_columns
    negative_rows = negative_rows.select(*final_cols)

    # OPTIONAL: keep everything as string for Kafka consistency
    kafka_ready_df = negative_rows.select([
        col(c).cast("string").alias(c) for c in negative_rows.columns
    ])

    # Convert to Kafka format
    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # Write to Kafka
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: log_negative_values_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="dirty-negative-values",
        source_file="file.csv",
        project_name="my_project"
    )) \
    .start()

def detect_region_conflicts_to_kafka(batch_df, batch_id, kafka_bootstrap_servers, topic, source_file):

    expected_columns = expected_types.fieldNames()
    df = batch_df

    # Normalize column names
    for column in df.columns:
        new_col = column.strip()
        new_col = " ".join(new_col.split())
        df = df.withColumnRenamed(column, new_col)

    df = df.toDF(*[c.lower() for c in df.columns])
    expected_lower = [c.lower() for c in expected_columns]

    # Select expected columns
    df = df.select(*[c for c in expected_lower if c in df.columns])

    # Add missing columns
    for col_name in expected_lower:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    df = df.select(*expected_lower)

    required = {"customer_name", "region"}
    if not required.issubset(set(df.columns)):
        return

    if df.rdd.isEmpty():
        return

    # Detect conflicts
    conflicts = (
        df.groupBy("customer_name")
          .agg(countDistinct("region").alias("region_count"))
          .filter(col("region_count") > 1)
    )

    if conflicts.rdd.isEmpty():
        return

    conflict_rows = df.join(
        conflicts.select("customer_name"),
        on="customer_name",
        how="inner"
    )

    # Metadata
    conflict_rows = (
        conflict_rows
        .withColumn("issue", lit("Conflicting Region for Customer"))
        .withColumn("source_file", lit(source_file))
        .withColumn("logged_at", current_timestamp())
    )

    # Row hash
    conflict_rows = conflict_rows.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast(StringType()) for c in expected_lower]),
            256
        )
    )

    conflict_rows = conflict_rows.dropDuplicates(["row_hash"])

    metadata_cols = [
        "issue",
        "source_file",
        "logged_at",
        "row_hash"
    ]

    final_cols = metadata_cols + expected_lower
    conflict_rows = conflict_rows.select(*final_cols)

    # Cast everything to string (Kafka safe)
    kafka_ready_df = conflict_rows.select([
        col(c).cast("string").alias(c) for c in conflict_rows.columns
    ])

    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: detect_region_conflicts_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="dirty-region-conflicts",
        source_file="file.csv"
    )) \
    .start()

REPLACEMENT_CHAR = "\uFFFD"

SPECIAL_CHARS = {
    "\ufeff": "BOM/zero-width no-break space",
    "\u200b": "zero-width space",
    "\u200c": "zero-width non-joiner",
    "\u200d": "zero-width joiner",
    "\u2060": "word joiner",
    "\u202e": "right-to-left override",
    "\u202a": "left-to-right embedding",
    "\u202b": "right-to-left embedding",
    "\u00a0": "non-breaking space",
}

ALL_REPLACEMENT_CHARS = {REPLACEMENT_CHAR} | set(SPECIAL_CHARS.keys())

def detect_special_chars_to_kafka(
    batch_df,
    batch_id,
    kafka_bootstrap_servers,
    topic,
    source_file,
    customer_id_col,
    customer_name_col="Customer_Name",
    skip_columns=None
):

    skip_columns = skip_columns or []
    expected_columns = expected_types.fieldNames()

    df = batch_df.select(*[c for c in expected_columns if c in batch_df.columns])

    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    df = df.select(*expected_columns)

    allowed_pattern = r"^(?:[a-zA-Z0-9\s.,;:()_/]|(?<!\s)-(?!\s))+$"
    edge_pattern = r"^[^a-zA-Z0-9]|[^a-zA-Z0-9]$"
    special_pattern = "[" + "".join(ALL_REPLACEMENT_CHARS) + "]"

    issue_exprs = []
    bad_value_exprs = []

    for column in expected_columns:

        if column in skip_columns:
            continue

        string_col = col(column).cast(StringType())

        special_cond = string_col.rlike(special_pattern)
        edge_cond = string_col.rlike(edge_pattern)
        invalid_cond = ~string_col.rlike(allowed_pattern)

        any_issue = special_cond | edge_cond | invalid_cond

        issue_exprs.append(
            when(special_cond, lit(f"Contains special char in {column}"))
            .when(edge_cond, lit(f"Invalid leading/trailing char in {column}"))
            .when(invalid_cond, lit(f"Invalid characters in {column}"))
        )

        bad_value_exprs.append(
            when(any_issue, string_col)
        )

    df_with_issues = (
        df
        .withColumn("issue_array", array(*issue_exprs))
        .withColumn("bad_value_array", array(*bad_value_exprs))
        .withColumn("issue", expr("concat_ws(' | ', filter(issue_array, x -> x is not null))"))
        .withColumn("bad_value", expr("concat_ws(' | ', filter(bad_value_array, x -> x is not null))"))
    )

    non_ascii_pattern = r"[^\x00-\x7F]"

    df_with_issues = df_with_issues.withColumn(
        "special_ascii_issue",
        when(col(customer_name_col).rlike(non_ascii_pattern),
             lit("Non-ASCII character detected in Customer_Name"))
    )

    df_with_issues = df_with_issues.withColumn(
        "issue",
        concat_ws(" | ", col("issue"), col("special_ascii_issue"))
    )

    bad_df = df_with_issues.filter(col("issue") != "")

    if bad_df.rdd.isEmpty():
        return

    bad_df = (
        bad_df
        .withColumn("source_file", lit(source_file))
        .withColumn("logged_at", current_timestamp())
    )

    # Row hash
    bad_df = bad_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast(StringType()) for c in expected_columns]),
            256
        )
    )

    bad_df = bad_df.dropDuplicates(["row_hash"])

    metadata_cols = [
        "issue",
        "bad_value",
        "source_file",
        "logged_at",
        "row_hash"
    ]

    final_cols = metadata_cols + expected_columns
    bad_df = bad_df.select(*final_cols)

    # Cast all to string (Kafka-safe)
    kafka_ready_df = bad_df.select([
        col(c).cast("string").alias(c) for c in bad_df.columns
    ])

    # Convert to Kafka format
    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # Write to Kafka
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: detect_special_chars_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="dirty-special-chars",
        source_file="file.csv",
        customer_id_col="Order_ID"
    )) \
    .start()

def validate_bigint_to_kafka(
    batch_df,
    batch_id,
    kafka_bootstrap_servers,
    topic,
    column_name,
    source_file=None
):

    # -------------------------------
    # Detect invalid BIGINT values
    # -------------------------------
    dirty_df = batch_df.filter(
        (col(column_name).isNotNull()) &
        (~col(column_name).rlike("^[0-9]+$"))
    ).withColumn("dirty_column", lit(column_name)) \
     .withColumn("dirty_value", col(column_name)) \
     .withColumn("error_type", lit("INVALID_BIGINT")) \
     .withColumn("source_file", lit(source_file)) \
     .withColumn("logged_at", current_timestamp())

    # If nothing dirty → exit
    if dirty_df.rdd.isEmpty():
        return

    # -------------------------------
    # Row hash for dedup
    # -------------------------------
    dirty_df = dirty_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", col(column_name).cast("string"), col("source_file")),
            256
        )
    ).dropDuplicates(["row_hash"])

    # -------------------------------
    # Select final columns
    # -------------------------------
    kafka_ready_df = dirty_df.select([
        col(c).cast("string").alias(c) for c in dirty_df.columns
    ])

    # -------------------------------
    # Kafka format
    # -------------------------------
    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    # -------------------------------
    # Write to Kafka
    # -------------------------------
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: validate_bigint_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="dirty-bigint-values",
        column_name="Order_ID",
        source_file="file.csv"
    )) \
    .start()

def detect_typos_to_kafka(
    spark_df,
    batch_id,
    kafka_bootstrap_servers,
    topic,
    expected_types,
    source_file,
    allowed_values=None,
    customer_id_col=None,
    max_distance=2
):

    if allowed_values is None:
        allowed_values = {}

    df = spark_df

    # Normalize column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    dirty_dfs = []

    for field in expected_types.fields:
        col_name = field.name

        if isinstance(field.dataType, StringType) and col_name in allowed_values:

            valid_values = allowed_values[col_name]

            for valid in valid_values:

                condition = (
                    (col(col_name).isNotNull()) &
                    (levenshtein(lower(col(col_name)), lit(valid.lower())) <= max_distance) &
                    (lower(col(col_name)) != lit(valid.lower()))
                )

                typo_df = df.filter(condition) \
                    .withColumn("dirty_reason", lit("Possible typo")) \
                    .withColumn("column_flagged", lit(col_name)) \
                    .withColumn("suggested_value", lit(valid)) \
                    .withColumn("source_file", lit(source_file)) \
                    .withColumn("logged_at", current_timestamp())

                dirty_dfs.append(typo_df)

    if not dirty_dfs:
        return

    # Union all typo detections
    final_dirty_df = dirty_dfs[0]
    for d in dirty_dfs[1:]:
        final_dirty_df = final_dirty_df.unionByName(d, allowMissingColumns=True)

    final_dirty_df = final_dirty_df.dropDuplicates()

    # -------------------------------
    # Row hash for dedup in Kafka
    # -------------------------------
    final_dirty_df = final_dirty_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast(StringType()) for c in final_dirty_df.columns]),
            256
        )
    )

    # -------------------------------
    # Cast all columns to string
    # -------------------------------
    kafka_ready_df = final_dirty_df.select([
        col(c).cast("string").alias(c) for c in final_dirty_df.columns
    ])

    # -------------------------------
    # Kafka format
    # -------------------------------
    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # -------------------------------
    # Write to Kafka
    # -------------------------------
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()
allowed_values = {
    "Region": ["North", "South", "East", "West"],
    "Payment_Method": ["Cash", "Card", "Online"]
}
query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: detect_typos_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="dirty-typos",
        expected_types=expected_types,
        source_file="file.csv",
        allowed_values=allowed_values,
        customer_id_col="Order_ID"
    )) \
    .start()

def clean_and_capitalize_ids_to_kafka(
    spark_df,
    batch_id,
    kafka_bootstrap_servers,
    topic,
    id_columns=None
):

    if id_columns is None:
        id_columns = []

    df = spark_df

    audit_dfs = []

    for col_name in id_columns:

        if col_name not in df.columns:
            continue

        original_col = col(col_name)

        cleaned_col = upper(
            regexp_replace(
                trim(original_col.cast("string")),
                r"\s+",
                " "
            )
        )

        # Detect if change happened
        change_condition = (
            original_col.isNotNull() &
            (original_col.cast("string") != cleaned_col)
        )

        audit_df = df.filter(change_condition) \
            .withColumn("column_name", lit(col_name)) \
            .withColumn("original_value", original_col.cast("string")) \
            .withColumn("cleaned_value", cleaned_col) \
            .withColumn("action", lit("ID_CLEAN_AND_UPPERCASE")) \
            .withColumn("logged_at", current_timestamp())

        audit_dfs.append(audit_df)

        # Apply transformation (clean actual dataframe)
        df = df.withColumn(
            col_name,
            when(
                col(col_name).isNull() |
                lower(trim(col(col_name).cast("string"))).isin("", "null", "none", "nan"),
                lit(None)
            ).otherwise(cleaned_col)
        )

    # -------------------------------
    # If no changes → exit
    # -------------------------------
    if not audit_dfs:
        return

    audit_df = audit_dfs[0]
    for d in audit_dfs[1:]:
        audit_df = audit_df.unionByName(d, allowMissingColumns=True)

    audit_df = audit_df.dropDuplicates()

    # -------------------------------
    # Row hash
    # -------------------------------
    audit_df = audit_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast("string") for c in audit_df.columns]),
            256
        )
    )

    # -------------------------------
    # Kafka format
    # -------------------------------
    kafka_ready_df = audit_df.select([
        col(c).cast("string").alias(c) for c in audit_df.columns
    ])

    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # -------------------------------
    # Write to Kafka
    # -------------------------------
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

    return df

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: clean_and_capitalize_ids_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="id-cleaning-audit",
        id_columns=["Order_ID"]
    )) \
    .start()
    
def clean_and_capitalize_strings_to_kafka(
    spark_df,
    batch_id,
    kafka_bootstrap_servers,
    topic,
    skip_columns=None
):

    if skip_columns is None:
        skip_columns = []

    df = spark_df

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    audit_dfs = []

    for col_name in string_columns:

        if col_name in skip_columns:
            continue

        original_col = col(col_name)

        cleaned_col = initcap(
            regexp_replace(
                trim(original_col),
                r"\s+",
                " "
            )
        )

        # Detect changes
        change_condition = (
            original_col.isNotNull() &
            (lower(trim(original_col)) != lower(trim(cleaned_col)))
        )

        audit_df = df.filter(change_condition) \
            .withColumn("column_name", lit(col_name)) \
            .withColumn("original_value", original_col.cast("string")) \
            .withColumn("cleaned_value", cleaned_col.cast("string")) \
            .withColumn("action", lit("TITLE_CASE_CLEAN")) \
            .withColumn("logged_at", current_timestamp())

        audit_dfs.append(audit_df)

        # Apply transformation
        df = df.withColumn(
            col_name,
            when(
                col(col_name).isNull() |
                lower(trim(col(col_name))).isin("", "null", "none", "nan"),
                lit(None)
            ).otherwise(cleaned_col)
        )

    # -------------------------------
    # If no changes → skip Kafka write
    # -------------------------------
    if not audit_dfs:
        return df

    audit_df = audit_dfs[0]
    for d in audit_dfs[1:]:
        audit_df = audit_df.unionByName(d, allowMissingColumns=True)

    audit_df = audit_df.dropDuplicates()

    # -------------------------------
    # Row hash
    # -------------------------------
    audit_df = audit_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[col(c).cast("string") for c in audit_df.columns]),
            256
        )
    )

    # -------------------------------
    # Kafka formatting
    # -------------------------------
    kafka_ready_df = audit_df.select([
        col(c).cast("string").alias(c) for c in audit_df.columns
    ])

    kafka_df = kafka_ready_df.selectExpr(
        "CAST(row_hash AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

    return df

query = streaming_df.writeStream \
    .foreachBatch(lambda df, batch_id: clean_and_capitalize_strings_to_kafka(
        df,
        batch_id,
        kafka_bootstrap_servers="kafka:9092",
        topic="string-cleaning-audit",
        skip_columns=["description"]
    )) \
    .start()

# =========================
# 1. Logging Setup (unchanged)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =========================
# 2. Kafka Connection Config
# =========================
KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:9092",  # replace with your cluster
    "client_id": "data-quality-streaming",
    "acks": "all",
    "retries": 3,
    "linger_ms": 5
}

POSTGRES_CONFIG = {
    "url": f"jdbc:postgresql://postgres:5432/Test_Tb",
    "properties": {
        "user": "postgres",
        "password": "Mics0123",
        "driver": "org.postgresql.Driver"
    }
}
# Example topic registry (optional but recommended)
KAFKA_TOPICS = {
    "null_values": "dirty-null-values",
    "negatives": "dirty-negative-values",
    "typos": "dirty-typos",
    "special_chars": "dirty-special-chars",
    "region_conflicts": "dirty-region-conflicts",
    "bigint_errors": "dirty-bigint-values",
    "string_cleaning": "string-cleaning-audit"
}

def log_audit_to_kafka(
    username,
    action,
    table_name,
    details,
    kafka_bootstrap_servers,
    topic
):

    try:
        audit_event = {
            "username": username,
            "action": action,
            "table_name": table_name,
            "details": details,
            "logged_at": datetime.now().isoformat()
        }

        # Convert to Spark DataFrame
        audit_df = spark.createDataFrame([audit_event])

        kafka_df = audit_df.selectExpr(
            "CAST(null AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", topic) \
            .save()

    except Exception as e:
        logging.warning(f"⚠️ Kafka audit log failed: {e}")

log_audit_to_kafka(
    username="etl_user",
    action="NULL_DETECTION",
    table_name="customers",
    details="Null values found in customer_name",
    kafka_bootstrap_servers="kafka:9092",
    topic="audit-events"
)
def emit_audit_event(
    kafka_bootstrap_servers,
    topic,
    action,
    table_name,
    status,
    source_file
):
    event = {
        "action": action,
        "table_name": table_name,
        "status": status,
        "source_file": source_file,
        "logged_at": datetime.now().isoformat()
    }

    df = spark.createDataFrame([event])

    kafka_df = df.selectExpr(
        "CAST(null AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

def emit_dirty_type_schema_event(
    table_name,
    columns,
    kafka_bootstrap_servers,
    topic
):

    try:
        schema_event = {
            "event_type": "DIRTY_TYPE_SCHEMA",
            "table_name": table_name,
            "columns": list(columns),
            "logged_at": datetime.now().isoformat()
        }

        df = spark.createDataFrame([schema_event])

        kafka_df = df.selectExpr(
            "CAST(null AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", topic) \
            .save()

    except Exception as e:
        logging.warning(f"⚠️ Schema event failed: {e}")

def fix_data_types_with_kafka_logging(
    spark,
    df,
    expected_types,
    customer_id_col,
    kafka_bootstrap_servers,
    topic,
    source_file
):

    dirty_exprs = []

    for field in expected_types.fields:

        col_name = field.name
        dtype = field.dataType

        original_col = col(col_name)
        df = df.withColumn(f"{col_name}_original", original_col)

        dirty_col = None

        # ---------------- DATE ----------------
        if isinstance(dtype, DateType):

            date_pattern = r"""^(?:
                (?:\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))|
                ((0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-\d{4})|
                ((0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])-\d{4})|
                (?:\d{4}/(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2]))|
                ((0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/\d{4})|
                ((0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/\d{4})|
                (?:\d{8})
            )$"""

            dirty_col = when(
                original_col.isNotNull() & (~original_col.rlike(date_pattern)),
                struct(
                    lit(col_name).alias("column"),
                    original_col.cast("string").alias("value"),
                    lit("INVALID_DATE").alias("error_type")
                )
            )

        # ---------------- INTEGER ----------------
        elif isinstance(dtype, IntegerType):

            numeric_pattern = r'^-?\d+$'

            dirty_col = when(
                original_col.isNotNull() & (~original_col.cast("string").rlike(numeric_pattern)),
                struct(
                    lit(col_name).alias("column"),
                    original_col.cast("string").alias("value"),
                    lit("INVALID_INTEGER").alias("error_type")
                )
            )

        # ---------------- FLOAT / DOUBLE ----------------
        elif isinstance(dtype, (FloatType, DoubleType)):

            float_pattern = r'^-?\d+(\.\d+)?([eE][+-]?\d+)?$'

            dirty_col = when(
                original_col.isNotNull() &
                (~regexp_replace(original_col.cast("string"), r"[$,]", "").rlike(float_pattern)),
                struct(
                    lit(col_name).alias("column"),
                    original_col.cast("string").alias("value"),
                    lit("INVALID_FLOAT").alias("error_type")
                )
            )

        # ---------------- STRING ----------------
        elif isinstance(dtype, StringType):

            numeric_pattern = r'^[0-9]+(\.[0-9]+)?$'

            dirty_col = when(
                original_col.isNotNull() & original_col.cast("string").rlike(numeric_pattern),
                struct(
                    lit(col_name).alias("column"),
                    original_col.cast("string").alias("value"),
                    lit("STRING_NUMERIC_MIX").alias("error_type")
                )
            )

        # ---------------- OTHER TYPES ----------------
        else:
            dirty_col = when(
                original_col.isNotNull(),
                struct(
                    lit(col_name).alias("column"),
                    original_col.cast("string").alias("value"),
                    lit("UNKNOWN_TYPE").alias("error_type")
                )
            )

        dirty_exprs.append(dirty_col)

    # -------------------------------
    # Combine all dirty detections
    # -------------------------------
    dirty_array = array(*dirty_exprs)

    dirty_df = (
        df
        .withColumn("dirty_array", expr("filter(dirty_array, x -> x is not null)"))
        .withColumn("dirty_item", explode("dirty_array"))
        .withColumn("dirty_columns", col("dirty_item.column"))
        .withColumn("dirty_values", col("dirty_item.value"))
        .withColumn("error_type", col("dirty_item.error_type"))
    )

    # -------------------------------
    # If no dirty rows → skip
    # -------------------------------
    if dirty_df.rdd.isEmpty():
        clean_df = df
    else:

        # Add metadata
        dirty_df = dirty_df.withColumn("source_file", lit(source_file)) \
                           .withColumn("logged_at", current_timestamp())

        # Row hash
        dirty_df = dirty_df.withColumn(
            "row_hash",
            sha2(concat_ws("|", *[col(c).cast("string") for c in dirty_df.columns]), 256)
        )

        # Kafka format
        kafka_ready_df = dirty_df.select([
            col(c).cast("string").alias(c) for c in dirty_df.columns
        ])

        kafka_df = kafka_ready_df.selectExpr(
            "CAST(row_hash AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", topic) \
            .save()

        # Remove dirty rows
        clean_df = df.join(
            dirty_df.select(customer_id_col).distinct(),
            on=customer_id_col,
            how="left_anti"
        )

    # -------------------------------
    # TYPE CASTING FOR CLEAN DATA
    # -------------------------------
    for field in expected_types.fields:

        col_name = field.name
        dtype = field.dataType

        if col_name in clean_df.columns:

            if isinstance(dtype, DateType):
                clean_df = clean_df.withColumn(
                    col_name,
                    expr(f"""
                    coalesce(
                        to_date(col(col_name), "yyyy-MM-dd"),
                        to_date(col(col_name), "dd-MM-yyyy"),
                        to_date(col(col_name), "MM-dd-yyyy"),
                        to_date(col(col_name), "yyyy/MM/dd"),
                        to_date(col(col_name), "dd/MM/yyyy"),
                        to_date(col(col_name), "MM/dd/yyyy"),
                        to_date(col(col_name), "yyyyMMdd")
                    )
                    """)
                )

            elif isinstance(dtype, IntegerType):
                clean_df = clean_df.withColumn(col_name, expr(f"try_cast({col_name} as INT)"))

            elif isinstance(dtype, FloatType):
                clean_df = clean_df.withColumn(col_name, expr(f"try_cast({col_name} as FLOAT)"))

            elif isinstance(dtype, DoubleType):
                clean_df = clean_df.withColumn(col_name, expr(f"try_cast({col_name} as DOUBLE)"))

            else:
                clean_df = clean_df.withColumn(col_name, col(col_name).cast(dtype))

        else:
            clean_df = clean_df.withColumn(col_name, lit(None).cast(dtype))

    # Final schema alignment
    clean_df = clean_df.select(*[f.name for f in expected_types.fields])

    return clean_df

def enforce_expected_schema(df, expected_types):

    for field in expected_types.fields:

        col_name = field.name
        dtype = field.dataType

        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
        else:
            df = df.withColumn(col_name, lit(None).cast(dtype))

    # Ensure strict ordering
    df = df.select(*[f.name for f in expected_types.fields])

    return df

def run_streaming_etl_microbatch(
    spark,
    batch_df,
    batch_id,
    expected_types,
    jdbc_url,
    db_properties,
    kafka_bootstrap_servers,
    source_file
):

    main_table_name = "orders"

    try:
        # =====================================================
        # 1️⃣ Basic Cleaning
        # =====================================================
        customer_id_col = detect_id_column(batch_df)

        df = clean_and_capitalize_strings_to_kafka(
            spark_df=batch_df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="string-cleaning-audit",
            skip_columns=["description"]
        )

        df = clean_and_capitalize_ids_to_kafka(
            spark_df=df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="id-cleaning-audit",
            id_columns=["Order_ID"]
        )

        df = enforce_expected_schema(df, expected_types)

        # =====================================================
        # 2️⃣ REGION VALIDATION → KAFKA EVENT
        # =====================================================
        valid_regions = ["West", "East", "South", "North", "Central"]

        if "Region" in df.columns:

            invalid_region_df = df.filter(
                (col("Region").isNotNull()) &
                (~col("Region").isin(valid_regions))
            )

            # write dirty records to Kafka
            if not invalid_region_df.rdd.isEmpty():

                kafka_df = invalid_region_df.selectExpr(
                    "cast(null as string) as key",
                    "to_json(struct(*)) as value"
                )

                kafka_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                    .option("topic", "dirty-region-events") \
                    .save()

            # keep only valid rows
            df = df.filter(
                col("Region").isin(valid_regions) | col("Region").isNull()
            )

        # =====================================================
        # REGION CONFLICT DETECTION → KAFKA
        # =====================================================

        detect_region_conflicts_to_kafka(
            batch_df=df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="dirty-region-conflicts",
            source_file=source_file
        )

        # OPTIONAL: REMOVE CONFLICTING CUSTOMERS

        conflicts = (
            df.groupBy("Customer_Name")
            .agg(countDistinct("Region").alias("region_count"))
            .filter(col("region_count") > 1)
        )

        df = df.join(
            conflicts.select("Customer_Name"),
            on="Customer_Name",
            how="left_anti"
        )
        # =====================================================
        # 4️⃣ TYPE FIXING + TYPE ERRORS → KAFKA
        # =====================================================
        df = fix_data_types_with_kafka_logging(
            spark,
            df,
            expected_types,
            customer_id_col,
            kafka_bootstrap_servers,
            "dirty-type-events",
            source_file
        )

        # =====================================================
        # 5️⃣ NULL + NEGATIVE LOGGING → KAFKA
        # =====================================================
        log_null_values_to_kafka(
            batch_df=df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="dirty-null-values",
            source_file=source_file,
            project_name="sales_etl"
        )
        log_negative_values_to_kafka(
            batch_df=df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="dirty-negative-values",
            source_file=source_file,
            project_name="sales_etl"
        )

        df = df.dropna()

        numeric_cols = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, NumericType)
        ]

        if numeric_cols:
            cond = None
            for c in numeric_cols:
                cond = (col(c) <= 0) if cond is None else (cond | (col(c) <= 0))
            df = df.filter(~cond)

        # =====================================================
        # 6️⃣ SPECIAL CHARACTERS → KAFKA
        # =====================================================
        detect_special_chars_to_kafka(
            batch_df=df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="dirty-special-chars",
            source_file=source_file,
            customer_id_col=customer_id_col,
            customer_name_col="Customer_Name",
            skip_columns=None
        )

        # =====================================================
        # 7️⃣ TYPOS → KAFKA
        # =====================================================
        detect_typos_to_kafka(
            spark_df=df,
            batch_id=batch_id,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="dirty-typos",
            expected_types=expected_types,
            source_file=source_file,
            allowed_values=allowed_values,
            customer_id_col=customer_id_col
        )

        # =====================================================
        # 8️⃣ FINAL CLEAN DATA LOAD
        # =====================================================
        primary_key_col = detect_id_column(df)

        df = df.dropDuplicates([primary_key_col])

        existing_df = spark.read.jdbc(
            url=jdbc_url,
            table=f'(SELECT "{primary_key_col}" FROM orders) AS existing',
            properties=db_properties
        )

        df = df.join(existing_df, on=primary_key_col, how="left_anti")

        df = enforce_expected_schema(df, expected_types)

        df.write.mode("append").jdbc(
            url=jdbc_url,
            table=main_table_name,
            properties=db_properties
        )
        
        # =====================================================
        # 9️⃣ AUDIT EVENT → KAFKA (instead of Postgres)
        # =====================================================
        emit_audit_event(
            kafka_bootstrap_servers,
            "audit-events",
            "INSERT",
            main_table_name,
            "ETL_SUCCESS",
            source_file
        )
    except Exception as e:
        logging.error(f"❌ ETL Failed: {e}")

        import traceback
        traceback.print_exc()

        emit_audit_event(
            kafka_bootstrap_servers,
            "audit-events",
            "ROLLBACK",
            main_table_name,
            str(e),
            source_file
        )

        raise

main_query = (
    streaming_df.writeStream
    .foreachBatch(
        lambda df, batch_id: run_streaming_etl_microbatch(
            spark=spark,
            batch_df=df,
            batch_id=batch_id,
            expected_types=expected_types,
            jdbc_url="jdbc:postgresql://postgres:5432/Test_Tb",
            db_properties={
                "user": "postgres",
                "password": "Mics0123",
                "driver": "org.postgresql.Driver"
            },
            kafka_bootstrap_servers="kafka:9092",
            source_file="sales_stream"
        )
    )
    .outputMode("append")
    .start()
)

main_query.awaitTermination()