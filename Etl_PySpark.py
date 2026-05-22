import json
import logging
import os
import re
import traceback
import unicodedata
from datetime import datetime
from functools import reduce

import dateparser
from airflow.hooks.postgres_hook import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import (array, col, concat_ws, countDistinct,
                                   current_timestamp, expr, lit,
                                   regexp_replace, sha2, trim, when)
from pyspark.sql.types import (DateType, DecimalType, DoubleType, FloatType,
                               IntegerType, LongType, NumericType, ShortType,
                               StringType, StructField, StructType)
from sqlalchemy import (Date, DateTime, Float, Integer, String, Text,
                        create_engine, text)
from sqlalchemy.exc import SQLAlchemyError
from word2number import w2n

_ = Integer, Float, String, DateTime, Date

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

ALLOWED_PATTERN = re.compile(
    r"^[a-zA-Z0-9\s.,;:()\-_/£$¥₱₩₦₭₮₨₹₡₢₫₲₴₵₸₺₼₾₿]+$"
)

def strip_currency_symbols(val):
    
    if isinstance(val, str):
        # Remove only leading/trailing currency symbols, not digits or punctuation
        cleaned = re.sub(r'[€£$¥₱₩₦₭₮₨₹₡₢₫₲₴₵₸₺₼₾₿,]', '', val)
        return cleaned.strip()
    return val

def cast_all_columns_to_text(df):
    """
    Cast all columns in a DataFrame to string
    so PostgreSQL will store them as TEXT.
    """
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])
# ---------- safer get_source_data ----------
def detect_id_column(df):
    """
    Dynamically detect a customer ID column.
    Priority:
      1. Exact match 'customer_id'
      2. Any column ending with '_id'
    """
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}

    if "order_id" in lower_map:
        return lower_map["order_id"]

    id_candidates = [c for c in cols if c.lower().endswith("_id")]

    if id_candidates:
        return id_candidates[0]

    raise ValueError("❌ No Order ID column detected")

def get_source_data(spark, jdbc_url, connection_props, folder, execution_date, force_from_raw=True):

    # --- normalize execution_date ---
    if isinstance(execution_date, str):
        execution_date_str = execution_date
        live_date = execution_date.replace("-", "_")
    else:
        execution_date_str = execution_date.strftime("%Y-%m-%d")
        live_date = execution_date.strftime("%Y_%m_%d")

    raw_table_name = f"raw_sales_{live_date}"

    if force_from_raw:
        logging.info(f"Loading data from existing raw table: {raw_table_name} - Etl_PySpark.py:92")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=raw_table_name,
            properties=connection_props
        )

        source_file = f"{raw_table_name} table"
        return df, source_file

    file_list = [f for f in os.listdir(folder) if f.lower().startswith("test_data")]

    if not file_list:
        logging.error(f"No source files found in {folder} for date {execution_date_str} - Etl_PySpark.py:106")
        return spark.createDataFrame([], schema=None), None

    dfs = []

    for file_name in file_list:
        file_path = os.path.join(folder, file_name)

        # Add source_file column
        df_tmp = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("nullValue", "")
            .csv(file_path)
        )

        df_tmp = df_tmp.withColumn("source_file", F.lit(file_name))

        dfs.append(df_tmp)

        logging.info(f"Loaded {file_name} with {df_tmp.count()} rows - Etl_PySpark.py:127")

    # Union all files
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.unionByName(other_df)

    logging.info(f"Total rows combined: {df.count()} - Etl_PySpark.py:134")

    for column in df.columns:
        df = df.withColumn(
            column,
            when(trim(col(column)) == "", None)
            .otherwise(col(column))
        )

    df.write.jdbc(
        url=jdbc_url,
        table=raw_table_name,
        mode="overwrite",  # same as if_exists="replace"
        properties=connection_props
    )

    logging.info(f"✅ Saved raw data to {raw_table_name} as TEXT - Etl_PySpark.py:150")

    return df, file_list[0]

def log_null_values(spark_df, jdbc_url, connection_props, source_file, project_name):

    expected_columns = expected_types.fieldNames()

    # Select only expected business columns (ignore extra raw columns)
    df = spark_df.select(*[c for c in expected_columns if c in spark_df.columns])

    # Add any missing expected columns as NULL
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder columns exactly as expected_types
    df = df.select(*expected_columns)

    null_patterns = ["", "NULL", "null", "none", "NaN", "nan",
                     "Nan", "NA", "N/A", "n/a", "na"]

    for column in expected_columns:
        df = df.withColumn(
            column,
            when(trim(col(column)).isin(null_patterns), None)
            .otherwise(col(column))
        )

    null_condition = reduce(
        lambda a, b: a | b,
        [col(c).isNull() for c in expected_columns]
    )

    null_rows = df.filter(null_condition)

    null_count = null_rows.count()
    logging.info(f"Found {null_count} rows with null values - Etl_PySpark.py:187")

    if null_count == 0:
        logging.info("No null rows found to log. - Etl_PySpark.py:190")
        
        # create empty table structure
        empty_df = spark_df.limit(0)

        table_name = f"dirty_null_values_{datetime.now().strftime('%Y_%m_%d')}"

        empty_df.write \
            .mode("overwrite") \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                properties=connection_props
            )
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

    table_name = f"dirty_null_values_{datetime.now().strftime('%Y_%m_%d')}"

    null_rows.write \
        .mode("overwrite") \
        .option("createTableColumnTypes", 
                "issue VARCHAR(1000), null_columns VARCHAR(1000), source_file VARCHAR(1000), project_name VARCHAR(100), logged_at TIMESTAMP, row_hash VARCHAR(64), " +
                ",".join([f"{c} VARCHAR(1000)" for c in expected_columns])) \
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_props
        )

    logging.warning(f"⚠️ Logged {null_rows.count()} null rows to {table_name} - Etl_PySpark.py:261")
    
def log_negative_values(spark_df, jdbc_url, connection_props, source_file, project_name):

    expected_columns = expected_types.fieldNames()

    # Select only expected business columns
    df = spark_df.select(*[c for c in expected_columns if c in spark_df.columns])

    # Add missing expected columns as NULL
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder exactly as expected schema
    df = df.select(*expected_columns)

    numeric_cols = [
        field.name
        for field in expected_types.fields
        if isinstance(field.dataType, NumericType)
    ]

    if not numeric_cols:
        logging.info("No numeric columns defined in expected_types. - Etl_PySpark.py:285")
        return

    negative_condition = reduce(
        lambda a, b: a | b,
        [F.expr(f"try_cast({c} as double) <= 0") for c in numeric_cols]
    )

    negative_rows = df.filter(negative_condition)

    if negative_rows.limit(1).count() == 0:
        logging.info("No negative rows found. - Etl_PySpark.py:296")
        return

    negative_columns_expr = array(*[
        when(F.expr(f"try_cast({c} as double) <= 0"), lit(c)).otherwise(None)
        for c in numeric_cols
    ])

    negative_rows = negative_rows.withColumn(
        "negative_columns_raw", negative_columns_expr
    )

    negative_rows = negative_rows.withColumn(
        "negative_columns",
        expr("filter(negative_columns_raw, x -> x is not null)")
    ).drop("negative_columns_raw")

    negative_rows = (
        negative_rows
        .withColumn("issue", lit("Negative values found"))
        .withColumn("source_file", lit(source_file))
        .withColumn("project_name", lit(project_name))
        .withColumn("logged_at", current_timestamp())
    )

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
    
    if negative_rows.limit(1).count() > 0:
        table_name = f"dirty_negative_values_{datetime.now().strftime('%Y_%m_%d')}"

        # Cast all columns to string for TEXT storage
        negative_rows = cast_all_columns_to_text(negative_rows)

        # Dynamically generate TEXT types for all columns
        column_types = ", ".join([f"{c} VARCHAR(1000)" for c in negative_rows.columns])

        negative_rows.write \
            .mode("append") \
            .option("createTableColumnTypes", column_types) \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                properties=db_properties
            )
        
        logging.warning(f"⚠️ Logged negative rows to {table_name} as TEXT - Etl_PySpark.py:360")

def detect_region_conflicts(spark_df, source_file):
    expected_columns = expected_types.fieldNames()
    df = spark_df

    # Normalize column names
    for column in df.columns:
        new_col = column.strip()
        new_col = " ".join(new_col.split())
        df = df.withColumnRenamed(column, new_col)

    df = df.toDF(*[c.lower() for c in df.columns])
    expected_lower = [c.lower() for c in expected_columns]

    # Select only expected columns that exist
    df = df.select(*[c for c in expected_lower if c in df.columns])

    # Add missing expected columns
    for col_name in expected_lower:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder strictly
    df = df.select(*expected_lower)

    required = {"customer_name", "region"}
    if not required.issubset(set(df.columns)):
        logging.error(
            f"❌ Missing required columns in {source_file}. Found: {df.columns}"
        )
        return spark_df.sparkSession.createDataFrame([], df.schema)

    if df.limit(1).count() == 0:
        return spark_df.sparkSession.createDataFrame([], df.schema)

    conflicts = (
        df.groupBy("customer_name")
          .agg(countDistinct("region").alias("region_count"))
          .filter(col("region_count") > 1)
    )

    if conflicts.limit(1).count() == 0:
        return spark_df.sparkSession.createDataFrame([], df.schema)

    conflict_rows = df.join(
        conflicts.select("customer_name"),
        on="customer_name",
        how="inner"
    )

    conflict_rows = (
        conflict_rows
        .withColumn("issue", lit("Conflicting Region for Customer"))
        .withColumn("source_file", lit(source_file))
        .withColumn("logged_at", current_timestamp())
    )

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

    # -------------------------------
    # CAST ALL COLUMNS TO TEXT
    # -------------------------------
    conflict_rows = cast_all_columns_to_text(conflict_rows)

    return conflict_rows

REPLACEMENT_CHAR = "\uFFFD"

# Other invisible/special characters
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

def detect_and_clean_special_chars(
    spark_df,
    source_file,
    customer_id_col,
    customer_name_col="Customer_Name",
    skip_columns=None
):

    skip_columns = skip_columns or []
    expected_columns = expected_types.fieldNames()

    df = spark_df.select(*[c for c in expected_columns if c in spark_df.columns])

    # Add missing expected columns
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder strictly
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
            when(
                special_cond, lit(f"Contains special char in {column}")
            ).when(
                edge_cond, lit(f"Invalid leading/trailing char in {column}")
            ).when(
                invalid_cond, lit(f"Invalid characters in {column}")
            )
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
    # ---------- ADD NON-ASCII DETECTION HERE ----------
    non_ascii_pattern = r"[^\x00-\x7F]"

    df_with_issues = df_with_issues.withColumn(
        "special_ascii_issue",
        when(col("Customer_Name").rlike(non_ascii_pattern),
            lit("Non-ASCII character detected in Customer_Name"))
    )

    # Combine with existing issues
    df_with_issues = df_with_issues.withColumn(
        "issue",
        F.concat_ws(" | ", col("issue"), col("special_ascii_issue"))
    )

    bad_df = df_with_issues.filter(col("issue") != "")

    if bad_df.limit(1).count() == 0:
        return df, spark_df.sparkSession.createDataFrame([], df.schema)

    bad_df = (
        bad_df
        .withColumn("source_file", lit(source_file))
        .withColumn("logged_at", current_timestamp())
    )

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

    df_clean = df
    
    # Remove rows with issues from clean df
    df_clean = df_clean.join(
        bad_df.select(customer_id_col),
        on=customer_id_col,
        how="left_anti"
    )

    for column in expected_columns:
        if column in skip_columns:
            continue

        df_clean = df_clean.withColumn(
            column,
            regexp_replace(
                col(column).cast(StringType()),
                special_pattern,
                ""
            )
        )

    return df_clean, bad_df

def validate_and_cast_bigint(df, column_name, source_file=None):
    """
    Detect invalid BIGINT values, log them separately,
    and safely cast valid values.
    """
    # Detect invalid BIGINT values
    dirty_df = df.filter(
        (F.col(column_name).isNotNull()) &
        (~F.col(column_name).rlike("^[0-9]+$"))
    ).withColumn("dirty_column", F.lit(column_name)) \
     .withColumn("dirty_value", F.col(column_name)) \
     .withColumn("error_type", F.lit("INVALID_BIGINT")) \
     .withColumn("source_file", F.lit(source_file))

    # Cast the column to STRING (text)
    clean_df = df.withColumn(column_name, F.col(column_name).cast("string"))

    return clean_df, dirty_df

def detect_typos(
    spark_df,
    expected_types,
    source_file,
    allowed_values=None,
    customer_id_col=None,
    max_distance=2
):
    """
    Detect likely misspelled values using Levenshtein distance.
    Returns dirty dataframe ready for PostgreSQL loading.
    """

    if allowed_values is None:
        allowed_values = {}

    df = spark_df

    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    dirty_dfs = []

    for field in expected_types.fields:
        col_name = field.name

        if isinstance(field.dataType, T.StringType) and col_name in allowed_values:

            valid_values = allowed_values[col_name]

            for valid in valid_values:
                condition = (
                    (F.col(col_name).isNotNull()) &
                    (F.levenshtein(F.lower(F.col(col_name)), F.lit(valid.lower())) <= max_distance) &
                    (F.lower(F.col(col_name)) != F.lit(valid.lower()))
                )

                typo_df = df.filter(condition) \
                    .withColumn("dirty_reason", F.lit("Possible typo")) \
                    .withColumn("column_flagged", F.lit(col_name)) \
                    .withColumn("suggested_value", F.lit(valid)) \
                    .withColumn("source_file", F.lit(source_file)) \
                    .withColumn("logged_at", F.current_timestamp())

                dirty_dfs.append(typo_df)

    if dirty_dfs:
        final_dirty_df = dirty_dfs[0]
        for d in dirty_dfs[1:]:
            final_dirty_df = final_dirty_df.unionByName(d, allowMissingColumns=True)
        return final_dirty_df.dropDuplicates()
    else:
        # Return empty DataFrame with same schema to avoid NoneType errors
        return spark_df.sparkSession.createDataFrame([], spark_df.schema)

def load_dirty_to_postgres(
    spark,
    dirty_df,
    jdbc_url,
    db_properties
):
    """
    Automatically create daily dirty table and append records.
    """

    if dirty_df is None or dirty_df.limit(1).count() == 0:
        print("No dirty records found. - Etl_PySpark.py:681")
        return

    today_str = datetime.now().strftime("%Y_%m_%d")
    table_name = f"dirty_typos_{today_str}"

    try:
        existing_df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=db_properties
        )

        # Align column order
        dirty_df = dirty_df.select(*existing_df.columns)

        write_mode = "append"

    except Exception:
        write_mode = "overwrite"

    dirty_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=write_mode,
        properties=db_properties
    )

    print(f"Loaded dirty records into table: {table_name} - Etl_PySpark.py:709")   
 
def clean_and_capitalize_ids(df, id_columns=None):
    """
    Clean and uppercase selected ID columns.
    Preserves real null values.
    """

    if id_columns is None:
        id_columns = []

    for col in id_columns:
        if col not in df.columns:
            print(f"⚠️ Column '{col}' not found in DataFrame. - Etl_PySpark.py:722")
            continue

        cleaned_col = (
            F.upper(
                F.regexp_replace(
                    F.trim(F.col(col).cast("string")),
                    r"\s+",
                    " "
                )
            )
        )

        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNull() |
                F.lower(F.trim(F.col(col))).isin(
                    "", "null", "none", "nan"
                ),
                F.lit(None)
            ).otherwise(cleaned_col)
        )

        print(f"🧹 Cleaned + UPPERCASE ID column '{col}' - Etl_PySpark.py:746")

    return df

def clean_and_capitalize_strings(df, skip_columns=None):
    """
    Clean and title-case all string columns except skipped ones.
    Preserves real NULL values.
    """

    if skip_columns is None:
        skip_columns = []

    # Identify string columns from Spark schema
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for col in string_columns:
        if col in skip_columns:
            continue

        cleaned_col = F.initcap(
            F.regexp_replace(
                F.trim(F.col(col)),
                r"\s+",
                " "
            )
        )

        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNull() |
                F.lower(F.trim(F.col(col))).isin(
                    "", "null", "none", "nan"
                ),
                F.lit(None)
            ).otherwise(cleaned_col)
        )

    return df

# ✅ 1. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# ✅ 2. Database Connection Info
hook = PostgresHook(postgres_conn_id='postgres_default')  # Or the Conn Id you used
conn = hook.get_connection('postgres_default')

jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"

db_properties = {
    "user": conn.login,
    "password": conn.password,
    "driver": "org.postgresql.Driver"
}

# ✅ 4. Audit Logging Function
def log_audit(hook, username, action, table_name, details):
    try:
        hook.run("""
            INSERT INTO public.audit_log (username, action, table_name, details)
            VALUES (%s, %s, %s, %s)
        """, parameters=(username, action, table_name, details))

    except Exception as e:
        logging.warning(f"⚠️ Audit log failed: {e}")
 
def create_dirty_type_table(hook, table_name, df_sample):
    """
    Create dirty_type table in PostgreSQL if not exists.
    Uses PostgresHook instead of SQLAlchemy engine.
    """

    base_cols = """
        id SERIAL PRIMARY KEY,
        column_flagged TEXT,
        expected_type TEXT,
        issue TEXT,
        source_file TEXT,
        logged_at TIMESTAMP
    """

    metadata_cols = {
        "column_flagged",
        "expected_type",
        "issue",
        "source_file",
        "logged_at"
    }

    for col in df_sample.columns:
        if col not in metadata_cols:
            base_cols += f', "{col}" TEXT'

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({base_cols});
    """

    try:
        hook.run(create_sql)
    except Exception as e:
        logging.warning(f"⚠️ Failed creating dirty type table: {e} - Etl_PySpark.py:853")

def fix_data_types_with_dirty_logging(
    spark, df, expected_types, customer_id_col, jdbc_url, db_properties, source_file
):
    # Keep track of dirty columns per row
    dirty_map_exprs = []

    for field in expected_types.fields:
        col_name = field.name
        dtype = field.dataType

        original_col_name = f"{col_name}_original"
        df = df.withColumn(original_col_name, F.col(col_name))
        original_col = F.col(original_col_name)

        dirty_col = None

        if isinstance(dtype, DateType):
            # Combine all regexes into one pattern using OR (|)
            date_pattern = (
                r"^(?:"
                r"(?:\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))|"        # YYYY-MM-DD
                r"((0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-\d{4})|"          # DD-MM-YYYY
                r"((0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])-\d{4})|"          # MM-DD-YYYY
                r"(?:\d{4}/(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2]))|"        # YYYY/DD/MM
                r"((0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/\d{4})|"          # DD/MM/YYYY
                r"((0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/\d{4})|"          # MM/DD/YYYY
                r"(?:\d{8})"                                               # YYYYMMDD
                r")$"
            )

            dirty_col = F.when(
                original_col.isNotNull() & (~original_col.rlike(date_pattern)),
                F.struct(
                    F.lit(col_name).alias("column"),
                    original_col.cast("string").alias("value")
                )
            )
            
        elif isinstance(dtype, IntegerType):
            cleaned = F.regexp_replace(original_col, r"[$,]", "")
            numeric_pattern = r'^-?\d+$'

            dirty_col = F.when(
                original_col.isNotNull() & (~F.col(col_name).rlike(numeric_pattern)),
                F.struct(
                    F.lit(col_name).alias("column"),
                    original_col.cast("string").alias("value")
                )
            )

        elif isinstance(dtype, (FloatType, DoubleType)):
            cleaned = F.regexp_replace(original_col, r"[$,]", "")
            float_pattern = r'^-?\d+(\.\d+)?([eE][+-]?\d+)?$'

            dirty_col = F.when(
                original_col.isNotNull() & (~cleaned.rlike(float_pattern)),
                F.struct(
                    F.lit(col_name).alias("column"),
                    original_col.cast("string").alias("value")
                )
            )

        elif isinstance(dtype, StringType):
            numeric_pattern = r'^[0-9]+(\.[0-9]+)?$'
            dirty_col = F.when(
                original_col.isNotNull() & F.col(col_name).rlike(numeric_pattern),
                F.struct(
                    F.lit(col_name).alias("column"),
                    original_col.cast("string").alias("value")
                )
            )

        else:
            dirty_col = F.when(
                original_col.isNotNull(),
                F.struct(
                    F.lit(col_name).alias("column"),
                    original_col.cast("string").alias("value")
                )
            )

        dirty_map_exprs.append(dirty_col)

    # Combine all dirty columns
    combined_dirty = F.array(*[c for c in dirty_map_exprs if c is not None])

    dirty_df = (
        df
        .withColumn("combined_dirty", combined_dirty)
        .withColumn("dirty_array", F.expr("filter(combined_dirty, x -> x is not null)"))
        .filter(F.size("dirty_array") > 0)
        .withColumn(
            "dirty_columns",
            F.concat_ws(",", F.expr("transform(dirty_array, x -> x.column)"))
        )
        .withColumn(
            "dirty_values",
            F.concat_ws(",", F.expr("transform(dirty_array, x -> x.value)"))
        )
    )

    # -------------------------------
    # Prepare dirty_df for PostgreSQL
    # -------------------------------
    if dirty_df.limit(1).count() > 0:
        dirty_df = (
            dirty_df
            .withColumn("issue", F.lit("Type conversion failed"))
            .withColumn("source_file", F.lit(source_file))
            .withColumn("logged_at", F.current_timestamp())
        )

        metadata_cols = [
            "issue",
            "source_file",
            "logged_at",
            "dirty_columns",
            "dirty_values"
        ]

        # Explicitly select business columns in expected_types order
        business_cols = [f.name for f in expected_types.fields if f.name in df.columns]

        # Cast everything to string to ensure TEXT storage
        dirty_df = cast_all_columns_to_text(dirty_df)

        # Reorder columns: metadata first, then expected business columns
        dirty_df = dirty_df.select(metadata_cols + business_cols)

        table_name = f"dirty_type_conversion_{datetime.now().strftime('%Y_%m_%d')}"

        dirty_df.write.mode("append").jdbc(
            url=jdbc_url,
            table=table_name,
            properties=db_properties
        )

    # Remove dirty rows from main table
    if dirty_df.limit(1).count() > 0:
        clean_df = df.join(
            dirty_df.select(customer_id_col).distinct(),
            on=customer_id_col,
            how="left_anti"
        )
    else:
        clean_df = df

    # -------------------------------
    # CAST CLEAN DATA TO EXPECTED TYPES
    # -------------------------------
    for field in expected_types.fields:

        col_name = field.name
        dtype = field.dataType

        if col_name in clean_df.columns:

            if isinstance(dtype, DateType):

                clean_df = clean_df.withColumn(
                    col_name,
                    F.expr(f"""
                    coalesce(
                        try_to_date({col_name}, 'yyyy-MM-dd'),
                        try_to_date({col_name}, 'dd-MM-yyyy'),
                        try_to_date({col_name}, 'MM-dd-yyyy'),
                        try_to_date({col_name}, 'yyyy/MM/dd'),
                        try_to_date({col_name}, 'dd/MM/yyyy'),
                        try_to_date({col_name}, 'MM/dd/yyyy'),
                        try_to_date({col_name}, 'yyyyMMdd')
                    )
                    """)
                )

            elif isinstance(dtype, IntegerType):
                clean_df = clean_df.withColumn(
                    col_name,
                    F.expr(f"try_cast({col_name} as INT)")
                )

            elif isinstance(dtype, FloatType):
                clean_df = clean_df.withColumn(
                    col_name,
                    F.expr(f"try_cast({col_name} as FLOAT)")
                )

            elif isinstance(dtype, DoubleType):
                clean_df = clean_df.withColumn(
                    col_name,
                    F.expr(f"try_cast({col_name} as DOUBLE)")
                )

            else:
                clean_df = clean_df.withColumn(
                    col_name,
                    F.col(col_name).cast(dtype)
                )

        else:
            clean_df = clean_df.withColumn(
                col_name,
                F.lit(None).cast(dtype)
            )

    # Reorder columns to match expected schema
    clean_df = clean_df.select(*[f.name for f in expected_types.fields])

    return clean_df
def enforce_expected_schema(df, expected_types):
    """
    Force DataFrame columns to match the expected Spark schema.
    Ensures final clean table has correct datatypes.
    """

    for field in expected_types.fields:
        col_name = field.name
        dtype = field.dataType

        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        else:
            df = df.withColumn(col_name, F.lit(None).cast(dtype))

    # reorder columns
    df = df.select(*[f.name for f in expected_types.fields])

    return df

# ✅ 6. ETL with rollback logic
def run_etl_for_date(spark, execution_date, force_from_raw=True):

    main_table_name = "sales"

    try:
        folder = os.path.join(os.path.dirname(__file__), "Datas")

        # ✅ Correct Spark call signature
        df, source_file = get_source_data(
            spark,
            jdbc_url,
            db_properties,
            folder,
            execution_date,
            force_from_raw=force_from_raw
        )
        customer_id_col = detect_id_column(df)
        df = clean_and_capitalize_strings(df)
        df = clean_and_capitalize_ids(df, id_columns=None)
        original_df = df

        # =====================================================
        # 2️⃣ Invalid Region Detection (Spark Version)
        # =====================================================
        valid_regions = ["West", "East", "South", "North", "Central"]

        expected_columns = expected_types.fieldNames()

        # Enforce expected schema layout
        df = df.select(*[c for c in expected_columns if c in df.columns])

        for col_name in expected_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(None))

        df = df.select(*expected_columns)

        if "Region" in expected_columns:

            invalid_region_df = df.filter(
                (F.col("Region").isNotNull()) &
                (~F.col("Region").isin(valid_regions))
            )

            if invalid_region_df.limit(1).count() > 0:

                invalid_region_df = (
                    invalid_region_df
                    .withColumn("issue", F.lit("Invalid Region Value"))
                    .withColumn("column_flagged", F.lit("Region"))
                    .withColumn("source_file", F.lit(source_file))
                    .withColumn("project_name", F.lit("sales_etl"))
                    .withColumn("logged_at", F.current_timestamp())
                )

                # Stable row_hash (business columns only)
                invalid_region_df = invalid_region_df.withColumn(
                    "row_hash",
                    F.sha2(
                        F.concat_ws("|", *[F.col(c).cast("string") for c in expected_columns]),
                        256
                    )
                )

                invalid_region_df = invalid_region_df.dropDuplicates(["row_hash"])
                # Final strict column order
                metadata_cols = [
                    "issue",
                    "column_flagged",
                    "source_file",
                    "project_name",
                    "logged_at",
                    "row_hash"
                ]

                final_cols = metadata_cols + expected_columns

                invalid_region_df = invalid_region_df.select(*final_cols)

                table_name = f"dirty_invalid_region_{datetime.now().strftime('%Y_%m_%d')}"

                invalid_region_df.write.mode("append").jdbc(
                    url=jdbc_url,
                    table=table_name,
                    properties=db_properties
                )

                # Remove invalid rows from main df
                df = df.filter(
                    (F.col("Region").isNull()) |
                    (F.col("Region").isin(valid_regions))
                )

                logging.warning("⚠️ Invalid region rows removed (schemaaligned) - Etl_PySpark.py:1176")

        # =====================================================
        # 3️⃣ Region Conflict Detection
        # =====================================================
        region_conflicts_df = detect_region_conflicts(df, source_file)

        if region_conflicts_df.limit(1).count() > 0:

            table_name = f"dirty_region_conflicts_{datetime.now().strftime('%Y_%m_%d')}"

            region_conflicts_df.write.mode("append").jdbc(
                url=jdbc_url,
                table=table_name,
                properties=db_properties
            )

            # Remove conflicted customers from main df
            df = df.join(
                region_conflicts_df.select("customer_name"),
                on="customer_name",
                how="left_anti"
            )
        # =====================================================
        # 5️⃣ Fix Data Types (Spark Version)
        # =====================================================
        df = fix_data_types_with_dirty_logging(
            spark,
            df,
            expected_types,
            customer_id_col,
            jdbc_url,
            db_properties,
            source_file
        )

        # =====================================================
        # 6️⃣ Log + Remove Null Rows
        # =====================================================
        log_null_values(df, jdbc_url, db_properties, source_file, "sales_etl")

        df = df.dropna()

        # =====================================================
        # 7️⃣ Log + Remove Negative Values
        # =====================================================
        log_negative_values(df, jdbc_url, db_properties, source_file, "sales_etl")
        numeric_cols = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, NumericType)
        ]

        if numeric_cols:
            condition = None
            for c in numeric_cols:
                cond = F.col(c) <= 0
                condition = cond if condition is None else condition | cond
            df = df.filter(~condition)
        # =====================================================
        # 8️⃣ Special Character Detection
        # =====================================================
        df, bad_chars_df = detect_and_clean_special_chars(
            spark_df=df,
            source_file=source_file,
            customer_id_col=customer_id_col
        )

        if bad_chars_df.limit(1).count() > 0:
            table_name = f"dirty_unknown_chars_{datetime.now().strftime('%Y_%m_%d')}"

            # Cast all columns to string to ensure TEXT storage
            bad_chars_df = cast_all_columns_to_text(bad_chars_df)

            # Generate createTableColumnTypes dynamically with TEXT for all columns
            column_types = ", ".join([f'{c} VARCHAR(1000)' for c in bad_chars_df.columns])
            
            bad_chars_df.write \
                .mode("overwrite") \
                .option("createTableColumnTypes", column_types) \
                .jdbc(
                    url=jdbc_url,
                    table=table_name,
                    properties=db_properties
                )
        # =====================================================
        # 9️⃣ Detect Typos
        # =====================================================
        ALLOWED_VALUES = {
            # "Region": ["North", "South", "East", "West"],
            # "Status": ["Active", "Inactive"]
        }

        dirty_df = detect_typos(
            spark_df=df,
            expected_types=expected_types,
            source_file=source_file,
            allowed_values=ALLOWED_VALUES,
            customer_id_col="Customer_ID"
        )

        load_dirty_to_postgres(
            spark=spark,
            dirty_df=dirty_df,
            jdbc_url=jdbc_url,
            db_properties=db_properties
        )
        
        # =====================================================
        # 🔟 Final Dedup + Clean Load
        # =====================================================
        # Detect primary key dynamically
        primary_key_col = detect_id_column(df)

        # Deduplicate inside Spark using PK only
        df = df.dropDuplicates([primary_key_col])

        # Remove rows that already exist in PostgreSQL
        existing_ids_df = spark.read.jdbc(
            url=jdbc_url,
            table=f'(SELECT "{primary_key_col}" FROM "{main_table_name}") AS existing_ids',
            properties=db_properties
        )

        df = df.join(
            existing_ids_df,
            on=primary_key_col,
            how="left_anti"
        )
        # ✅ Force final schema types
        df = enforce_expected_schema(df, expected_types)
    
        # Write only new rows
        df.write.mode("append").jdbc(
            url=jdbc_url,
            table=main_table_name,
            properties=db_properties
        )

        logging.info(f"✅ Successfully loaded new records into {main_table_name} - Etl_PySpark.py:1314")

        log_audit(hook, "admin", "INSERT", main_table_name, "ETL success")

    except Exception as e:

        logging.error(f"❌ ETL Failed: {e} - Etl_PySpark.py:1320")
        logging.error(traceback.format_exc())

        log_audit(hook, "admin", "ROLLBACK", main_table_name, str(e))
        raise                  