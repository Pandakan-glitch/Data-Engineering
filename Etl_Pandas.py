import json
import logging
import os
import re
import traceback
import unicodedata
from datetime import datetime

import dateparser
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from pandas.api.types import is_string_dtype
from sqlalchemy import Date, DateTime, Float, Integer, String, Text, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from word2number import w2n

# prevent linter from removing these
_ = Integer, Float, String, DateTime, Date

expected_types = {
    "Row ID": "int64",
    "Segment": "object",
    "Country": "object", 
    "Product": "object", 
    "Discount Band": "object", 
    "Units Sold": "float", 
    "Manufacturing Price": "float", 
    "Sale Price": "float", 
    "Gross Sales": "float", 
    "Discounts": "float",  
    "Sales": "float", 
    "COGS": "float", 
    "Profit": "float",
    "Date": "date",
    "Month Number": "int64",
    "Month Name": "object",
    "Year": "int64"
}

# ✅ Global regex for allowed characters — includes currency symbols
ALLOWED_PATTERN = re.compile(
    r"^[a-zA-Z0-9\s.,;:()\-_/£$¥₱₩₦₭₮₨₹₡₢₫₲₴₵₸₺₼₾₿]+$"
)

def strip_currency_symbols(val):
    
    if isinstance(val, str):
        # Remove only leading/trailing currency symbols, not digits or punctuation
        cleaned = re.sub(r'[€£$¥₱₩₦₭₮₨₹₡₢₫₲₴₵₸₺₼₾₿,]', '', val)
        return cleaned.strip()
    return val

def force_text_schema(df):
    """Return a dtype mapping where all columns are TEXT."""
    return {col: Text() for col in df.columns}

# ✅ Build a global unified schema
def build_sqlalchemy_schema(expected_types):
    type_map = {
        "int64": Integer(),
        "float": Float(),
        "float64": Float(),
        "object": String(),
        "string(100)": String(100),
        "datetime64[ns]": DateTime(),
        "date": Date(),
    }

    schema = {col: type_map.get(dtype, String()) for col, dtype in expected_types.items()}

    # Include metadata fields used in all dirty tables
    schema.update({
        "issue": String(),
        "source_file": String(),
        "project_name": String(),
        "logged_at": DateTime(),
        "column_flagged": String(),
        "bad_date_value": String(),
        "null_columns": String(),
        "negative_columns": String(),
        "row_hash": String(),
        "column_name": String(),
        "value": String(),
    })

    return schema

GLOBAL_DTYPE_MAPPING = build_sqlalchemy_schema(expected_types)

# ✅ Helper to align DataFrame columns with schema
BUSINESS_DTYPE_MAPPING = {col: GLOBAL_DTYPE_MAPPING[col] for col in expected_types.keys()}

# ---------- Helper to align DataFrame columns with schema (fixed) ----------
def ensure_schema_alignment(df, mapping, business_only=False):
    """
    Ensure all expected columns exist in df.
    mapping: dict-like mapping of column -> (SQLAlchemy type OR pandas dtype string)
    business_only flag is accepted for compatibility, but we always use the passed mapping.
    """
    # Always use the mapping the caller provided (do not reference an undefined global)
    if mapping is None:
        raise ValueError("mapping must be provided to ensure_schema_alignment")

    # Add any missing columns (as pd.NA) so df.to_sql won't fail on missing columns.
    missing_cols = [c for c in mapping.keys() if c not in df.columns]
    for c in missing_cols:
        df[c] = pd.NA

    # Reorder columns so metadata (if present) appears after business columns if desired
    # (optional) return df with columns ordered as mapping.keys() + rest
    ordered_cols = list(mapping.keys()) + [c for c in df.columns if c not in mapping.keys()]
    df = df.reindex(columns=ordered_cols)

    return df

def align_business_schema(df):
    """Align only with the original business columns (no metadata)."""
    return ensure_schema_alignment(df, BUSINESS_DTYPE_MAPPING, business_only=True)

def align_dirty_schema(df):
    """Align with full schema including metadata columns."""
    return ensure_schema_alignment(df, GLOBAL_DTYPE_MAPPING)

# ---------- safer get_source_data ----------
def get_source_data(engine, folder, execution_date, force_from_raw=True):
    # --- normalize execution_date ---
    if isinstance(execution_date, str):
        execution_date_str = execution_date
        live_date = execution_date.replace("-", "_")
    else:
        execution_date_str = execution_date.strftime("%Y-%m-%d")
        live_date = execution_date.strftime("%Y_%m_%d")

    raw_table_name = f"raw_sales_{live_date}"

    if force_from_raw:
        logging.info(f"Loading data from existing raw table: {raw_table_name}  Etl_Script.py:138 - Etl_Pandas.py:137")
        query = text(f"SELECT * FROM {raw_table_name}")
        df = pd.read_sql(query, engine)
        source_file = f"{raw_table_name} table"
        return df, source_file

    # --- Find CSV file(s) ---
    file_list = [f for f in os.listdir(folder) if f.lower().startswith("financials")]
    if not file_list:
        logging.error(f"No source files found in {folder} for date {execution_date_str}  Etl_Script.py:147 - Etl_Pandas.py:146")
        return pd.DataFrame(), None

    dfs = []
    for file_name in file_list:
        file_path = os.path.join(folder, file_name)
        df_tmp = pd.read_csv(
            file_path,
            dtype=str,  # ✅ Read ALL columns as text
            na_values=["", "NULL", "null", "None", "none", "NaN", "nan", "Nan"],
            keep_default_na=True
        )
        df_tmp["source_file"] = file_name
        dfs.append(df_tmp)
        logging.info(f"Loaded {file_name} with {len(df_tmp)} rows  Etl_Script.py:161 - Etl_Pandas.py:160")

    df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Total rows combined: {len(df)}  Etl_Script.py:164 - Etl_Pandas.py:163")

    # Normalize: collapse whitespace-only to NaN
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # ✅ Keep all columns as TEXT in PostgreSQL
    dtype_mapping = {col: String() for col in df.columns}

    # --- Save raw (replace) with explicit TEXT types ---
    with engine.begin() as conn:
        df.to_sql(
            raw_table_name,
            conn,
            index=False,
            if_exists="replace",
            dtype=dtype_mapping
        )
        logging.info(f"✅ Saved raw data to {raw_table_name} as TEXT (preserving original values)  Etl_Script.py:181 - Etl_Pandas.py:180")

    return df, file_list[0]

# ---------- improved log_null_values ----------
def log_null_values(df, engine, source_file, project_name):
    df_for_check = df.copy()
    obj_cols = df_for_check.select_dtypes(include="object").columns
    df_for_check[obj_cols] = df_for_check[obj_cols].replace(
        [r"^\s*$", "", "NULL", "null", "none", "NaN", "nan", "Nan", "NA", "N/A", "n/a", "na"],
        pd.NA, regex=True
    )

    null_rows = df_for_check[df_for_check.isnull().any(axis=1)].copy()
    logging.info(f"Found {len(null_rows)} rows with null values  Etl_Script.py:195 - Etl_Pandas.py:194")

    if null_rows.empty:
        logging.info("No null rows found to log.  Etl_Script.py:198 - Etl_Pandas.py:197")
        return

    null_rows["null_columns"] = null_rows.apply(
        lambda row: json.dumps([col for col in df_for_check.columns if pd.isna(row[col])]),
        axis=1
    )

    null_rows["issue"] = "Null values found"
    null_rows["source_file"] = source_file
    null_rows["project_name"] = project_name
    null_rows["logged_at"] = pd.Timestamp.utcnow()

    metadata_cols = ["issue", "null_columns", "source_file", "project_name", "logged_at"]
    final_cols = metadata_cols + [c for c in df_for_check.columns if c not in metadata_cols]
    null_rows = null_rows.reset_index(drop=True)
    null_rows["row_hash"] = (null_rows.astype(str).agg("|".join, axis=1).map(hash))
    null_rows = null_rows.drop_duplicates(subset=["row_hash"])

    table_name = f"dirty_null_values_{datetime.now().strftime('%Y_%m_%d')}"
    null_rows = clean_and_capitalize_ids(null_rows, [])
    null_rows  = clean_and_capitalize_strings(null_rows, [])
   
    try:
        null_rows[final_cols].to_sql(
            table_name, engine, if_exists="append", index=False,
            method="multi", chunksize=500, dtype=force_text_schema(null_rows)
        )
        logging.warning(f"⚠️ Logged {len(null_rows)} null rows to {table_name}  Etl_Script.py:226 - Etl_Pandas.py:225")
    except Exception as e:
        logging.error(f"Failed to write null rows to {table_name}: {e}  Etl_Script.py:228 - Etl_Pandas.py:227")

def log_negative_values(df, engine, source_file, project_name):
    numeric_cols = df.select_dtypes(include=["number"]).columns
    negative_rows = df[df[numeric_cols].lt(0).any(axis=1)].copy()

    if not negative_rows.empty:
        negative_rows["negative_columns"] = negative_rows.apply(
            lambda row: [col for col in numeric_cols if pd.notnull(row[col]) and row[col] < 0],
            axis=1
        )

        negative_rows["issue"] = "Negative values found"
        negative_rows["source_file"] = source_file
        negative_rows["project_name"] = project_name
        negative_rows["logged_at"] = datetime.now()

        data_columns = [col for col in df.columns if col not in ["issue", "negative_columns", "source_file", "project_name", "logged_at"]]
        final_cols = ["issue", "negative_columns", "source_file", "project_name", "logged_at"] + data_columns

        live_date = datetime.now().strftime("%Y_%m_%d")
        table_name = f"dirty_negative_values_{live_date}"
        
        
        negative_rows = clean_and_capitalize_ids(negative_rows, [])
        negative_rows  = clean_and_capitalize_strings(negative_rows, [])
        
    
        negative_rows[final_cols].to_sql(
            table_name, engine, if_exists="append", index=False, dtype=force_text_schema(negative_rows)
        )

        logging.warning(f"⚠️ Logged {len(negative_rows)} negative rows to {table_name}  Etl_Script.py:260 - Etl_Pandas.py:259")

def detect_region_conflicts(df, source_file):
    # Normalize headers
    df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)

    required = {"customer_name", "region"}
    if not required.issubset(df.columns):
        logging.error(
            f"❌ Missing required columns in {source_file}. "
            f"Found: {list(df.columns)}"
        )
        return pd.DataFrame()

    if df.empty:
        logging.warning(f"⚠️ No data rows in {source_file}, skipping conflict detection.  Etl_Script.py:275 - Etl_Pandas.py:274")
        return pd.DataFrame()

    # Step 1: Find customers with multiple Regions
    conflicts = (
        df.groupby("customer_name")["region"]
        .nunique()
        .reset_index()
        .query("region > 1")
    )

    if conflicts.empty:
        return pd.DataFrame()

    # Step 2: Get the actual rows for those customers
    conflicted_customers = conflicts["customer_name"].tolist()
    conflict_rows = df[df["customer_name"].isin(conflicted_customers)].copy()

    # Add metadata
    conflict_rows["issue"] = "Conflicting Region for Customer"
    conflict_rows["source_file"] = source_file
    conflict_rows["logged_at"] = datetime.now()

    return conflict_rows

def track_numeric_strings_in_categorical(df, source_file, expected_types, skip_columns=None):
    skip_columns = skip_columns or []
    dirty_records = []

    # Only columns EXPECTED to be text/object
    object_columns = {
        col for col, dtype in expected_types.items()
        if dtype == "object"
    }

    for col in df.columns:
        # Skip if:
        # 1. Column not expected to be object
        # 2. Column explicitly skipped
        if col not in object_columns or col in skip_columns:
            continue

        # Convert to string safely
        series = df[col].astype(str)

        # Match pure numeric strings (e.g. "123", "00045")
        mask = series.str.fullmatch(r"\d+")

        if mask.any():
            subset = df.loc[mask].copy()
            subset["dirty_reason"] = f"Numeric string in categorical column '{col}'"
            subset["row_index"] = subset.index
            dirty_records.append(subset)

            logging.warning(
                f"⚠️ Found {mask.sum()} numeric-string values in '{col}' "
                f""
            )

    if not dirty_records:
        return pd.DataFrame()

    dirty_df = pd.concat(dirty_records, ignore_index=True)
    dirty_df["source_file"] = source_file
    dirty_df["logged_at"] = datetime.now()

    return dirty_df

# Primary replacement character
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

# Combine all for detection
ALL_REPLACEMENT_CHARS = {REPLACEMENT_CHAR} | set(SPECIAL_CHARS.keys())

def detect_and_clean_special_chars(df, source_file, skip_columns=None):
    """
    Detect replacement/special characters in string columns, log them,
    and return a cleaned DataFrame where these characters are removed.
    """
    skip_columns = skip_columns or []
    
    allowed_pattern = re.compile(
        r"^(?:[a-zA-Z0-9\s.,;:()_/]|(?<!\s)-(?!\s))+$"
    )

    # NEW: start/end special character check
    edge_special_pattern = re.compile(r"^[^a-zA-Z0-9]|[^a-zA-Z0-9]$")

    bad_rows = []
    df_clean = df.copy()

    for idx, row in df.iterrows():
        row_issues = []          # collect multiple issues
        row_columns = []         # which columns had problems
        row_bad_values = []      # store bad values

        for col, val in row.items():
            if col in skip_columns or pd.isna(val):
                continue

            s = str(val)

            # Check for special characters
            special_found = [c for c in ALL_REPLACEMENT_CHARS if c in s]
            if special_found:
                reason_list = [SPECIAL_CHARS.get(c, "unknown replacement char") for c in special_found]
                row_issues.append(f"Contains special char(s) in {col}: {', '.join(reason_list)}")
                row_columns.append(col)
                row_bad_values.append(f"{col}: {s}")
                continue
            
             # 2️⃣ NEW: Check for special characters at start or end
            if edge_special_pattern.search(s):
                row_issues.append(
                    f"Invalid leading/trailing character in {col}"
                )
                row_columns.append(col)
                row_bad_values.append(f"{col}: {s}")
                continue

            # Check for unknown characters
            if not allowed_pattern.fullmatch(s):
                row_issues.append(f"Invalid characters in {col}")
                row_columns.append(col)
                row_bad_values.append(f"{col}: {s}")

        # After looping all columns: log ONCE per row
        if row_issues:
            bad_row = row.copy()
            bad_row["issue"] = " | ".join(row_issues)
            bad_row["column_flagged"] = ", ".join(row_columns)
            bad_row["bad_value"] = " | ".join(row_bad_values)
            bad_row["source_file"] = source_file
            bad_row["logged_at"] = datetime.now()
            bad_row["index"] = idx   # IMPORTANT for dropping later
            bad_rows.append(bad_row)


    bad_df = pd.DataFrame(bad_rows) if bad_rows else pd.DataFrame()
    return df_clean, bad_df

ALLOWED_VALUES = {
    #"Ship Mode": {"First Class", "Second Class", "Standard Class", "Same Day"},                                                                             
    #"Region": {"West", "East", "South", "North", "Central"},
    #"Segment": {"Consumer", "Corporate", "Home Office"},
    #"Country": {"United States"},
    #"Category": {"Furniture", "Office Supplies", "Technology"},
    #"Sub-Category": {"Tables", "Art", "Storage", "Bookcases", "Fasteners", "Envelopes", "Appliances", "Accessories", "Paper", "Phones", "Binders", "Copiers", "Supplies", "Labels", "Chairs", "Machines", "Furnishings"}
}

def detect_typos(df, expected_types, allowed_values=ALLOWED_VALUES, min_length=3):
    """
    Detect likely-typo rows for TEXT columns only.

    Uses expected_types (schema-level truth) instead of dataframe dtypes
    to avoid ETL casting issues where everything becomes TEXT.

    Only columns with expected_types[col] == "object" are checked.
    """

    typo_rows = []

    # 1️⃣ Identify columns that SHOULD be text
    text_columns = [
        col for col, dtype in expected_types.items()
        if dtype == "object" and col in df.columns
    ]

    for col in text_columns:
        series = df[col].astype(str).fillna("").str.strip()

        mask = pd.Series(False, index=df.index)
        reasons = []

        # 2️⃣ Allowed values check
        if col in allowed_values and allowed_values[col]:
            allowed_set = {str(v).strip().title() for v in allowed_values[col]}
            mask_allowed = ~series.str.title().isin(allowed_set) & (series != "")
            if mask_allowed.any():
                mask |= mask_allowed
                reasons.append(f"Value not in allowed set for '{col}'")

        # 3️⃣ Minimum length check
        mask_short = series.str.len().lt(min_length) & (series != "")
        if mask_short.any():
            mask |= mask_short
            reasons.append(f"Too short (<{min_length}) in '{col}'")

        # 4️⃣ Double spaces
        mask_spacing = series.str.contains(r"\s{2,}", regex=True)
        if mask_spacing.any():
            mask |= mask_spacing
            reasons.append(f"Contains double spaces in '{col}'")

        # 5️⃣ Suspicious characters
        bad_char_mask = series.str.contains(
            r"[^a-zA-Z0-9\s\.\,\;\:\!\?\(\)\-_/]",
            regex=True
        )
        if bad_char_mask.any():
            mask |= bad_char_mask
            reasons.append(f"Contains suspicious characters in '{col}'")

        # 6️⃣ Collect results
        if mask.any():
            subset = df.loc[mask].copy()
            subset["dirty_reason"] = "; ".join(reasons)
            subset["column_flagged"] = col
            subset["row_index"] = subset.index
            typo_rows.append(subset)

    if not typo_rows:
        return pd.DataFrame()

    typo_df = pd.concat(typo_rows, ignore_index=True, sort=False)
    typo_df["logged_at"] = datetime.now()
    return typo_df.astype(object)

# ---------- function: save to Postgres ----------
def save_typos_to_postgres(engine, typo_df, table_name="typo_log"):
    """
    Creates a typo table in Postgres if not exists, and inserts typo rows.
    """
    try:
        with engine.begin() as conn:
            # Create table if not exists
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    column_name TEXT,
                    value TEXT,
                    issue TEXT,
                    detected_at TIMESTAMP DEFAULT NOW()
                )
            """))

        # Insert typo rows
        if not typo_df.empty:
            typo_df.to_sql(table_name, engine, if_exists="append", index=False)

    except SQLAlchemyError as e:
        logging.error(f"Error saving typo data: {e}")
        raise
# ---------- Dirty dates helpers (drop-in replacement) ----------
def create_dirty_dates_table(engine, table_name, df_sample=None):
    """
    Ensure daily dirty_dates table exists with TEXT-friendly columns
    including the original dataset columns if provided.
    This implementation avoids duplicating metadata columns like source_file.
    """
    # canonical metadata columns used across dirty tables
    metadata_cols = {
        "id", "row_index", "column_flagged", "bad_date_value",
        "issue", "source_file", "project_name", "logged_at"
    }

    base_columns = """
        id SERIAL PRIMARY KEY,
        row_index INT,
        column_flagged TEXT,
        bad_date_value TEXT,
        issue TEXT,
        source_file TEXT,
        project_name TEXT,
        logged_at TIMESTAMP
    """

    if df_sample is not None:
        for col in df_sample.columns:
            # skip columns that are already part of the metadata to avoid duplicates
            if col in metadata_cols:
                continue
            # add remaining columns as TEXT
            base_columns += f', "{col}" TEXT'

    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({base_columns});"

    with engine.begin() as conn:
        conn.execute(text(create_sql))
def parse_strict_date(val, idx=None, col=None, bad_date_rows=None):
    """
    Strict parsing: return pd.Timestamp on success, original text on fail.
    bad_date_rows is a list to append (idx, col, bad_value, reason).
    """
    if bad_date_rows is None:
        bad_date_rows = []

    try:
        text = "" if pd.isna(val) else str(val).strip()
        if text == "" or text.lower() in ["none", "nan", "null"]:
            return pd.NaT

        # if no digit (e.g. "March" or "next week") treat as partial/dynamic
        if not any(ch.isdigit() for ch in text):
            bad_date_rows.append((idx, col, text, "dynamic_or_partial"))
            return text

        # strict formats only
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y"):
            try:
                parsed = datetime.strptime(text, fmt)
                # sanity checks (month/day range)
                if parsed.month < 1 or parsed.month > 12 or parsed.day < 1 or parsed.day > 31:
                    raise ValueError("Invalid month/day")
                return pd.Timestamp(parsed)
            except ValueError:
                continue

        # nothing matched -> log
        bad_date_rows.append((idx, col, text, "unparseable"))
        return text

    except Exception as e:
        bad_date_rows.append((idx, col, str(val), f"error:{e}"))
        return str(val)


def log_dirty_dates(engine, df, original_df, bad_date_rows, source_file, project_name="sales_etl"):
    """
    bad_date_rows: list of tuples (row_index, column_flagged, bad_date_value, reason)
    Writes to table dirty_dates_YYYY_MM_DD (auto created).
    """

    if not bad_date_rows:
        logging.info("✅ No dirty dates to log.  Etl_Script.py:612 - Etl_Pandas.py:611")
        return

    # Always reset index so row_index becomes positional (0..n-1)
    df = df.reset_index(drop=True)
    original_df = original_df.reset_index(drop=True)

    table_name = f"dirty_dates_{datetime.now().strftime('%Y_%m_%d')}"
    create_dirty_dates_table(engine, table_name, df_sample=df)

    rows = []
    for row_index, column_flagged, bad_date_value, reason in bad_date_rows:

        # 🔒 Safe row access: prevent KeyError
        if row_index < 0 or row_index >= len(df):
            logging.error(f"❌ Invalid row_index={row_index}, df length={len(df)}  skipping.  Etl_Script.py:627 - Etl_Pandas.py:626")
            continue

        # Use .iloc (positional), NEVER .loc because loc requires label match
        row_snapshot = df.iloc[row_index].to_dict()

        rows.append({
            **row_snapshot,
            "row_index": row_index,
            "column_flagged": column_flagged,
            "bad_date_value": bad_date_value,
            "issue": reason,
            "source_file": source_file,
            "project_name": project_name,
            "logged_at": datetime.now(),
        })

    dirty_df = pd.DataFrame(rows)

    dtype_map = {
        "row_index": Integer(),
        "column_flagged": String(),
        "bad_date_value": String(),
        "issue": String(),
        "source_file": String(),
        "project_name": String(),
        "logged_at": DateTime(),
        "row_snapshot": Text()
    }

    dirty_df = clean_and_capitalize_ids(dirty_df, [])
    dirty_df  = clean_and_capitalize_strings(dirty_df , [])
    

    with engine.begin() as conn:
        dirty_df.to_sql(table_name, conn, if_exists="append", index=False, dtype=dtype_map)

    logging.warning(f"⚠️ Logged {len(dirty_df)} dirty date rows to table {table_name}  Etl_Script.py:664 - Etl_Pandas.py:663")

def convert_spelled_numbers(val):
    try:
        if isinstance(val, str) and any(c.isalpha() for c in val):  
            return w2n.word_to_num(val)
        return val
    except:
        return val
    
def clean_and_capitalize_ids(df, id_columns=None):
    """
    Clean and uppercase only selected ID columns (e.g., Order ID, Customer ID)
    while preserving real NaN values.
    """
    if id_columns is None:
        id_columns = []  # explicit only — no defaults

    for col in id_columns:
        if col not in df.columns:
            logging.warning(f"⚠️ Column '{col}' not found in DataFrame.  Etl_Script.py:684 - Etl_Pandas.py:683")
            continue

        df[col] = (
            df[col]
            .astype("string")  # ✅ preserves <NA>
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .replace(
                ["", "NULL", "null", "none", "NaN", "nan", "Nan"],
                pd.NA
            )
            .str.upper()   
        )

        logging.info(f"🧹 Cleaned + UPPERCASE ID column '{col}'  Etl_Script.py:699 - Etl_Pandas.py:698")

    return df

def clean_and_capitalize_strings(df, skip_columns=None):
    if skip_columns is None:
        skip_columns = []

    for col in df.select_dtypes(include=["object", "string"]).columns:
        if col in skip_columns:
            continue

        df[col] = (
            df[col]
            .astype("string")  # ✅ preserves <NA>
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .replace(
                ["", "NULL", "null", "none", "NaN", "nan", "Nan"],
                pd.NA
            )
            .str.title()
        )

    return df

# ✅ 1. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# ✅ 2. Database Connection Info
hook = PostgresHook(postgres_conn_id='postgres_default')  # Or the Conn Id you used
engine = hook.get_sqlalchemy_engine()


# ✅ 4. Audit Logging Function
def log_audit(username, action, table_name, details):
    try:
        with engine.connect() as connection:
            connection.execute(text("""
                INSERT INTO public.audit_log (username, action, table_name, details)
                VALUES (:username, :action, :table_name, :details)
            """), {
                'username': username,
                'action': action,
                'table_name': table_name,
                'details': details
            })
    except Exception as e:
        logging.warning(f"⚠️ Audit log failed: {e}")
        
def create_dirty_type_table(engine, table_name, df_sample):
    base_cols = """
        id SERIAL PRIMARY KEY,
        row_index INT,
        column_flagged TEXT,
        expected_type TEXT,
        issue TEXT,
        source_file TEXT,
        logged_at TIMESTAMP
    """

    for col in df_sample.columns:
        if col not in {
            "row_index",
            "column_flagged",
            "expected_type",
            "issue",
            "source_file",
            "logged_at"
        }:
            base_cols += f', "{col}" TEXT'

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({base_cols});
        """))

# ✅ 5. Data Type Fixing Function
def fix_data_types_with_dirty_logging(df, expected_types, engine, source_file):
    """
    Fix data types and log all conversion errors per row as a single entry.
    """
    df = df.reset_index(drop=True)
    dirty_rows = []

    for idx, row in df.iterrows():
        row_dirty = False
        error_details = []

        for col, expected_dtype in expected_types.items():
            if col not in df.columns:
                continue

            val = row[col]

            if pd.isna(val):
                continue

            try:
                # ---- DATE ----
                if expected_dtype in ("date", "datetime64[ns]"):
                    parsed = pd.to_datetime(val, errors="raise")
                    df.at[idx, col] = parsed

                # ---- NUMERIC ----
                elif "int" in expected_dtype or "float" in expected_dtype:
                    clean_val = strip_currency_symbols(val)
                    num = pd.to_numeric(clean_val, errors="raise")
                    df.at[idx, col] = num

                # ---- STRING ----
                else:
                    df.at[idx, col] = str(val)

            except Exception as e:
                row_dirty = True
                error_details.append(f"{col}: Type conversion failed")

        if row_dirty:
            dirty_rows.append({
                "row_index": idx,
                "column_flagged": ", ".join([col.split(':')[0] for col in error_details]),
                "expected_type": ", ".join([expected_types[col.split(':')[0]] for col in error_details]),
                "issue": "; ".join(error_details),
                "source_file": source_file,
                "logged_at": datetime.now(),
                **row.astype(str).to_dict()
            })

    # ---- WRITE DIRTY ROWS ----
    if dirty_rows:
        dirty_df = pd.DataFrame(dirty_rows)
        table_name = f"dirty_type_conversion_{datetime.now().strftime('%Y_%m_%d')}"
        create_dirty_type_table(engine, table_name, df)

        dirty_df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            dtype=force_text_schema(dirty_df)
        )

        logging.warning(
            f"⚠️ Logged {len(dirty_df)} type conversion errors to {table_name}"
        )

        # drop dirty rows from df
        df = df.drop(index=dirty_df["row_index"].astype(int).unique())

    # ---- FINAL CAST (SAFE ONLY) ----
    for col, dtype in expected_types.items():
        if col in df.columns:
            if "int" in dtype:
                df[col] = df[col].astype("Int64")
            elif "float" in dtype:
                df[col] = df[col].astype("Float64")

    return df.reset_index(drop=True)

# ✅ 6. ETL with rollback logic
def run_etl_for_date(execution_date, force_from_raw=True):
    main_table_name = "financials"

    try:
        folder = os.path.join(os.path.dirname(__file__), 'Datas')
        df, source_file = get_source_data(engine, folder, execution_date, force_from_raw=True)
        original_df = df.copy()
        df = df.reset_index(drop=True)
        df = clean_and_capitalize_strings(df, [])
        
        # 🔎 1️⃣ Detect invalid or misspelled region names first
        valid_regions = {"West", "East", "South", "North", "Central"}
        # make an explicit copy to avoid SettingWithCopy issues
        if "region" not in df.columns:
            logging.warning("⚠️ 'region' column missing — skipping region validation.")
        else:
            invalid_region_rows = df[
                ~df["region"].isin(valid_regions) & df["region"].notna()
            ].copy()
            
            if not invalid_region_rows.empty:
                live_date = datetime.now().strftime("%Y_%m_%d")
                invalid_table = f"dirty_invalid_region_{live_date}"

                # use .loc to avoid SettingWithCopyWarning
                invalid_region_rows.loc[:, "issue"] = "Invalid Region Value"
                invalid_region_rows.loc[:, "source_file"] = source_file
                invalid_region_rows.loc[:, "logged_at"] = datetime.now()
               
                invalid_region_rows = clean_and_capitalize_ids(invalid_region_rows, [])
                invalid_region_rows = clean_and_capitalize_strings(invalid_region_rows, [])
                invalid_region_rows.to_sql(
                    invalid_table,
                    engine,
                    if_exists="append",
                    index=False,
                    dtype=force_text_schema(invalid_region_rows)
                )
                df = df.drop(index=invalid_region_rows.index)
                logging.warning(f"⚠️ Logged and removed {len(invalid_region_rows)} rows with invalid region names to {invalid_table}")
        # 🔎 2️⃣ Detect region conflicts (after removing invalid regions)
        region_conflicts_df = detect_region_conflicts(df, source_file)
        if not region_conflicts_df.empty:
            live_date = datetime.now().strftime("%Y_%m_%d")
            conflict_table = f"dirty_region_conflicts_{live_date}"
            region_conflicts_df = clean_and_capitalize_ids(region_conflicts_df, [])
            region_conflicts_df = clean_and_capitalize_strings(region_conflicts_df, [])
            region_conflicts_df.to_sql(
                conflict_table,
                engine,
                if_exists="append",
                index=False,
                dtype=force_text_schema(region_conflicts_df)
            )
            df = df.drop(index=region_conflicts_df.index.unique(), )
            logging.warning(f"⚠️ Logged and removed {len(region_conflicts_df)} rows with region conflicts")
  
        # create bad_date_rows per-run
        bad_date_rows = []   
        # 🔎 Automatically detect date-like columns from expected_types or df content
        date_columns = [
            col for col, dtype in expected_types.items()
            if "date" in dtype.lower() or "datetime" in dtype.lower()
        ]

        # fallback: also detect columns that look like dates (optional)
        if not date_columns:
            possible_dates = []
            for col in df.columns:
                sample_vals = df[col].dropna().astype(str).head(10)
                if sample_vals.str.contains(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}").any():
                    possible_dates.append(col)
            date_columns = possible_dates

        logging.info(f"📅 Auto-detected date columns: {date_columns}")

        # Strict parse each detected date column
        for col in date_columns:
            if col in df.columns:
                df[col] = [
                    parse_strict_date(v, idx=i, col=col, bad_date_rows=bad_date_rows)
                    for i, v in enumerate(df[col])
                ]
                
        # Log them (auto-creates table)
        log_dirty_dates(engine, df, original_df, bad_date_rows, source_file, project_name="sales_etl")
        # clear list (not necessary since local) but if reuse variable later:
        bad_date_rows.clear()

        # 🔢 Auto-convert numeric-like columns (no hardcoded list)
        for col, dtype in expected_types.items():
            if col in df.columns and any(k in dtype for k in ["int", "float"]):
                # Convert spelled-out numbers (e.g., "ten" → 10)
                df[col] = df[col].apply(convert_spelled_numbers)

                # Then safely coerce to numeric (invalid → NaN)
                df[col] = df[col].apply(strip_currency_symbols)

                logging.info(f"🔢 Cleaned numeric column '{col}' (converted spelled numbers & coerced to numeric)")

        # 🧹 Nulls, capitalization, negatives, etc.
        log_null_values(df, engine, source_file, "sales_etl")
        before = len(df)
        # Final strict null enforcement
        string_cols = df.select_dtypes(include=["string", "object"]).columns
        df[string_cols] = df[string_cols].replace(
            ["Nan", "NaN", "nan", "NULL", ""],
            pd.NA
        )
        df = df.dropna(how="any").reset_index(drop=True)

        logging.info(f"🧼 Removed {before - len(df)} rows with null values after logging")

        # 🧹 Step 3️⃣ Log + Remove negative numeric values
        log_negative_values(df, engine, source_file, "sales_etl")
        num_cols = df.select_dtypes(include=["number"]).columns
        before = len(df)
        df = df[~df[num_cols].lt(0).any(axis=1)].reset_index(drop=True)
        logging.info(f"🧼 Removed {before - len(df)} rows with negative numeric values after logging")
        
        # 🔢 Detect numeric strings in string-type columns (DEDICATED TABLE)
        numeric_string_issues_df = track_numeric_strings_in_categorical(df, source_file, expected_types, skip_columns=["order_id"])
        if not numeric_string_issues_df.empty:
            live_date = datetime.now().strftime("%Y_%m_%d")
            numeric_table = f"dirty_numeric_strings_{live_date}"

            # Save dirty rows for review
            numeric_string_issues_df = clean_and_capitalize_ids(numeric_string_issues_df, [])
            numeric_string_issues_df = clean_and_capitalize_strings(numeric_string_issues_df, [])
            numeric_string_issues_df.to_sql(
                numeric_table,
                engine,
                if_exists="append",
                index=False,
                dtype=force_text_schema(numeric_string_issues_df)
            )

            # Drop dirty rows by saved row_index (preferred)
            if "row_index" in numeric_string_issues_df.columns:
                df = df.drop(
                    index=numeric_string_issues_df["row_index"].astype(int).unique(),
                    errors='ignore'
                )
            else:
                # fallback (if the detector didn’t add row_index)
                df = df.drop(index=numeric_string_issues_df.index.unique(), errors='ignore')

            logging.warning(f"⚠️ Logged and removed {len(numeric_string_issues_df)} rows with numeric strings to {numeric_table}")

        df, unknown_chars_df = detect_and_clean_special_chars(df, source_file, skip_columns=["Email", "Website"])
        if not unknown_chars_df.empty:
            live_date = datetime.now().strftime("%Y_%m_%d")
            unknown_chars_table = f"dirty_unknown_chars_{live_date}"
            try:
                unknown_chars_df = clean_and_capitalize_ids(unknown_chars_df, [])
                unknown_chars_df = clean_and_capitalize_strings(unknown_chars_df, [])
                unknown_chars_df.to_sql(
                    unknown_chars_table,
                    engine,
                    if_exists="append",
                    index=False,
                    dtype=force_text_schema(unknown_chars_df)
                )
                
                logging.warning(f"⚠️ Logged {len(unknown_chars_df)} rows with replacement/special characters to {unknown_chars_table}")
            except Exception as e:
                logging.error(f"Failed to write unknown char rows to {unknown_chars_table}: {e}")
                raise
            
            if not unknown_chars_df.empty:
                # Drop rows using original index
                if "index" in unknown_chars_df.columns:
                    drop_idx = unknown_chars_df["index"].astype(int).unique()
                    df = df.drop(index=drop_idx, errors="ignore")

                else:
                    df = df.drop(index=unknown_chars_df.index.unique(), errors="ignore")

                df = df.reset_index(drop=True)

        # 🔎 Detect typos (full rows)
        typo_df = detect_typos(df, expected_types=expected_types, allowed_values=ALLOWED_VALUES)
        if not typo_df.empty:
            live_date = datetime.now().strftime("%Y_%m_%d")
            typo_table = f"dirty_typos_{live_date}"

            try:
                typo_df = clean_and_capitalize_ids(typo_df, [])
                typo_df = clean_and_capitalize_strings(typo_df, [])
                typo_df.to_sql(
                    typo_table,
                    engine,
                    if_exists="append",
                    index=False,
                    dtype=force_text_schema(typo_df)
                )
                logging.warning(f"⚠️ Logged {len(typo_df)} typo rows to {typo_table}")
            except Exception as e:
                logging.error(f"Failed to write typo rows to {typo_table}: {e}")
                # do not stop ETL; keep proceeding but do not drop rows if write failed
                raise

            # Drop dirty rows from main df using the original row_index values
            if "row_index" in typo_df.columns:
                drop_indices = typo_df["row_index"].astype(int).unique()
            else:
                drop_indices = typo_df.index.unique()

            before_len = len(df)
            df = df.drop(index=drop_indices, errors="ignore").reset_index(drop=True)
            logging.info(f"🧹 Removed {before_len - len(df)} typo rows from main df")
            
        # ✅ Continue cleaning + type fixing
        df = fix_data_types_with_dirty_logging(
            df=df,
            expected_types=expected_types,
            engine=engine,
            source_file=source_file
        )

        df = df.dropna().drop_duplicates()
       
        # 🚀 Load to clean table (Business Columns Only)
        with engine.begin() as conn:
            df = clean_and_capitalize_ids(df, [])
            df = align_business_schema(df)
            df.to_sql(
                main_table_name,
                conn,
                if_exists='append',
                index=False,
                dtype=BUSINESS_DTYPE_MAPPING
            )
            logging.info(f"✅ Loaded {len(df)} cleaned rows into {main_table_name}")
            log_audit("admin", "INSERT", main_table_name, f"Loaded {len(df)} rows from CSV")

    except SQLAlchemyError as e:
        logging.error(f"❌ Database error occurred: {e}")
        log_audit("admin", "ROLLBACK", main_table_name, f"Rollback due to error: {e}")
    except Exception as e:
        logging.error(f"❌ ETL job failed: {e}")
        logging.error(traceback.format_exc())
        log_audit("admin", "ERROR", main_table_name, f"ETL failure: {e}")  