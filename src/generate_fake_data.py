import pandas as pd
from faker import Faker
import random
import json
from datetime import datetime
import os

def extract_foreign_keys(schema):
    """Extract multiple foreign key relationships from schema JSON."""
    foreign_keys = {}
    fk_list = schema.get('foreign_keys', [])  #  Supports multiple FKs

    for fk_info in fk_list:
        target_table = fk_info.get('target_table')
        local_col_nm = fk_info.get('local_col_nm')
        target_col_nm = fk_info.get('target_col_nm')

        if target_table and local_col_nm and target_col_nm:
            foreign_keys[local_col_nm] = {
                'target_table': target_table,
                'target_col_nm': target_col_nm  # Store the actual target column name
            }

    return foreign_keys  #  Returns empty dict if no foreign keys exist

def generate_fake_data(schema_file, num_rows, output_format="csv", output_dir="output_files", data_registry=None):
    """Generates fake data while maintaining referential integrity for foreign keys."""
    fake = Faker()
    current_year = datetime.now().year
    start_year = current_year - 5

    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
            table_name = schema.get('table_name', os.path.splitext(os.path.basename(schema_file))[0])
            columns = schema.get('columns', {})
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Error: Issue with schema file: {schema_file}")
        return None

    foreign_keys = extract_foreign_keys(schema)  #  Supports multiple FKs
    data = []

    for _ in range(num_rows):
        row = {}
        for column, details in columns.items():
            data_type = details['type'] if isinstance(details, dict) and 'type' in details else details
            
            if column in foreign_keys:
                #  Enforce Foreign Key Integrity
                target_table = foreign_keys[column]['target_table']
                target_col_nm = foreign_keys[column]['target_col_nm']

                if data_registry and target_table in data_registry and target_col_nm in data_registry[target_table].columns:
                    row[column] = random.choice(data_registry[target_table][target_col_nm].tolist())  #  Select only from parent data
                else:
                    row[column] = None  #  Fallback if parent data is missing (shouldn't happen)

            elif column == "business_date":
                row[column] = random.randint(start_year * 10000 + 101, current_year * 10000 + 1231)  # YYYYMMDD format

            else:
                row[column] = generate_fake_value(data_type, fake)

        data.append(row)

    df = pd.DataFrame(data)

    if not df.empty:
        os.makedirs(output_dir, exist_ok=True)
        output_filename = os.path.join(output_dir, f"{table_name}.{output_format.lower()}")

        if output_format.lower() == "csv":
            df.to_csv(output_filename, index=False)
        elif output_format.lower() == "parquet":
            df.to_parquet(output_filename, index=False)
        else:
            raise ValueError("Invalid output format. Choose 'csv' or 'parquet'.")

        print(f" Data saved to {output_filename}")

    return df

def generate_fake_value(data_type, fake):
    """Generates a fake value based on the data type."""
    if data_type.lower() == "int":
        return fake.random_int()
    elif data_type.lower() == "bigint":
        return fake.random_int() * 1000000
    elif data_type.lower() == "string":
        return fake.word()
    elif data_type.lower() == "timestamp":
        return fake.date_time()
    elif data_type.lower() == "boolean":
        return fake.boolean()
    elif data_type.lower() == "double":
        return fake.pyfloat(left_digits=5, right_digits=2)
    elif isinstance(data_type.lower(), str) and data_type.lower().startswith("decimal"):
        precision, scale = map(int, data_type.lower()[8:-1].split(","))
        return fake.pydecimal(left_digits=precision - scale, right_digits=scale, positive=True)
    elif data_type.lower() == "float":
        return fake.pyfloat(left_digits=5, right_digits=2)
    return None  

#  Example Usage:
schema_dir = "../schema_files"
output_dir = "../output_files"
num_rows = 10
output_format = "csv"

# Get all JSON schema files
json_files = [f for f in os.listdir(schema_dir) if f.endswith(".json")]

# Data Registry to store DataFrames for FK relationships
data_registry = {}

#  Process Parent Tables First (Enforces Referential Integrity)
for json_file in json_files:
    schema_file_path = os.path.join(schema_dir, json_file)
    with open(schema_file_path, 'r') as f:
        schema = json.load(f)
        table_name = schema.get('table_name', os.path.splitext(json_file)[0])
        if "dim" in table_name.lower():  #  Identify Parent Tables
            fake_df = generate_fake_data(schema_file_path, num_rows, output_format, output_dir, data_registry)
            if fake_df is not None:
                data_registry[table_name] = fake_df  #  Store generated values for FK usage
                print(f" Processed Parent Table: {json_file}")

#  Process Child Tables (Uses Parent Data)
for json_file in json_files:
    schema_file_path = os.path.join(schema_dir, json_file)
    with open(schema_file_path, 'r') as f:
        schema = json.load(f)
        table_name = schema.get('table_name', os.path.splitext(json_file)[0])
        if "fact" in table_name.lower():  #  Identify Fact Tables
            fake_df = generate_fake_data(schema_file_path, num_rows, output_format, output_dir, data_registry)
            if fake_df is not None:
                data_registry[table_name] = fake_df
                print(f" Processed Child Table: {json_file}")
