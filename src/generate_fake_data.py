import json
import pandas as pd
from faker import Faker
from datetime import datetime
import os
import random

def extract_foreign_keys(schema_file):
    """
    Extracts foreign key relationships from a JSON schema file.

    Args:
        schema_file (str): Path to the JSON schema file.

    Returns:
        dict: A dictionary where keys are column names, and values are dictionaries
              containing 'target_table' and 'fk_name' for foreign keys.
              Returns an empty dictionary if no foreign keys are found.
    """

    foreign_keys = {}
    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
            for column, details in schema.items():
                if isinstance(details, dict) and 'foreign_key' in details:
                    foreign_keys[column] = {
                        'target_table': details['foreign_key']['target_table'],
                        'fk_name': details['foreign_key']['fk_name']
                    }
    except FileNotFoundError:
        print(f"Error: Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in: {schema_file}")
    return foreign_keys

def generate_fake_data(schema_file, num_rows, output_format="csv", output_dir="output_files", data_registry=None):
    """
    Generates fake data based on a JSON schema definition and saves it to a file
    in the specified output directory, using the same name as the JSON file.
    Ensures 'business_date' is within the last 5 years and handles foreign key relationships.

    Args:
        schema_file (str): Path to the JSON schema file containing the table schema.
        num_rows (int): The number of fake data rows to generate.
        output_format (str, optional): The format of the output data.
                                     Either "csv" or "parquet". Defaults to "csv".
        output_dir (str, optional): Directory to save the output file. Defaults to "output_files".
        data_registry (dict, optional): A dictionary to store generated DataFrames for foreign key referencing.
                                       Keys are table names, values are the corresponding DataFrames.

    Returns:
        pandas.DataFrame: A DataFrame containing the generated fake data.
    """

    fake = Faker()
    current_year = datetime.now().year
    start_year = current_year - 5

    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
    except FileNotFoundError:
        print(f"Error: Schema file not found: {schema_file}")
        return None  # Or raise an exception if you prefer

    filename_without_ext = os.path.splitext(os.path.basename(schema_file))[0]
    foreign_keys = extract_foreign_keys(schema_file)
    data = []

    for _ in range(num_rows):
        row = {}
        for column, data_type in schema.items():
            if column == "business_date":
                # Ensure 'business_date' is within the last 5 years
                year = fake.random_int(min=start_year, max=current_year)
                month = fake.random_int(min=1, max=12)
                day = fake.random_int(min=1, max=28)
                row[column] = datetime(year, month, day).strftime('%Y-%m-%d')
            elif column in foreign_keys:
                target_table = foreign_keys[column]['target_table']
                if data_registry and target_table in data_registry and not data_registry[target_table].empty:
                    row[column] = random.choice(data_registry[target_table][foreign_keys[column]['fk_name']].tolist())
                else:
                    row[column] = fake.random_int(min=1, max=1000)  # Assign random FK if missing
            elif data_type == "int":
                row[column] = fake.random_int()
            elif data_type == "bigint":
                row[column] = fake.random_int() * 1000000
            elif data_type == "string":
                row[column] = fake.word()
            elif data_type == "timestamp":
                row[column] = fake.date_time().isoformat()
            elif data_type == "boolean":
                row[column] = fake.boolean()
            elif data_type == "double":
                row[column] = fake.pyfloat(left_digits=5, right_digits=2)
            elif data_type.startswith("decimal"):
                precision, scale = map(int, data_type[8:-1].split(","))
                row[column] = fake.pydecimal(left_digits=precision - scale, right_digits=scale, positive=True)
            elif data_type == "float":
                row[column] = fake.pyfloat(left_digits=5, right_digits=2)
            else:
                row[column] = None

        data.append(row)  # ✅ Ensure data is added to the list



    df = pd.DataFrame(data)

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_filename = os.path.join(output_dir, f"{filename_without_ext}.{output_format.lower()}")

    if output_format.lower() == "csv":
        df.to_csv(output_filename, index=False)
        print(f"Fake data saved to {output_filename}")
    elif output_format.lower() == "parquet":
        df.to_parquet(output_filename, index=False)
        print(f"Fake data saved to {output_filename}")
    else:
        raise ValueError("Invalid output format. Choose either 'csv' or 'parquet'.")

    return df

# Example Usage:
schema_dir = "../schema_files"  # Directory containing JSON schema files
output_dir = "../output_files"  # Directory to save output files
num_rows = 1000
output_format = "parquet"  # or "csv"

# Get a list of all JSON files in the schema directory
json_files = [f for f in os.listdir(schema_dir) if f.endswith(".json")]

# Data Registry to store generated DataFrames for foreign key referencing
data_registry = {}

# Process each JSON file
for json_file in json_files:
    schema_file_path = os.path.join(schema_dir, json_file)
    table_name = os.path.splitext(json_file)[0]  # Use filename as table name
    fake_df = generate_fake_data(schema_file_path, num_rows, output_format, output_dir, data_registry)
    if fake_df is not None:
        data_registry[table_name] = fake_df  # Store the generated DataFrame
        print(f"Processed {json_file}")