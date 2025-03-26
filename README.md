# Python-Faker

Python-Faker is a data generation tool that uses JSON schema definitions to generate realistic fake data for testing and development purposes. It supports referential integrity, ensuring that foreign key relationships between tables are maintained.

## Features

- Generate fake data based on JSON schema files.
- Supports various data types like `STRING`, `INT`, `BIGINT`, `DECIMAL`, `BOOLEAN`, `DATE`, and `TIMESTAMP`.
- Maintains referential integrity for foreign key relationships.
- Outputs data in `CSV` or `Parquet` formats.
- Handles parent-child table relationships for consistent data generation.

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/Python-Faker.git
   cd Python-Faker
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

### 1. Prepare JSON Schema Files
Place your JSON schema files in the `schema_files` directory. Each schema file should define:
- `table_name`: The name of the table.
- `columns`: A dictionary of column names and their data types.
- `foreign_keys` (optional): A list of foreign key relationships.

Example schema file (`dim_store.json`):
```json
{
    "table_name": "dim_store",
    "columns": {
        "store_key": "BIGINT",
        "store_name": "STRING",
        "store_address": "STRING",
        "store_city": "STRING",
        "store_state": "STRING",
        "store_zip": "STRING",
        "created_date": "TIMESTAMP",
        "updated_date": "TIMESTAMP"
    }
}
```

---

### 2. Generate Fake Data
Run the `generate_fake_data.py` script to generate fake data based on the schema files.

#### Command:
```bash
python src/generate_fake_data.py
```

#### Parameters:
- `schema_dir`: Directory containing the JSON schema files (default: `schema_files`).
- `output_dir`: Directory to save the generated data files (default: `output_files`).
- `num_rows`: Number of rows to generate for each table (default: `10`).
- `output_format`: Format of the output files (`csv` or `parquet`, default: `csv`).

#### Example:
```bash
python src/generate_fake_data.py --schema_dir schema_files --output_dir output_files --num_rows 100 --output_format csv
```

---

### 3. Data Registry for Foreign Keys
The script automatically processes parent tables first and stores their data in a registry. This ensures that child tables can reference valid foreign key values.

---

## Directory Structure

```
Python-Faker/
├── schema_files/          # JSON schema files
│   ├── dim_store.json
│   ├── dim_customer.json
│   └── fact_sales_txn.json
├── output_files/          # Generated data files
│   ├── dim_store.csv
│   ├── dim_customer.csv
│   └── fact_sales_txn.csv
├── src/
│   ├── generate_fake_data.py  # Main script for data generation
│   └── utils.py               # Utility functions (if applicable)
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

---

## Supported Data Types

The following data types are supported in the JSON schema files:
- `STRING`
- `INT`
- `BIGINT`
- `DECIMAL(precision, scale)`
- `BOOLEAN`
- `DATE`
- `TIMESTAMP`
- `FLOAT`

---

## Example Workflow

1. Add your JSON schema files to the `schema_files` directory.
2. Run the `generate_fake_data.py` script to generate fake data.
3. Find the generated data files in the `output_files` directory.

---

## Contributing

Contributions are welcome! If you find a bug or have a feature request, please open an issue or submit a pull request.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.