# Python U5 Project

This is a MySQL database interaction tool with Python. It creates tables and handles CSV data import into MySQL database tables.

## Setup

1. Clone the repository
2. Create a virtual environment (already done):
   ```bash
   python -m venv .venv
   ```
3. Activate the virtual environment:
   - Windows:
     ```powershell
     .venv\Scripts\activate
     ```
   - Unix/MacOS:
     ```bash
     source .venv/bin/activate
     ```
4. Install required dependencies:
   ```bash
   pip install mysql-connector-python pandas python-dotenv
   ```
5. Create a `.env` file in the project root with your MySQL credentials:
   ```env
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_HOST=127.0.0.1
   DB_NAME=shop
   ```

## Project Structure

```
├── .venv/              # Virtual environment (not in git)
├── data/              # CSV data files
│   ├── customers.csv  # Customer information
│   ├── products.csv   # Product catalog
│   └── orders.csv     # Order records
├── src/               # Source code directory
│   ├── connect.py     # Database connection handling
│   └── main.py       # Main application logic
├── .env              # Environment variables (not in git)
├── .gitignore        # Git ignore file
└── README.md         # This file
```


## DatabaseManager Class

The core logic is encapsulated in the `DatabaseManager` class (`src/db_manager.py`). This class provides a clean interface for all database operations:

- `test_connection()`: Tests the MySQL connection and logs the result.
- `create_tables()`: Drops and recreates the `customers`, `products`, and `orders` tables with the correct schema and foreign keys.
- `import_csv_data()`: Reads the CSV files, removes rows with null values, and imports the data into the database. Handles duplicate keys and data type conversions.
- `verify_data()`: Prints a sample of data from each table to verify successful import.

The class is initialized with the database config, the data directory, and a logger. All database logic is now object-oriented and easy to extend.

## Usage

The project provides functionality to:
1. Connect to MySQL database securely using environment variables
2. Import data from CSV files into existing MySQL tables (Use create functions, if you prefer to make the tables with Python)
3. Handle data with proper error checking and logging

To run the data import:
```bash
python src/main.py
```

The script will:
1. Test the database connection
2. Import data from CSV files into their respective tables:
   - customers.csv → customers table
   - products.csv → products table
   - orders.csv → orders table

All operations are logged to both console and a `cpy-errors.log` file.


### Database Schema

The script automatically creates the following table structure in your MySQL database (no need to pre-create tables):

**customers**
- customer_id (INT, Primary Key)
- name (VARCHAR)
- email (VARCHAR)

**products**
- product_id (INT, Primary Key)
- product_name (VARCHAR)
- price (DECIMAL)

**orders**
- order_id (INT, Primary Key)
- date_time (DATETIME)
- customer_id (INT, Foreign Key to customers.customer_id)
- product_id (INT, Foreign Key to products.product_id)

## Error Handling

- Connection issues are automatically retried with exponential backoff
- All errors are logged to both console and file
- Duplicate records are handled with `ON DUPLICATE KEY UPDATE`
- Data type conversions (e.g., timestamps) are handled automatically

## Dependencies

- mysql-connector-python
- pandas
- python-dotenv


## Author & Credits

**Author:** Andy Sylvia Rosenvold

**Credits:**
- GitHub Copilot (AI pair programmer) - used for tool suggestions, file structure and debugging
- MySQL with Python documentation (https://dev.mysql.com/doc/connector-python/en/ and https://pandas.pydata.org/docs/)

## License

GPL-3.0 license

## Notes

- Uses MySQL 8.0.20+ alias syntax for `ON DUPLICATE KEY UPDATE` (future-proof, no deprecation warnings)

- Tables can be created from Python using the included `create_tables()` function in `main.py` (run the script to drop, create, and import in one go)

