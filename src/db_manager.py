"""DatabaseManager class for MySQL operations."""

import pandas as pd
from connect import mysql_connection, config, logger
from pathlib import Path

class DatabaseManager:
    def __init__(self, config, data_dir, logger):
        self.config = config
        self.data_dir = data_dir
        self.logger = logger

    def test_connection(self):
        """Test the MySQL connection using the current configuration."""
        try:
            with mysql_connection(self.config) as conn:
                if conn is not None:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    self.logger.info("Database connection test successful!")
                    return True
            self.logger.error("Failed to establish database connection")
            return False
        except Exception as e:
            self.logger.error(f"Error testing database connection: {str(e)}")
            return False

    def import_csv_data(self):
        """Import data from CSV files into the existing database tables."""
        try:
            # Read CSV files
            customers_df = pd.read_csv(self.data_dir / 'customers.csv')
            products_df = pd.read_csv(self.data_dir / 'products.csv')
            orders_df = pd.read_csv(self.data_dir / 'orders.csv')

            # Debug: print/log DataFrame columns to diagnose header issues
            self.logger.info(f"customers_df columns: {list(customers_df.columns)}")
            self.logger.info(f"products_df columns: {list(products_df.columns)}")
            self.logger.info(f"orders_df columns: {list(orders_df.columns)}")

            # Remove rows with any null values before import
            customers_df = customers_df.dropna()
            products_df = products_df.dropna()
            orders_df = orders_df.dropna()

            # Convert datetime string to MySQL datetime format
            orders_df['date_time'] = pd.to_datetime(orders_df['date_time'], utc=True).dt.tz_convert(None)

            with mysql_connection(self.config) as conn:
                if conn is not None:
                    cursor = conn.cursor()

                    # Import customers
                    for _, row in customers_df.iterrows():
                        cursor.execute("""
                            INSERT INTO customers (customer_id, name, email)
                            VALUES (%s, %s, %s)
                            AS new
                            ON DUPLICATE KEY UPDATE
                            name = new.name,
                            email = new.email
                        """, (row['customer_id'], row['name'], row['email']))

                    # Import products
                    for _, row in products_df.iterrows():
                        cursor.execute("""
                            INSERT INTO products (product_id, product_name, price)
                            VALUES (%s, %s, %s)
                            AS new
                            ON DUPLICATE KEY UPDATE
                            product_name = new.product_name,
                            price = new.price
                        """, (row['product_id'], row['product_name'], row['price']))

                    # Import orders
                    for _, row in orders_df.iterrows():
                        cursor.execute("""
                            INSERT INTO orders (order_id, date_time, customer_id, product_id)
                            VALUES (%s, %s, %s, %s)
                            AS new
                            ON DUPLICATE KEY UPDATE
                            date_time = new.date_time,
                            customer_id = new.customer_id,
                            product_id = new.product_id
                        """, (row['order_id'], row['date_time'], row['customer_id'], row['product_id']))

                    conn.commit()
                    self.logger.info("CSV data imported successfully!")
                    return True

                self.logger.error("Failed to establish database connection")
                return False
        except Exception as e:
            self.logger.error(f"Error importing CSV data: {str(e)}")
            return False

    def create_tables(self):
        """Create the necessary database tables, dropping existing ones first."""
        try:
            with mysql_connection(self.config) as conn:
                if conn is not None:
                    cursor = conn.cursor()
                    # Drop existing tables in correct order (due to foreign keys)
                    for table in ["orders", "products", "customers"]:
                        try:
                            cursor.execute(f"DROP TABLE IF EXISTS {table}")
                        except Exception as drop_err:
                            self.logger.warning(f"Warning dropping table {table}: {drop_err}")
                    # Create customers table
                    cursor.execute("""
                        CREATE TABLE customers (
                            customer_id INT PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            email VARCHAR(255) NOT NULL
                        )
                    """)
                    # Create products table
                    cursor.execute("""
                        CREATE TABLE products (
                            product_id INT PRIMARY KEY,
                            product_name VARCHAR(255) NOT NULL,
                            price DECIMAL(10, 5) NOT NULL
                        )
                    """)
                    # Create orders table
                    cursor.execute("""
                        CREATE TABLE orders (
                            order_id INT PRIMARY KEY,
                            date_time DATETIME NOT NULL,
                            customer_id INT,
                            product_id INT,
                            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                            FOREIGN KEY (product_id) REFERENCES products(product_id)
                        )
                    """)
                    conn.commit()
                    self.logger.info("Database tables created successfully!")
                    return True
                self.logger.error("Failed to establish database connection")
                return False
        except Exception as e:
            self.logger.error(f"Error creating tables: {str(e)}")
            return False

    def verify_data(self):
        """Prints a sample of data from each table to verify import."""
        with mysql_connection(self.config) as conn:
            if conn is not None:
                cursor = conn.cursor()
                print("\nCustomers:")
                cursor.execute("SELECT * FROM customers LIMIT 5")
                for row in cursor.fetchall():
                    print(row)
                print("\nProducts:")
                cursor.execute("SELECT * FROM products LIMIT 5")
                for row in cursor.fetchall():
                    print(row)
                print("\nOrders:")
                cursor.execute("SELECT * FROM orders LIMIT 5")
                for row in cursor.fetchall():
                    print(row)
            else:
                print("Failed to connect to database for verification.")
