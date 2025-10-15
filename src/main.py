"""Main module for database operations."""

import os
from pathlib import Path

import pandas as pd
from connect import config, logger
from db_manager import DatabaseManager

DATA_DIR = Path(__file__).parent.parent / 'data'

if __name__ == "__main__":
	db = DatabaseManager(config, DATA_DIR, logger)
	if db.test_connection():
		if db.create_tables():
			db.import_csv_data()
			db.verify_data()
