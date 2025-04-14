import os
from database_manager import create_table, insert_into_table, search_in_table
import csv

BASE_DIR = "databases"

def register(username, password):
    """Register a new user by adding a row to the database."""
    # Check if the username already exists
    row_id = search_in_table('user_database', 'users', "username", username)
    if row_id is not None:  # If a row ID is found, the username already exists
        print("Username already exists! Choose a different one.")
        return

    # Insert the new user into the database
    insert_into_table('user_database', 'users', [username, password])
    print(f"User '{username}' registered successfully!")

def sign_in(username, password):
    """Sign in a user by verifying credentials in the database."""
    # Search for the username in the database
    row_id = search_in_table('user_database', 'users', "username", username)
    if row_id is None:  # If no row ID is found, the username does not exist
        print("Username not found!")
        return False

    # Retrieve the actual row data from the table
    result = get_row_by_id('user_database', 'users', row_id)
    if not result:
        print("Error retrieving user data.")
        return False

    # Verify the password
    if result[0] == username and result[1] == password:
        print(f"Welcome back, {username}!")
        return True

    print("Incorrect password!")
    return False

def get_row_by_id(db_name, table_name, row_id):
    """Retrieve a row from the table using the row ID."""
    table_data_file = os.path.join(BASE_DIR, db_name, "tables", table_name, "table_data.csv")
    
    if not os.path.exists(table_data_file):
        print(f"Error: Data file for table '{table_name}' does not exist.")
        return None
    
    with open(table_data_file, "r") as f:
        reader = list(csv.reader(f))
        if row_id < 1 or row_id >= len(reader):  # Ensure row_id is within valid range
            print(f"Error: Row ID {row_id} is out of range.")
            return None
        return reader[row_id]  # Return the row