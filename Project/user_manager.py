import os
import json
import struct
from database_manager import create_table, insert_into_table, search_in_table

BASE_DIR = "databases"

def register(username, password):
    """Register a new user by adding a row to the database."""
    # Check if the username already exists
    location = search_in_table('user_database', 'users', "username", username)
    if location != -1:  # If a location is found, the username already exists
        print("Username already exists! Choose a different one.")
        return

    # Insert the new user into the database
    insert_into_table('user_database', 'users', [username, password])
    print(f"User '{username}' registered successfully!")

def sign_in(username, password):
    """Sign in a user by verifying credentials in the database."""
    location = search_in_table('user_database', 'users', "username", username)
    if location == -1:
        print("Username not found!")
        return False

    result = get_row_by_location('user_database', 'users', location)
    if not result:
        print("Error retrieving user data.")
        return False

    # Compare exact strings
    stored_username, stored_password = result
    if stored_username == username and stored_password == password:
        return True

    print("Incorrect password!")
    return False

def get_row_by_location(db_name, table_name, location):
    """Retrieve a row from the table using its location (page ID and row offset)."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    table_data_file = os.path.join(table_path, "table_data.bin")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata for table '{table_name}' does not exist.")
        return None

    # Load metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)

    page_size = metadata["page_size"]
    page_id, row_offset = location

    # Read the row from the binary file
    with open(table_data_file, "rb") as f:
        f.seek(page_id * page_size)  # Seek to the correct page
        row_data = f.read(40)  # Read 40 bytes (20 for username + 20 for password)
        
        # Split into username and password (20 bytes each)
        username = row_data[:20].decode('utf-8').rstrip('\x00')
        password = row_data[20:40].decode('utf-8').rstrip('\x00')
        
        return [username, password]