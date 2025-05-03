import os
import json
import struct
from BTree import BTreeIndex

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
    

def insert_into_table(db_name, table_name, values):
    """Insert a row into any table and update column indexes."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    table_data_file = os.path.join(table_path, "table_data.bin")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata for table '{table_name}' does not exist.")
        return
    
    # Load metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    
    columns = metadata["columns"]
    page_size = metadata["page_size"]
    num_pages = metadata["pages"]
    
    if len(values) != len(columns):
        print(f"Error: Value count does not match column count.")
        return
    
    # Serialize the row into binary format with proper encoding
    encoded_values = []
    for value in values:
        if isinstance(value, str):
            encoded_value = value.encode('utf-8').ljust(20, b'\x00')
        else:
            encoded_value = str(value).encode('utf-8').ljust(20, b'\x00')
        encoded_values.append(encoded_value)

    row_data = b''.join(encoded_values)
    row_size = len(row_data)
    
    # Check if the row fits into the last page
    with open(table_data_file, "r+b") as f:
        if num_pages == 0 or f.seek((num_pages - 1) * page_size) and f.read(row_size) == b"":
            # Create a new page if necessary
            f.seek(num_pages * page_size)
            f.write(b"\x00" * page_size)  # Initialize the page with empty bytes
            num_pages += 1
            metadata["pages"] = num_pages
        
        # Write the row into the page
        f.seek((num_pages - 1) * page_size)
        f.write(row_data)
    
    # Update metadata
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=4)
    
    # Update column indexes
    for col, value in zip(columns, values):
        index_file = os.path.join(table_path, metadata["indexes"][col])
        btree_index = BTreeIndex(index_file)
        btree_index.insert(value, (num_pages - 1, row_size))  # Store (page ID, row offset)
        btree_index.close()
    
    print(f"Inserted row into '{table_name}': {values}")


def search_in_table(db_name, table_name, column_name, search_value):
    """Search for a value in the indexed column using B-Tree."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata for table '{table_name}' does not exist.")
        return None
    
    # Load metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    
    if column_name not in metadata["columns"]:
        print(f"Error: Column '{column_name}' does not exist in the table.")
        return None
    
    # Search in the column index
    index_file = os.path.join(table_path, metadata["indexes"][column_name])
    btree_index = BTreeIndex(index_file)
    result = btree_index.search(search_value)
    btree_index.close()
    
    if result is None:
        print(f"Value '{search_value}' not found in column '{column_name}'.")
        return None
    
    print(f"Found value '{search_value}' at location: {result}")
    return result