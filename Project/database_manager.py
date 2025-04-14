import os
import json
import struct
from BTree import BTreeIndex

BASE_DIR = "databases"

def create_table(db_name, table_name, columns, page_size=4096):
    """Creates a table with binary storage and column indexes."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    if os.path.exists(table_path):
        print(f"Table '{table_name}' already exists in database '{db_name}'.")
        return
    
    os.makedirs(table_path)
    
    # Create binary data file
    table_data_file = os.path.join(table_path, "table_data.bin")
    with open(table_data_file, "wb") as f:
        pass  # Create an empty binary file
    
    # Create metadata file
    metadata = {
        "columns": columns,
        "page_size": page_size,
        "indexes": {col: f"{col}_index.btree" for col in columns},
        "pages": 0  # Number of pages in the table
    }
    metadata_file = os.path.join(table_path, "metadata.json")
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=4)
    
    # Create empty B-Tree index files for each column
    for col in columns:
        index_file = os.path.join(table_path, f"{col}_index.btree")
        BTreeIndex(index_file).close()  # Initialize empty B-Tree index
    
    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

import os
import json
import struct

BASE_DIR = "databases"

import struct

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

def delete_from_table(db_name, table_name, column_name, search_value):
    """Delete rows from the table where the column matches the search value."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    table_data_file = os.path.join(table_path, "table_data.bin")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata for table '{table_name}' does not exist.")
        return
    
    # Load metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    
    if column_name not in metadata["columns"]:
        print(f"Error: Column '{column_name}' does not exist in the table.")
        return
    
    # Search for the row to delete
    index_file = os.path.join(table_path, metadata["indexes"][column_name])
    btree_index = BTreeIndex(index_file)
    location = btree_index.search(search_value)
    
    if location is None:
        print(f"Value '{search_value}' not found in column '{column_name}'.")
        return
    
    # Mark the row as deleted in the binary file
    page_id, row_offset = location
    with open(table_data_file, "r+b") as f:
        f.seek(page_id * metadata["page_size"] + row_offset)
        f.write(b"DELETED")  # Mark the row as deleted
    
    # Remove the entry from the column index
    btree_index.delete(search_value)
    btree_index.close()
    
    print(f"Deleted row where {column_name} = '{search_value}'.")

def show_table(db_name, table_name):
    """Display all rows from the table."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    table_data_file = os.path.join(table_path, "table_data.bin")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata for table '{table_name}' does not exist.")
        return
    
    # Load metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    
    page_size = metadata["page_size"]
    num_pages = metadata["pages"]
    
    # Read and display all rows
    with open(table_data_file, "rb") as f:
        for page_id in range(num_pages):
            f.seek(page_id * page_size)
            page_data = f.read(page_size)
            print(f"Page {page_id}: {page_data}")