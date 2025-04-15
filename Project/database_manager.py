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
    """Display all rows from the table in a formatted way."""
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
    
    # Calculate column widths
    col_width = 20  # Fixed width for each column
    total_width = (col_width + 3) * len(columns) - 1
    
    # Print table header
    print(f"\nTable: {table_name}")
    print("-" * total_width)
    header = " | ".join(col.ljust(col_width) for col in columns)
    print(header)
    print("-" * total_width)
    
    # Read and display all rows
    with open(table_data_file, "rb") as f:
        for page_id in range(num_pages):
            f.seek(page_id * page_size)
            page_data = f.read(page_size)
            
            # Process each row in the page
            row_size = col_width * len(columns)
            for i in range(0, len(page_data), row_size):
                row_data = page_data[i:i+row_size]
                if row_data.strip(b'\x00'):  # Skip empty rows
                    # Split row data into fields
                    fields = []
                    for j in range(0, len(row_data), col_width):
                        field = row_data[j:j+col_width].decode('utf-8').rstrip('\x00')
                        fields.append(field)
                    
                    # Skip deleted rows
                    if not any(field.startswith('DELETED') for field in fields):
                        # Format and print the row
                        row = " | ".join(field.ljust(col_width) for field in fields)
                        if row.strip():  # Only print non-empty rows
                            print(row)
    
    print("-" * total_width)

def update_table(db_name, table_name, column, new_value, condition_column, condition_value):
    """Update rows in the table where condition_column equals condition_value."""
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
    if column not in columns or condition_column not in columns:
        print(f"Error: Column does not exist in table.")
        return
    
    # Find the row using the condition
    location = search_in_table(db_name, table_name, condition_column, condition_value)
    if location == -1:
        print(f"No rows found where {condition_column} = '{condition_value}'")
        return
    
    # Get the complete row first
    page_id, _ = location  # Ignore the provided row_offset
    col_index = columns.index(column)
    row_size = 20 * len(columns)  # Each field is 20 bytes
    field_offset = col_index * 20  # Offset within the row for the target column
    
    with open(table_data_file, "r+b") as f:
        # Calculate the exact position of the field we want to update
        field_position = (page_id * metadata["page_size"]) + field_offset
        
        # Seek to the correct field position
        f.seek(field_position)
        
        # Write the new value
        new_value_bytes = new_value.encode('utf-8').ljust(20, b'\x00')
        f.write(new_value_bytes)
    
    # Update the index for the changed column
    if column == condition_column:
        index_file = os.path.join(table_path, metadata["indexes"][column])
        btree_index = BTreeIndex(index_file)
        btree_index.delete(condition_value)
        btree_index.insert(new_value, location)
        btree_index.close()
    
    print(f"Updated row where {condition_column} = '{condition_value}'")
    print(f"Set {column} = '{new_value}'")


def create_database(db_name):
    """Create a new database directory."""
    db_path = os.path.join(BASE_DIR, db_name)
    
    if os.path.exists(db_path):
        print(f"Error: Database '{db_name}' already exists.")
        return False
    
    try:
        os.makedirs(os.path.join(db_path, "tables"))
        print(f"Database '{db_name}' created successfully.")
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def drop_database(db_name):
    """Delete an entire database."""
    db_path = os.path.join(BASE_DIR, db_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return False
    
    try:
        import shutil
        shutil.rmtree(db_path)
        print(f"Database '{db_name}' dropped successfully.")
        return True
    except Exception as e:
        print(f"Error dropping database: {e}")
        return False

def list_databases():
    """List all available databases."""
    if not os.path.exists(BASE_DIR):
        print("No databases found.")
        return []
    
    databases = [d for d in os.listdir(BASE_DIR) 
                if os.path.isdir(os.path.join(BASE_DIR, d))]
    
    if not databases:
        print("No databases found.")
    else:
        print("\nAvailable Databases:")
        for db in databases:
            print(f"- {db}")
    
    return databases

def list_tables(db_name):
    """List all tables in a database."""
    db_path = os.path.join(BASE_DIR, db_name, "tables")
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return []
    
    tables = [t for t in os.listdir(db_path) 
             if os.path.isdir(os.path.join(db_path, t))]
    
    if not tables:
        print(f"No tables found in database '{db_name}'.")
    else:
        print(f"\nTables in database '{db_name}':")
        for table in tables:
            print(f"- {table}")
    
    return tables

def drop_table(db_name, table_name):
    """Delete a table and its associated files."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    
    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return False
    
    try:
        import shutil
        shutil.rmtree(table_path)
        print(f"Table '{table_name}' dropped successfully.")
        return True
    except Exception as e:
        print(f"Error dropping table: {e}")
        return False

def describe_table(db_name, table_name):
    """Show table structure and metadata."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return None
    
    try:
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        
        print(f"\nTable: {table_name}")
        print("Columns:")
        for col in metadata["columns"]:
            print(f"- {col}")
        print(f"\nPage size: {metadata['page_size']} bytes")
        print(f"Number of pages: {metadata['pages']}")
        print("\nIndexes:")
        for col, index_file in metadata["indexes"].items():
            print(f"- {col}: {index_file}")
        
        return metadata
    except Exception as e:
        print(f"Error reading table metadata: {e}")
        return None

def truncate_table(db_name, table_name):
    """Remove all rows from a table while keeping its structure."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    metadata_file = os.path.join(table_path, "metadata.json")
    table_data_file = os.path.join(table_path, "table_data.bin")
    
    if not os.path.exists(metadata_file):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return False
    
    try:
        # Load metadata
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        
        # Clear the data file
        with open(table_data_file, "wb") as f:
            pass
        
        # Reset page count
        metadata["pages"] = 0
        
        # Update metadata
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=4)
        
        # Clear all indexes
        for col in metadata["columns"]:
            index_file = os.path.join(table_path, metadata["indexes"][col])
            BTreeIndex(index_file).close()  # Recreate empty index
        
        print(f"Table '{table_name}' truncated successfully.")
        return True
    except Exception as e:
        print(f"Error truncating table: {e}")
        return False