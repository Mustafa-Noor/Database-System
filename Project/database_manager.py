import csv
import os
import json
import shutil
from BTree import BTreeIndex

BASE_DIR = "databases"

def create_table(db_name, table_name, columns):
    """Creates a table with a primary key column (id) and B-Tree indexes."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    if os.path.exists(table_path):
        print(f"Table '{table_name}' already exists in database '{db_name}'.")
        return
    
    os.makedirs(table_path)
    table_data_file = os.path.join(table_path, "table_data.csv")
    
    # Add 'id' as the first column
    columns = ["id"] + columns
    with open(table_data_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # Write headers
    
    # Create B-Tree index for each column
    for column in columns:
        index_file = os.path.join(table_path, f"{column}_index.btree")
        BTreeIndex(index_file).close()  # Initialize empty B-Tree index
    
    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

def drop_database(db_name):
    """Deletes an entire database."""
    db_path = os.path.join(BASE_DIR, db_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    shutil.rmtree(db_path)
    print(f"Database '{db_name}' deleted successfully.")

def create_table(db_name, table_name, columns):
    """Creates a table with B-Tree indexes for all columns inside the database."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    if os.path.exists(table_path):
        print(f"Table '{table_name}' already exists in database '{db_name}'.")
        return
    
    os.makedirs(table_path)
    table_data_file = os.path.join(table_path, "table_data.csv")
    with open(table_data_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # Writing headers
    
    for column in columns:
        index_file = os.path.join(table_path, f"{column}_index.btree")
        BTreeIndex(index_file).close()  # Initialize empty B-Tree index
    
    metadata_path = os.path.join(db_path, "metadata.json")
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
    
    metadata["tables"].append(table_name)
    metadata["indexes"][table_name] = columns
    
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)
    
    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

def insert_into_table(db_name, table_name, values):
    """Inserts a row into the table and updates all column B-Tree indexes."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist.")
        return

    table_data_file = os.path.join(table_path, "table_data.csv")

    with open(table_data_file, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    headers = rows[0]
    if len(values) != len(headers):
        print(f"Error: Value count does not match the column count.")
        return

    with open(table_data_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(values)

    row_id = len(rows)  # Row ID is the current row number
    for column_index, column_name in enumerate(headers):
        index_file = os.path.join(table_path, f"{column_name}_index.btree")
        btree_index = BTreeIndex(index_file)
        btree_index.insert(values[column_index], row_id)
        btree_index.close()

    print(f"Inserted values {values} into '{table_name}'.")

def search_in_table(db_name, table_name, column_name, search_value):
    """Search for a value in the indexed column using B-Tree."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist.")
        return

    index_file = os.path.join(table_path, f"{column_name}_index.btree")
    btree_index = BTreeIndex(index_file)
    row_id = btree_index.search(search_value)
    btree_index.close()

    if row_id == -1:
        print("Value not found.")
        return None

    print(f"Found value: {search_value} at RowID: {row_id}")
    return row_id

def delete_from_table(db_name, table_name, search_column, search_value):
    """Deletes rows from the table where the search_column matches the search_value."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist.")
        return

    table_data_file = os.path.join(table_path, "table_data.csv")

    with open(table_data_file, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    headers = rows[0]
    if search_column not in headers:
        print(f"Error: Column '{search_column}' does not exist in the table.")
        return

    search_index = headers.index(search_column)
    updated_rows = []
    row_ids_to_delete = []

    for i, row in enumerate(rows):
        if i == 0:  # Skip header
            updated_rows.append(row)
            continue
        if row[search_index] == search_value:
            row_ids_to_delete.append(i)
        else:
            updated_rows.append(row)

    with open(table_data_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(updated_rows)

    for column_index, column_name in enumerate(headers):
        index_file = os.path.join(table_path, f"{column_name}_index.btree")
        btree_index = BTreeIndex(index_file)
        for row_id in row_ids_to_delete:
            btree_index.delete(rows[row_id][column_index])
        btree_index.close()

    print(f"Deleted rows from '{table_name}' where {search_column} = {search_value}.")

def show_table(db_name, table_name):
    """Display all rows from the table."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist.")
        return
    
    table_data_file = os.path.join(table_path, "table_data.csv")
    with open(table_data_file, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            print(row)