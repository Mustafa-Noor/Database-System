import csv
import os
import json
import bisect
import shutil

BASE_DIR = "databases"
MAX_FILE_ROWS = 10  # Number of rows per file before splitting to new file

def create_database(db_name):
    """Creates a new database (folder)."""
    db_path = os.path.join(BASE_DIR, db_name)
    
    if os.path.exists(db_path):
        print(f"Database '{db_name}' already exists.")
        return
    
    os.makedirs(os.path.join(db_path, "tables"))
    # Create metadata file
    metadata = {
        "tables": [],
        "indexes": {}
    }
    with open(os.path.join(db_path, "metadata.json"), "w") as meta_file:
        json.dump(metadata, meta_file, indent=4)
    print(f"Database '{db_name}' created successfully.")


def drop_database(db_name):
    """Deletes an entire database."""
    db_path = os.path.join(BASE_DIR, db_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    shutil.rmtree(db_path)
    print(f"Database '{db_name}' deleted successfully.")


def create_table(db_name, table_name, columns):
    """Creates a table with indexed files for all columns inside the database."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    if os.path.exists(table_path):
        print(f"Table '{table_name}' already exists in database '{db_name}'.")
        return
    
    os.makedirs(table_path)
    # Create the first data file
    table_data_file = os.path.join(table_path, "table_data.csv")
    with open(table_data_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # Writing headers
    
    # Create index files for all columns
    for column in columns:
        index_file = os.path.join(table_path, f"{column}_index.csv")
        with open(index_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Value", "RowID"])  # Header for index
    
    # Update metadata with table and indexed columns
    metadata_path = os.path.join(db_path, "metadata.json")
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
    
    metadata["tables"].append(table_name)
    metadata["indexes"][table_name] = columns  # All columns are indexed now
    
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)
    
    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")


def insert_into_table(db_name, table_name, values):
    """Inserts a row into the table and updates all column index files."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist.")
        return
    
    # Check for the number of rows in the current data file
    table_data_file = os.path.join(table_path, "table_data.csv")
    
    with open(table_data_file, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    headers = rows[0]
    if len(values) != len(headers):
        print(f"Error: Value count does not match the column count.")
        return
    
    # Append to the data file
    with open(table_data_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(values)
    
    # Update index files for all columns
    row_id = len(rows)  # Row ID is the current row number
    for column_index, column_name in enumerate(headers):
        index_file = os.path.join(table_path, f"{column_name}_index.csv")
        with open(index_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([values[column_index], row_id])
        
        # Ensure the index file is sorted
        with open(index_file, "r") as f:
            index_data = list(csv.reader(f))
        
        # Skip the header row for sorting
        header = index_data[0]
        index_data = index_data[1:]
        index_data.sort(key=lambda x: x[0])  # Sort by the value column
        
        with open(index_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)  # Write the header back
            writer.writerows(index_data)
    
    print(f"Inserted values {values} into '{table_name}'.")


def search_in_table(db_name, table_name, column_name, search_value):
    """Search for a value in the indexed column."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)
    
    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist.")
        return
    
    index_file = os.path.join(table_path, f"{column_name}_index.csv")
    
    with open(index_file, "r") as f:
        reader = csv.reader(f)
        data = list(reader)
    
    # Convert data to tuples for binary search
    data = [(row[0], row[1]) for row in data[1:]]  # Skip header
    idx = bisect.bisect_left(data, (search_value,))
    
    if idx != len(data) and data[idx][0] == search_value:
        print(f"Found value: {data[idx]}")
    else:
        print("Value not found.")


def update_table(db_name, table_name, search_column, search_value, update_values):
    """Updates rows in the table where the search_column matches the search_value."""
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
    row_ids_to_update = []
    
    # Update rows in memory
    for i, row in enumerate(rows):
        if i == 0:  # Skip header
            updated_rows.append(row)
            continue
        if row[search_index] == search_value:
            row_ids_to_update.append(i)
            updated_row = row[:]
            for column_name, new_value in update_values.items():
                column_index = headers.index(column_name)
                updated_row[column_index] = new_value
            updated_rows.append(updated_row)
        else:
            updated_rows.append(row)
    
    # Write updated rows back to the data file
    with open(table_data_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(updated_rows)
    
    # Update index files
    for column_index, column_name in enumerate(headers):
        index_file = os.path.join(table_path, f"{column_name}_index.csv")
        with open(index_file, "r", newline="") as f:
            index_data = list(csv.reader(f))
        
        header = index_data[0]
        index_data = index_data[1:]
        
        # Update index data
        for row_id in row_ids_to_update:
            for index_row in index_data:
                if int(index_row[1]) == row_id:
                    index_row[0] = updated_rows[row_id][column_index]
        
        # Sort index data
        index_data.sort(key=lambda x: x[0])
        
        # Write updated index data back to the file
        with open(index_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(index_data)
    
    print(f"Updated rows in '{table_name}' where {search_column} = {search_value}.")


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
    
    # Filter rows in memory
    for i, row in enumerate(rows):
        if i == 0:  # Skip header
            updated_rows.append(row)
            continue
        if row[search_index] == search_value:
            row_ids_to_delete.append(i)
        else:
            updated_rows.append(row)
    
    # Write filtered rows back to the data file
    with open(table_data_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(updated_rows)
    
    # Update index files
    for column_index, column_name in enumerate(headers):
        index_file = os.path.join(table_path, f"{column_name}_index.csv")
        with open(index_file, "r", newline="") as f:
            index_data = list(csv.reader(f))
        
        header = index_data[0]
        index_data = index_data[1:]
        
        # Remove index data for deleted rows
        index_data = [row for row in index_data if int(row[1]) not in row_ids_to_delete]
        
        # Write updated index data back to the file
        with open(index_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(index_data)
    
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