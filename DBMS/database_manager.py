import os
import csv
import math

BASE_DIR = "databases"

def create_database(db_name):
    """Creates a new database (folder)."""
    db_path = os.path.join(BASE_DIR, db_name)
    
    if os.path.exists(db_path):
        print(f"Database '{db_name}' already exists.")
        return

    os.makedirs(os.path.join(db_path, "tables"))
    print(f"Database '{db_name}' created successfully.")

def create_table(db_name, table_name, columns):
    """Creates a table with indexed files inside the database."""
    db_path = os.path.join(BASE_DIR, db_name)
    table_path = os.path.join(db_path, "tables", table_name)

    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return
    
    if os.path.exists(table_path):
        print(f"Table '{table_name}' already exists in database '{db_name}'.")
        return

    os.makedirs(table_path)

    # Create the first indexed file (index_1.csv)
    first_index_file = os.path.join(table_path, "index_1.csv")
    with open(first_index_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ID"] + columns)

    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

def get_next_index_file(table_path):
    """Finds the next available index file for inserting data."""
    existing_indexes = [int(f.split("_")[1].split(".")[0]) for f in os.listdir(table_path) if f.startswith("index_")]
    
    if not existing_indexes:
        return os.path.join(table_path, "index_1.csv"), 1

    existing_indexes.sort()
    last_index = existing_indexes[-1]
    last_file = os.path.join(table_path, f"index_{last_index}.csv")

    # Count rows in last index file
    with open(last_file, "r") as f:
        row_count = sum(1 for _ in f) - 1  # Exclude header

    if row_count >= 10:
        next_index = last_index + 1
        return os.path.join(table_path, f"index_{next_index}.csv"), next_index
    return last_file, last_index

def insert_into_table(db_name, table_name, values):
    """Inserts a row into the table's indexed file, ensuring B-Tree organization."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return

    first_index_file = os.path.join(table_path, "index_1.csv")
    
    # Read table headers
    with open(first_index_file, "r") as f:
        reader = csv.reader(f)
        columns = next(reader)

    if len(values) != len(columns) - 1:  # Exclude ID column
        print(f"Error: Expected {len(columns)-1} values, but got {len(values)}.")
        return

    # Determine the next index file
    index_file, index_num = get_next_index_file(table_path)

    # Get new ID
    new_id = sum(1 for _ in open(first_index_file))  # Auto-increment ID

    with open(index_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([new_id] + values)

    print(f"Data inserted into '{table_name}' (Index {index_num}) successfully.")

def show_table(db_name, table_name):
    """Displays all data from the table's indexed files."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return

    index_files = sorted([f for f in os.listdir(table_path) if f.startswith("index_")])
    
    if not index_files:
        print(f"Table '{table_name}' is empty.")
        return

    print("\nTABLE:", table_name)
    print("-" * 50)

    for index_file in index_files:
        with open(os.path.join(table_path, index_file), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)

            if len(rows) < 2:
                continue

            print(" | ".join(rows[0]))  # Header
            print("-" * 50)
            for row in rows[1:]:
                print(" | ".join(row))

    print("\n")


def update_table(db_name, table_name, column, new_value, condition_column, condition_value):
    """Updates records in the table based on a condition."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return

    index_files = sorted([f for f in os.listdir(table_path) if f.startswith("index_")])

    if not index_files:
        print(f"Table '{table_name}' is empty.")
        return

    updated = False

    for index_file in index_files:
        file_path = os.path.join(table_path, index_file)
        
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if len(rows) < 2:
            continue  # Skip empty files

        headers = rows[0]

        if column not in headers or condition_column not in headers:
            print(f"Error: Column '{column}' or '{condition_column}' does not exist.")
            return

        column_index = headers.index(column)
        condition_index = headers.index(condition_column)

        # Update rows that match the condition
        for i in range(1, len(rows)):
            if rows[i][condition_index] == condition_value:
                rows[i][column_index] = new_value
                updated = True

        # Write back the updated content
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    if updated:
        print(f"Records updated successfully in '{table_name}'.")
    else:
        print(f"No matching records found in '{table_name}'.")
