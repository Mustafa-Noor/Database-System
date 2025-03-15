import os
import csv

BASE_DIR = "databases"

def create_database(db_name):
    """Creates a new database (folder)."""
    db_path = os.path.join(BASE_DIR, db_name)

    if os.path.exists(db_path):
        print(f"Database '{db_name}' already exists.")
        return

    os.makedirs(os.path.join(db_path, "tables"))
    print(f"Database '{db_name}' created successfully.")

def drop_database(db_name):
    """Deletes an entire database."""
    db_path = os.path.join(BASE_DIR, db_name)

    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return

    import shutil
    shutil.rmtree(db_path)
    print(f"Database '{db_name}' deleted successfully.")

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
        writer.writerow(["ID"] + columns)  # ID column is auto-incremented

    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

def drop_table(db_name, table_name):
    """Deletes an entire table from the database."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return

    import shutil
    shutil.rmtree(table_path)
    print(f"Table '{table_name}' deleted successfully from database '{db_name}'.")

def insert_into_table(db_name, table_name, values):
    """Inserts a row into the table."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return

    index_files = sorted([f for f in os.listdir(table_path) if f.startswith("index_")])

    if not index_files:
        print(f"Error: No index file found in table '{table_name}'.")
        return

    file_path = os.path.join(table_path, index_files[-1])

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) < 1:
        print(f"Error: Table '{table_name}' is missing headers.")
        return

    headers = rows[0]

    if len(values) != len(headers) - 1:  # Exclude ID column
        print(f"Error: Expected {len(headers)-1} values but got {len(values)}.")
        return

    new_id = len(rows)  # Simple auto-increment ID

    with open(file_path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([new_id] + values)

    print(f"Inserted values {values} into '{table_name}'.")

def show_table(db_name, table_name):
    """Displays all data from the table."""
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
                continue  # Skip empty files

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
            print(f"Error: One of the columns '{column}' or '{condition_column}' does not exist.")
            return

        column_index = headers.index(column)
        condition_index = headers.index(condition_column)

        for row in rows[1:]:
            if row[condition_index] == condition_value:
                row[column_index] = new_value
                updated = True

        # Write back the updated content
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    if updated:
        print(f"Records updated successfully in '{table_name}'.")
    else:
        print(f"No matching records found in '{table_name}'.")

def delete_from_table(db_name, table_name, condition_column, condition_value):
    """Deletes rows that match a condition in the table."""
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

    if not os.path.exists(table_path):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'.")
        return

    index_files = sorted([f for f in os.listdir(table_path) if f.startswith("index_")])

    if not index_files:
        print(f"Table '{table_name}' is empty.")
        return

    deleted = False

    for index_file in index_files:
        file_path = os.path.join(table_path, index_file)

        with open(file_path, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if len(rows) < 2:
            continue  # Skip empty files

        headers = rows[0]

        if condition_column not in headers:
            print(f"Error: Column '{condition_column}' does not exist.")
            return

        condition_index = headers.index(condition_column)

        # Keep only rows that don't match the condition
        new_rows = [rows[0]] + [row for row in rows[1:] if row[condition_index] != condition_value]

        if len(new_rows) < len(rows):
            deleted = True

        # Write back the filtered content
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(new_rows)

    if deleted:
        print(f"Records deleted successfully from '{table_name}'.")
    else:
        print(f"No matching records found in '{table_name}'.")
