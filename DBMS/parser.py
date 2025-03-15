import re
from database_manager import create_database, create_table, insert_into_table, show_table, update_table

current_db = None  # Track active database

def parse_command(command):
    global current_db

    command = command.strip()

    if match := re.match(r"CREATE DATABASE (\w+)", command, re.IGNORECASE):
        db_name = match.group(1)
        create_database(db_name)
        current_db = db_name

    elif match := re.match(r"USE DATABASE (\w+)", command, re.IGNORECASE):
        db_name = match.group(1)
        current_db = db_name
        print(f"Switched to database '{db_name}'.")

    elif match := re.match(r"CREATE TABLE (\w+) \((.+)\)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return
        
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(",")]
        create_table(current_db, table_name, columns)

    elif match := re.match(r"INSERT INTO (\w+) VALUES \((.+)\)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        values = [v.strip() for v in match.group(2).split(",")]
        insert_into_table(current_db, table_name, values)

    elif match := re.match(r"SHOW TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        show_table(current_db, table_name)

    elif match := re.match(r"UPDATE (\w+) SET (\w+) = (.+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        column = match.group(2)
        new_value = match.group(3).strip()
        condition_column = match.group(4)
        condition_value = match.group(5).strip()
        update_table(current_db, table_name, column, new_value, condition_column, condition_value)

    else:
        print("Invalid command.")
