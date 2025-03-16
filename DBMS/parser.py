import re
import uuid
from database_manager import *
from transaction_manager import *

current_db = None  # Tracks the currently active database

def parse_command(command, transaction_manager, txn_id):
    """Parses and executes user commands related to database operations and transactions."""
    global current_db

    command = command.strip()

    # Database commands
    if match := re.match(r"CREATE DATABASE (\w+)", command, re.IGNORECASE):
        db_name = match.group(1)
        create_database(db_name)
        current_db = db_name

    elif match := re.match(r"DROP DATABASE (\w+)", command, re.IGNORECASE):
        db_name = match.group(1)
        drop_database(db_name)
        if current_db == db_name:
            current_db = None

    elif match := re.match(r"USE DATABASE (\w+)", command, re.IGNORECASE):
        db_name = match.group(1)
        current_db = db_name
        print(f"Switched to database '{db_name}'.")

    # Table commands
    elif match := re.match(r"CREATE TABLE (\w+) \((.+)\)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return txn_id
        
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(",")]
        create_table(current_db, table_name, columns)

    elif match := re.match(r"DROP TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return txn_id

        table_name = match.group(1)
        drop_table(current_db, table_name)
    
    elif match := re.match(r"INSERT INTO (\w+) VALUES \((.+)\)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return txn_id
    
        table_name = match.group(1)
        values = [v.strip() for v in match.group(2).split(",")]
    
        # Find the highest existing ID across all index files
        highest_id = 0
        index_num = 1
    
        while True:
            index_file = f"databases/{current_db}/tables/{table_name}/index_{index_num}.csv"
            try:
                with open(index_file, "r") as file:
                    for line in file:
                        row = line.strip().split(",")
                        if row and row[0].isdigit():  # Ensure it's a valid ID
                            highest_id = max(highest_id, int(row[0]))
            except FileNotFoundError:
                break  # Stop when there's no more index files
            
            index_num += 1
    
        # Assign new ID as highest_id + 1
        new_id = highest_id + 1
        values.insert(0, str(new_id))  # Insert the ID at the beginning
    
        insert_into_table(current_db, table_name, values)
    

    elif match := re.match(r"SHOW TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return txn_id

        table_name = match.group(1)
        show_table(current_db, table_name)

    elif match := re.match(r"UPDATE (\w+) SET (\w+) = (.+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return txn_id

        table_name = match.group(1)
        column = match.group(2)
        new_value = match.group(3).strip()
        condition_column = match.group(4)
        condition_value = match.group(5).strip()
        update_table(current_db, table_name, column, new_value, condition_column, condition_value)

    elif match := re.match(r"DELETE FROM (\w+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return txn_id

        table_name = match.group(1)
        condition_column = match.group(2)
        condition_value = match.group(3).strip()

        # Call delete function and get the deleted ID
        deleted_id = delete_from_table(current_db, table_name, condition_column, condition_value)

        if deleted_id is not None:
            # Save the deleted ID in a file (append mode)
            with open(f"databases/{current_db}/tables/{table_name}/deleted_ids.txt", "a") as file:
                file.write(str(deleted_id) + "\n")

            print(f"Deleted row with ID {deleted_id}, saved to deleted_ids.txt")

    # Transaction-related commands
    elif match := re.match(r"START TRANSACTION FROM (\w+) (\w+) WHERE (\w+) = (\d+)", command, re.IGNORECASE):
        if not current_db:
            print("< Error: No database selected. Use 'USE DATABASE db_name'. >")
            return txn_id

        table_name, value_column, condition_column, condition_value = match.groups()
        txn_id = transaction_manager.start_transaction(current_db, table_name, condition_column, condition_value, value_column)
        return txn_id  # Update the transaction ID

    elif re.match(r"^[+-]\d+$", command):  # Handles value modifications in transactions
        if txn_id and transaction_manager.is_transaction_active(txn_id):
            transaction_manager.modify_transaction(txn_id, int(command))
        else:
            print("< Error: No active transaction. Start a transaction first. >")

    elif command.upper() == "SHOW VALUE OF TRANSACTION":
        if txn_id and transaction_manager.is_transaction_active(txn_id):
            transaction_manager.show_transaction_value(txn_id)
        else:
            print("< Error: No active transaction. Start a transaction first. >")

    elif command.upper() == "COMMIT TRANSACTION":
        if txn_id and transaction_manager.is_transaction_active(txn_id):
            transaction_manager.commit_transaction(txn_id)
            return None  # Reset txn_id after commit
        else:
            print("< Error: No active transaction to commit. >")

    elif command.upper() == "ROLLBACK TRANSACTION":
        if txn_id and transaction_manager.is_transaction_active(txn_id):
            transaction_manager.rollback_transaction(txn_id)
            return None  # Reset txn_id after rollback
        else:
            print("< Error: No active transaction to rollback. >")

    else:
        print("Invalid command.")

    return txn_id  # Return the current transaction ID
