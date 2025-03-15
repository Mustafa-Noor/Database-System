import re
from database_manager import *
from transaction_manager import *

current_db = None  # Track active database
transaction_active = False  # Flag to track transaction state

def parse_command(command, transaction_manager):
    global current_db, transaction_active
    import uuid
    txn_id = str(uuid.uuid4())  # Generates a unique transaction ID

    command = command.strip()

    if transaction_manager.is_transaction_active():
        prompt = "🔹"
    else:
        prompt = ">"

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

    elif match := re.match(r"CREATE TABLE (\w+) \((.+)\)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return
        
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(",")]
        create_table(current_db, table_name, columns)

    elif match := re.match(r"DROP TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        drop_table(current_db, table_name)

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

    elif match := re.match(r"DELETE FROM (\w+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        condition_column = match.group(2)
        condition_value = match.group(3).strip()
        delete_from_table(current_db, table_name, condition_column, condition_value)

    # Transaction-related commands
    elif match := re.match(r"START TRANSACTION FROM (\w+) (\w+) WHERE (\w+) = (\d+)", command, re.IGNORECASE):
        if not current_db:
            print("< Error: No database selected. Use 'USE DATABASE db_name'. >")
            return
    
        table_name, value_column, condition_column, condition_value = match.groups()
        
        transaction_manager.start_transaction(current_db, table_name, condition_column, condition_value, value_column, txn_id)

    elif re.match(r"^[+-]\d+$", command):
        if transaction_manager.is_transaction_active():
            transaction_manager.write_transaction(command, txn_id)
        else:
            print("< Error: No active transaction. Start a transaction first. >")

    elif command.upper() == "SHOW VALUE OF TRANSACTION":
        if transaction_manager.is_transaction_active():
            transaction_manager.show_transaction_value(txn_id)
        else:
            print("< Error: No active transaction. Start a transaction first. >")

    elif command.upper() == "COMMIT TRANSACTION":
        if transaction_manager.is_transaction_active():
            transaction_manager.commit_transaction(txn_id)
        else:
            print("< Error: No active transaction to commit. >")

    elif command.upper() == "ROLLBACK TRANSACTION":
        if transaction_manager.is_transaction_active():
            transaction_manager.rollback_transaction(txn_id)
        else:
            print("< Error: No active transaction to rollback. >")

    else:
        print("Invalid command.")
