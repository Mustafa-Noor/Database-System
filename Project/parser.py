import re
from database_manager import *

current_db = None  # Tracks the currently active database

def parse_command(command):
    """Parses and executes user commands related to database operations."""
    global current_db

    command = command.strip()

    # Database management commands
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

    elif command.upper() == "SHOW DATABASES":
        list_databases()

    # Table management commands
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

    elif command.upper() == "SHOW TABLES":
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return
        list_tables(current_db)

    elif match := re.match(r"DESCRIBE TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return
        
        table_name = match.group(1)
        describe_table(current_db, table_name)

    elif match := re.match(r"TRUNCATE TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return
        
        table_name = match.group(1)
        truncate_table(current_db, table_name)

    # Data manipulation commands
    elif match := re.match(r"INSERT INTO (\w+) VALUES \((.+)\)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        values = [v.strip().strip("'\"") for v in match.group(2).split(",")]
        insert_into_table(current_db, table_name, values)

    elif match := re.match(r"SHOW TABLE (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        show_table(current_db, table_name)

    elif match := re.match(r"SELECT \* FROM (\w+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        show_table(current_db, table_name)

    elif match := re.match(r"SELECT (\w+) FROM (\w+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        column = match.group(1)
        table_name = match.group(2)
        condition_column = match.group(3)
        condition_value = match.group(4).strip().strip("'\"")
        search_in_table(current_db, table_name, condition_column, condition_value)

    elif match := re.match(r"UPDATE (\w+) SET (\w+) = (.+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        column = match.group(2)
        new_value = match.group(3).strip().strip("'\"")
        condition_column = match.group(4)
        condition_value = match.group(5).strip().strip("'\"")
        update_table(current_db, table_name, column, new_value, condition_column, condition_value)

    elif match := re.match(r"DELETE FROM (\w+) WHERE (\w+) = (.+)", command, re.IGNORECASE):
        if not current_db:
            print("Error: No database selected. Use 'USE DATABASE db_name'.")
            return

        table_name = match.group(1)
        condition_column = match.group(2)
        condition_value = match.group(3).strip().strip("'\"")
        delete_from_table(current_db, table_name, condition_column, condition_value)

    else:
        print("Invalid command. Available commands:")
        print_help()

def print_help():
    """Print available commands and their syntax."""
    commands = {
        "Database Management": [
            "CREATE DATABASE <name>",
            "DROP DATABASE <name>",
            "USE DATABASE <name>",
            "SHOW DATABASES"
        ],
        "Table Management": [
            "CREATE TABLE <name> (column1, column2, ...)",
            "DROP TABLE <name>",
            "SHOW TABLES",
            "DESCRIBE TABLE <name>",
            "TRUNCATE TABLE <name>"
        ],
        "Data Manipulation": [
            "INSERT INTO <table> VALUES (value1, value2, ...)",
            "SHOW TABLE <name>",
            "SELECT * FROM <table>",
            "SELECT <column> FROM <table> WHERE <column> = <value>",
            "UPDATE <table> SET <column> = <value> WHERE <column> = <value>",
            "DELETE FROM <table> WHERE <column> = <value>"
        ]
    }

    print("\nAvailable Commands:")
    for category, cmds in commands.items():
        print(f"\n{category}:")
        for cmd in cmds:
            print(f"  {cmd}")

    # # Transaction-related commands
    # elif match := re.match(r"START TRANSACTION FROM (\w+) (\w+) WHERE (\w+) = (\d+)", command, re.IGNORECASE):
    #     if not current_db:
    #         print("< Error: No database selected. Use 'USE DATABASE db_name'. >")
    #         return txn_id

    #     table_name, value_column, condition_column, condition_value = match.groups()
    #     txn_id = transaction_manager.start_transaction(current_db, table_name, condition_column, condition_value, value_column)
    #     return txn_id  # Update the transaction ID

    # elif re.match(r"^[+-]\d+$", command):  # Handles value modifications in transactions
    #     if txn_id and transaction_manager.is_transaction_active(txn_id):
    #         transaction_manager.modify_transaction(txn_id, int(command))
    #     else:
    #         print("< Error: No active transaction. Start a transaction first. >")

    # elif command.upper() == "SHOW VALUE OF TRANSACTION":
    #     if txn_id and transaction_manager.is_transaction_active(txn_id):
    #         transaction_manager.show_transaction_value(txn_id)
    #     else:
    #         print("< Error: No active transaction. Start a transaction first. >")

    # elif command.upper() == "COMMIT TRANSACTION":
    #     if txn_id and transaction_manager.is_transaction_active(txn_id):
    #         transaction_manager.commit_transaction(txn_id)
    #         return None  # Reset txn_id after commit
    #     else:
    #         print("< Error: No active transaction to commit. >")

    # elif command.upper() == "ROLLBACK TRANSACTION":
    #     if txn_id and transaction_manager.is_transaction_active(txn_id):
    #         transaction_manager.rollback_transaction(txn_id)
    #         return None  # Reset txn_id after rollback
    #     else:
    #         print("< Error: No active transaction to rollback. >")

    # else:
    #     print("Invalid command.")

    # return txn_id  # Return the current transaction ID
