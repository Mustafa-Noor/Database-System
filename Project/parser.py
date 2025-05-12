import re
from database_manager import *
from transaction_manager import TransactionManager
from user_manager import get_user_databases, user_has_access_to_db, verify_session

current_db = None  # Tracks the currently active database
current_transaction = None  # Tracks the current transaction
transaction_manager = None  # Global transaction manager instance
user_transactions = {}  # {username: {"db": ..., "transaction_id": ..., "manager": ...}}

def parse_command(command, active_user=None, db_name=None):
    """Parses and executes user commands related to database operations (stateless, for web/API)."""
    command = command.strip()

    # Check if user is logged in for all database operations
    if not active_user and command.upper() not in ["HELP", "EXIT"]:
        return "Please sign in first!"

    # Transaction management commands (session-based for web API)
    if command.upper() == "BEGIN TRANSACTION":
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        username = active_user["username"]
        if username in user_transactions and user_transactions[username]["transaction_id"] is not None:
            return "Error: Transaction already in progress."
        manager = TransactionManager(db_name)
        transaction_id = manager.begin_transaction()
        user_transactions[username] = {"db": db_name, "transaction_id": transaction_id, "manager": manager}
        return f"Transaction {transaction_id} started."
    elif command.upper() == "COMMIT":
        username = active_user["username"]
        if username not in user_transactions or user_transactions[username]["transaction_id"] is None:
            return "Error: No transaction in progress."
        manager = user_transactions[username]["manager"]
        transaction_id = user_transactions[username]["transaction_id"]
        try:
            manager.commit_transaction(transaction_id)
            user_transactions[username]["transaction_id"] = None
            return f"Transaction {transaction_id} committed."
        except Exception as e:
            return f"Error committing transaction: {str(e)}"
    elif command.upper() == "ROLLBACK":
        username = active_user["username"]
        if username not in user_transactions or user_transactions[username]["transaction_id"] is None:
            return "Error: No transaction in progress."
        manager = user_transactions[username]["manager"]
        transaction_id = user_transactions[username]["transaction_id"]
        try:
            manager.abort_transaction(transaction_id)
            user_transactions[username]["transaction_id"] = None
            return f"Transaction {transaction_id} rolled back."
        except Exception as e:
            return f"Error rolling back transaction: {str(e)}"

    # Database management commands
    elif match := re.match(r"CREATE DATABASE (\w+)", command, re.IGNORECASE):
        db_name_create = match.group(1)
        owner = active_user["username"] if active_user and isinstance(active_user, dict) else None
        result = create_database(db_name_create, owner=owner)
        if result:
            return f"Database '{db_name_create}' created successfully"
        else:
            return f"Failed to create database '{db_name_create}'"

    elif match := re.match(r"DROP DATABASE (\w+)", command, re.IGNORECASE):
        db_name_drop = match.group(1)
        if not active_user or not user_has_access_to_db(active_user["username"], db_name_drop):
            return "Access denied: You do not own or have access to this database."
        try:
            result = drop_database(db_name_drop)
            if result:
                return f"Database '{db_name_drop}' dropped successfully"
            else:
                return f"Failed to drop database '{db_name_drop}'"
        except Exception as e:
            return f"Error dropping database: {str(e)}"

    elif match := re.match(r"USE\s+(?:DATABASE\s+)?(\w+)", command, re.IGNORECASE):
        db_name_use = match.group(1)
        if not active_user or not user_has_access_to_db(active_user["username"], db_name_use):
            return "Access denied: You do not own or have access to this database."
        return f"Switched to database '{db_name_use}'"

    elif command.upper() == "SHOW DATABASES":
        if not active_user or "username" not in active_user:
            return "Please sign in first!"
        user_dbs = get_user_databases(active_user["username"])
        if not user_dbs:
            return {"results": [], "columns": ["Database", "Type"]}
        else:
            return {"results": [[db["name"], db["type"]] for db in user_dbs], "columns": ["Database", "Type"]}

    # Table management commands
    elif match := re.match(
        r"CREATE\s+TABLE\s+(\w+)\s*\((.+?)\)\s*;?$",
        command, re.IGNORECASE | re.DOTALL
    ):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        table_name = match.group(1)
        columns_def = [col.strip() for col in match.group(2).split(",")]
        columns = []
        primary_key = None
        foreign_keys = []
        unique_constraints = []
        for col_def in columns_def:
            col_parts = col_def.split()
            if not col_parts:
                continue
            col_name = col_parts[0]
            if col_name.upper() == "FOREIGN":
                fk_match = re.search(r"FOREIGN KEY\s*\((\w+)\)\s*REFERENCES\s+(\w+)\s*\((\w+)\)", col_def, re.IGNORECASE)
                if fk_match:
                    fk_col, ref_table, ref_col = fk_match.groups()
                    fk = ForeignKey(fk_col, ref_table, ref_col)
                    foreign_keys.append(fk)
                continue
            col_type = col_parts[1].upper()
            is_primary = "PRIMARY KEY" in col_def.upper()
            if is_primary:
                primary_key = col_name
            is_nullable = "NOT NULL" not in col_def.upper()
            is_unique = "UNIQUE" in col_def.upper()
            if is_unique:
                unique_constraints.append(col_name)
            default_match = re.search(r"DEFAULT\s+(\w+)", col_def, re.IGNORECASE)
            default = default_match.group(1) if default_match else None
            column = Column(col_name, col_type, is_primary, is_nullable, default, is_unique)
            columns.append(column)
        result = create_table(db_name, table_name, columns, primary_key, foreign_keys, unique_constraints)
        if result:
            return f"Table '{table_name}' created successfully"
        else:
            return f"Failed to create table '{table_name}'"

    elif match := re.match(r"DROP TABLE (\w+)", command, re.IGNORECASE):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        table_name = match.group(1)
        result = drop_table(db_name, table_name)
        if result:
            return f"Table '{table_name}' dropped successfully"
        else:
            return f"Failed to drop table '{table_name}'"

    elif command.upper() == "SHOW TABLES":
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        db = Database(db_name)
        if not db.tables:
            return {"results": [], "columns": ["Table"]}
        return {"results": [[table] for table in db.tables.keys()], "columns": ["Table"]}

    elif match := re.match(r"DESCRIBE TABLE (\w+)", command, re.IGNORECASE):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        table_name = match.group(1)
        db = Database(db_name)
        if table_name not in db.tables:
            return f"Error: Table '{table_name}' does not exist."
        table = db.tables[table_name]
        desc = []
        for col in table.columns:
            key_type = "PRI" if col.is_primary else "UNI" if col.is_unique else ""
            nullable = "NO" if not col.is_nullable else "YES"
            default = str(col.default) if col.default is not None else ""
            desc.append([col.name, col.data_type, nullable, key_type, default])
        return {"results": desc, "columns": ["Field", "Type", "Null", "Key", "Default"]}

    # Data manipulation commands
    elif match := re.match(
        r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+?)\)\s*;?$",
        command, re.IGNORECASE | re.DOTALL
    ):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        table_name = match.group(1)
        values = [v.strip().strip("'\"") for v in match.group(2).split(",")]
        result = insert_into_table(db_name, table_name, values)
        if result:
            return f"Row inserted into table '{table_name}' successfully"
        else:
            return f"Failed to insert into table '{table_name}'"

    # JOIN support
    elif match := re.match(
        r"SELECT\s+(.+?)\s+FROM\s+(\w+)\s+(INNER|LEFT)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?(?:\s+OFFSET\s+(\d+))?\s*;?$",
        command, re.IGNORECASE | re.DOTALL
    ):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        select_columns = [col.strip() for col in match.group(1).split(",")]
        left_table = match.group(2)
        join_type = match.group(3).upper()
        right_table = match.group(4)
        left_table_alias = match.group(5)
        left_col = match.group(6)
        right_table_alias = match.group(7)
        right_col = match.group(8)
        where_clause = match.group(9)
        order_by = match.group(10)
        limit = int(match.group(11)) if match.group(11) else None
        offset = int(match.group(12)) if match.group(12) else 0
        # Parse WHERE clause if present
        where_func = None
        db = Database(db_name)
        all_columns = [col.name for col in db.tables[left_table].columns] + [col.name for col in db.tables[right_table].columns]
        if where_clause:
            where_func = parse_where_clause(where_clause, all_columns)
        # Parse ORDER BY clause if present
        order_by_func = None
        if order_by:
            order_by_func = parse_order_by_clause(order_by, all_columns)
        results = join_tables(
            db_name,
            left_table,
            right_table,
            left_col,
            right_col,
            select_columns,
            where_func,
            order_by_func,
            limit,
            offset,
            join_type
        )
        return {"results": results, "columns": select_columns}

    # UPDATE ... SET ... [WHERE ...] [RETURNING ...]
    elif match := re.match(
        r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?(?:\s+RETURNING\s+(.+?))?\s*;?$",
        command, re.IGNORECASE | re.DOTALL
    ):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        returning_clause = match.group(4)
        # Parse SET clause
        set_values = {}
        for set_item in set_clause.split(","):
            col_name, value = set_item.split("=")
            col_name = col_name.strip()
            value = value.strip().strip("'\"")
            set_values[col_name] = value
        # Parse WHERE clause if present
        where_func = None
        db = Database(db_name)
        if where_clause:
            all_columns = [col.name for col in db.tables[table_name].columns]
            where_func = parse_where_clause(where_clause, all_columns)
        # Parse RETURNING clause if present
        returning_columns = None
        if returning_clause:
            returning_columns = [col.strip() for col in returning_clause.split(",")]
        results = update_table(
            db_name,
            table_name,
            set_values,
            where_func,
            returning_columns
        )
        if returning_columns and results:
            return {"results": results, "columns": returning_columns}
        return f"Table '{table_name}' updated successfully"

    # DELETE FROM ... [WHERE ...] [RETURNING ...]
    elif match := re.match(
        r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+RETURNING\s+(.+?))?\s*;?$",
        command, re.IGNORECASE | re.DOTALL
    ):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        table_name = match.group(1)
        where_clause = match.group(2)
        returning_clause = match.group(3)
        # Parse WHERE clause if present
        where_func = None
        db = Database(db_name)
        if where_clause:
            all_columns = [col.name for col in db.tables[table_name].columns]
            where_func = parse_where_clause(where_clause, all_columns)
        # Parse RETURNING clause if present
        returning_columns = None
        if returning_clause:
            returning_columns = [col.strip() for col in returning_clause.split(",")]
        results = delete_from_table(
            db_name,
            table_name,
            where_func,
            returning_columns
        )
        if returning_columns and results:
            return {"results": results, "columns": returning_columns}
        return f"Rows deleted from table '{table_name}' successfully"

    # Advanced SELECT with WHERE, ORDER BY, LIMIT, OFFSET
    elif match := re.match(
        r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?(?:\s+OFFSET\s+(\d+))?\s*;?$",
        command, re.IGNORECASE | re.DOTALL
    ):
        if not db_name or not active_user or not user_has_access_to_db(active_user["username"], db_name):
            return "Access denied: You do not own or have access to this database."
        select_columns = [col.strip() for col in match.group(1).split(",")]
        table_name = match.group(2)
        where_clause = match.group(3)
        order_by = match.group(4)
        limit = int(match.group(5)) if match.group(5) else None
        offset = int(match.group(6)) if match.group(6) else 0
        # Parse WHERE clause if present
        where_func = None
        db = Database(db_name)
        all_columns = [col.name for col in db.tables[table_name].columns]
        if where_clause:
            where_func = parse_where_clause(where_clause, all_columns)
        # Parse ORDER BY clause if present
        order_by_func = None
        if order_by:
            order_by_func = parse_order_by_clause(order_by, all_columns)
        results = select_from_table(
            db_name,
            table_name,
            select_columns,
            where_func,
            order_by_func,
            limit,
            offset
        )
        return {"results": results, "columns": select_columns}

    elif command.upper() == "HELP":
        return show_help()

    else:
        return "Invalid command. Type 'HELP' for available commands."

def parse_where_clause(where_clause, columns):
    """Parse WHERE clause into a function that can be used to filter rows."""
    try:
        # Split into conditions (handling AND/OR/NOT)
        conditions = []
        current_condition = []
        not_flag = False
        
        for token in where_clause.split():
            if token.upper() == "NOT":
                not_flag = True
            elif token.upper() in ("AND", "OR"):
                if current_condition:
                    conditions.append((" ".join(current_condition), token.upper(), not_flag))
                    current_condition = []
                    not_flag = False
            else:
                current_condition.append(token)
        
        if current_condition:
            conditions.append((" ".join(current_condition), None, not_flag))
        
        def where_func(row):
            result = None
            for condition, operator, is_not in conditions:
                # Parse condition
                parts = condition.split()
                if len(parts) < 2:
                    print(f"Error: Invalid condition format: {condition}")
                    return False
                    
                col_name = parts[0]
                
                # Handle BETWEEN operator
                if len(parts) >= 5 and parts[1].upper() == "BETWEEN" and parts[3].upper() == "AND":
                    try:
                        start_value = parts[2].strip("'\"")
                        end_value = parts[4].strip("'\"")
                        op = "BETWEEN"
                        value = (start_value, end_value)
                    except IndexError:
                        print(f"Error: Invalid BETWEEN syntax: {condition}")
                        return False
                else:
                    op = parts[1].upper()
                    value = " ".join(parts[2:]).strip("'\"") if len(parts) > 2 else None
                
                # Get column index
                try:
                    col_idx = next(i for i, c in enumerate(columns) if c == col_name)
                except StopIteration:
                    print(f"Error: Column '{col_name}' not found in table.")
                    return False
                
                # Compare values
                row_value = row[col_idx]
                
                # Handle NULL values
                if row_value is None:
                    condition_result = False
                else:
                    if op == "BETWEEN":
                        try:
                            start_val, end_val = value
                            # Convert values to appropriate type based on column data type
                            if isinstance(row_value, (int, float)):
                                start_val = float(start_val)
                                end_val = float(end_val)
                                row_val = float(row_value)
                            elif isinstance(row_value, datetime.date):
                                start_val = datetime.strptime(start_val, "%Y-%m-%d").date()
                                end_val = datetime.strptime(end_val, "%Y-%m-%d").date()
                                row_val = row_value
                            else:  # String comparison
                                row_val = str(row_value).lower()
                                start_val = str(start_val).lower()
                                end_val = str(end_val).lower()
                            condition_result = start_val <= row_val <= end_val
                        except (ValueError, TypeError) as e:
                            print(f"Error: Invalid BETWEEN values for {col_name}: {str(e)}")
                            return False
                    else:
                        # Convert both values to strings for comparison
                        row_value_str = str(row_value).lower()
                        value_str = str(value).lower()
                        
                        if op == "=":
                            condition_result = row_value_str == value_str
                        elif op == "!=":
                            condition_result = row_value_str != value_str
                        elif op == ">":
                            condition_result = str(row_value) > value
                        elif op == "<":
                            condition_result = str(row_value) < value
                        elif op == ">=":
                            condition_result = str(row_value) >= value
                        elif op == "<=":
                            condition_result = str(row_value) <= value
                        elif op == "LIKE":
                            # Convert SQL LIKE pattern to regex
                            pattern = value.replace("%", ".*").replace("_", ".")
                            condition_result = bool(re.match(f"^{pattern}$", row_value_str, re.IGNORECASE))
                        elif op == "IN":
                            values = value.strip("()").split(",")
                            condition_result = row_value_str in [v.strip("'\"").lower() for v in values]
                        else:
                            print(f"Error: Unknown operator '{op}'")
                            return False
                
                # Apply NOT if specified
                if is_not:
                    condition_result = not condition_result
                
                # Combine with previous result
                if result is None:
                    result = condition_result
                elif operator == "AND":
                    result = result and condition_result
                elif operator == "OR":
                    result = result or condition_result
            
            return result
        
        return where_func
    except Exception as e:
        print(f"Error parsing WHERE clause: {str(e)}")
        return None

def parse_order_by_clause(order_by, columns):
    """Parse ORDER BY clause into a function that can be used to sort rows."""
    order_parts = order_by.split()
    col_name = order_parts[0]
    direction = order_parts[1].upper() if len(order_parts) > 1 else "ASC"
    
    try:
        col_idx = next(i for i, c in enumerate(columns) if c == col_name)
    except StopIteration:
        print(f"Error: Column '{col_name}' not found in table.")
        return None
    
    def order_by_func(row):
        return row[col_idx]
    
    return (order_by_func, direction == "DESC")

def select_from_table(db_name, table_name, columns=None, where=None, order_by=None, limit=None, offset=0):
    """Select rows from a table with optional filtering, ordering, and pagination."""
    db = Database(db_name)
    if table_name not in db.tables:
        print(f"Error: Table '{table_name}' does not exist.")
        return []

    table = db.tables[table_name]
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    data_file = os.path.join(table_path, "data.bin")

    # Determine which columns to select
    if columns is None or columns == ["*"]:
        columns = [col.name for col in table.columns]
    else:
        # Validate column names
        for col in columns:
            if col not in [c.name for c in table.columns]:
                print(f"Error: Column '{col}' does not exist.")
                return []

    # Read and filter rows
    results = []
    with open(data_file, "rb") as f:
        while True:
            try:
                row = []
                for col in table.columns:
                    if col.data_type == "INTEGER":
                        value = struct.unpack("i", f.read(4))[0]
                    elif col.data_type == "FLOAT":
                        value = struct.unpack("f", f.read(4))[0]
                    elif col.data_type == "BOOLEAN":
                        value = struct.unpack("?", f.read(1))[0]
                    elif col.data_type == "DATE":
                        date_str = f.read(10).decode()
                        value = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str.strip() else None
                    else:  # STRING
                        value = f.read(20).decode().rstrip('\x00')
                        value = None if not value.strip() else value
                    row.append(value)

                if not row:  # End of file
                    break

                # Apply WHERE clause if specified
                if where is None or where(row):
                    # Select only requested columns
                    selected_row = []
                    for col_name in columns:
                        col_idx = next(i for i, c in enumerate(table.columns) if c.name == col_name)
                        selected_row.append(row[col_idx])
                    results.append(selected_row)

            except (struct.error, ValueError, EOFError):
                break

    # Apply ORDER BY if specified
    if order_by:
        order_func, reverse = order_by
        results.sort(key=order_func, reverse=reverse)

    # Apply LIMIT and OFFSET
    if limit is not None:
        results = results[offset:offset + limit]
    else:
        results = results[offset:]

    return results

def print_results(results, columns):
    """Print query results in a formatted table."""
    if not results:
        print("No rows found.")
        return
    
    # Calculate column widths
    col_widths = [max(len(str(col)), max(len(str(row[i])) for row in results)) for i, col in enumerate(columns)]
    total_width = sum(col_widths) + 3 * (len(columns) - 1)
    
    # Print header
    print("\nQuery Results:")
    print("-" * total_width)
    header = " | ".join(col.ljust(width) for col, width in zip(columns, col_widths))
    print(header)
    print("-" * total_width)
    
    # Print rows
    for row in results:
        print(" | ".join(str(val).ljust(width) for val, width in zip(row, col_widths)))
    
    print("-" * total_width)
    print(f"Total rows: {len(results)}")

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
            "CREATE TABLE <name> (col1 TYPE [PRIMARY KEY] [NOT NULL] [UNIQUE] [DEFAULT value], col2 TYPE [FOREIGN KEY REFERENCES table(col) [ON DELETE action] [ON UPDATE action]], ...)",
            "DROP TABLE <name>",
            "SHOW TABLES",
            "DESCRIBE TABLE <name>"
        ],
        "Data Manipulation": [
            "INSERT INTO <table> VALUES (value1, value2, ...)",
            "SELECT col1, col2, ... FROM <table> [WHERE condition] [ORDER BY col [ASC|DESC]] [LIMIT n] [OFFSET n]",
            "SELECT col1, col2, ... FROM table1 [INNER|LEFT|RIGHT|FULL] JOIN table2 ON table1.col = table2.col [WHERE condition] [ORDER BY col [ASC|DESC]] [LIMIT n] [OFFSET n]",
            "UPDATE <table> SET col1 = value1, col2 = value2, ... [WHERE condition] [RETURNING col1, col2, ...]",
            "DELETE FROM <table> [WHERE condition] [RETURNING col1, col2, ...]"
        ]
    }

    print("\nAvailable Commands:")
    for category, cmds in commands.items():
        print(f"\n{category}:")
        for cmd in cmds:
            print(f"  {cmd}")

def drop_table(db_name, table_name):
    """Drop a table from the database."""
    db = Database(db_name)
    if table_name not in db.tables:
        print(f"Error: Table '{table_name}' does not exist.")
        return False

    # Check for foreign key references
    for other_table in db.tables.values():
        for fk in other_table.foreign_keys:
            if fk.ref_table == table_name:
                print(f"Error: Cannot drop table '{table_name}' - referenced by '{other_table.name}'.")
                return False

    # Remove table from database
    del db.tables[table_name]
    db.save_metadata()

    # Remove table files
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    if os.path.exists(table_path):
        import shutil
        shutil.rmtree(table_path)

    print(f"Table '{table_name}' dropped successfully.")
    return True

def show_tables(db_name):
    """Show all tables in the database."""
    db = Database(db_name)
    if not db.tables:
        print("No tables found.")
        return

    print("\nTables in database:")
    for table_name in db.tables:
        print(f"  {table_name}")

def describe_table(db_name, table_name):
    """Show the structure of a table."""
    db = Database(db_name)
    if table_name not in db.tables:
        print(f"Error: Table '{table_name}' does not exist.")
        return

    table = db.tables[table_name]
    print(f"\nTable: {table_name}")
    print("-" * 80)
    print("Column Name".ljust(20) + "Type".ljust(15) + "Nullable".ljust(10) + "Key".ljust(10) + "Default".ljust(15))
    print("-" * 80)

    for col in table.columns:
        key_type = "PRI" if col.is_primary else "UNI" if col.is_unique else ""
        nullable = "NO" if not col.is_nullable else "YES"
        default = str(col.default) if col.default is not None else ""
        print(f"{col.name.ljust(20)}{col.data_type.ljust(15)}{nullable.ljust(10)}{key_type.ljust(10)}{default.ljust(15)}")

    if table.foreign_keys:
        print("\nForeign Keys:")
        for fk in table.foreign_keys:
            print(f"  {fk.column} -> {fk.ref_table}.{fk.ref_column} (ON DELETE {fk.on_delete}, ON UPDATE {fk.on_update})")
