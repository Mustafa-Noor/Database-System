import os
import json
import struct
import sys
from datetime import datetime
from BTree import BTreeIndex

# Add project directory to path to ensure proper imports
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.append(project_dir)

# Remove the circular import
# from user_manager import record_database_owner

# Change BASE_DIR to be inside the project folder
BASE_DIR = os.path.join(os.path.dirname(__file__), "databases")

class Column:
    def __init__(self, name, data_type, is_primary=False, is_nullable=True, default=None, is_unique=False):
        self.name = name
        self.data_type = data_type
        self.is_primary = is_primary
        self.is_nullable = is_nullable
        self.default = default
        self.is_unique = is_unique

class ForeignKey:
    def __init__(self, column, ref_table, ref_column, on_delete="RESTRICT", on_update="RESTRICT"):
        self.column = column
        self.ref_table = ref_table
        self.ref_column = ref_column
        self.on_delete = on_delete  # RESTRICT, CASCADE, SET NULL
        self.on_update = on_update  # RESTRICT, CASCADE, SET NULL

class Table:
    def __init__(self, name, columns, primary_key=None, foreign_keys=None):
        self.name = name
        self.columns = columns  # List of Column objects
        self.primary_key = primary_key
        self.foreign_keys = foreign_keys or []
        self.indexes = {}  # Column name -> BTreeIndex

class Database:
    def __init__(self, name):
        self.name = name
        self.tables = {}
        self.path = os.path.join(BASE_DIR, name)
        self.load_metadata()

    def load_metadata(self):
        """Load database metadata from disk."""
        if not os.path.exists(self.path):
            return

        metadata_file = os.path.join(self.path, "metadata.json")
        if os.path.exists(metadata_file):
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
                for table_name, table_data in metadata["tables"].items():
                    columns = []
                    for col_data in table_data["columns"]:
                        col = Column(
                            name=col_data["name"],
                            data_type=col_data["type"],
                            is_primary=col_data.get("is_primary", False),
                            is_nullable=col_data.get("is_nullable", True),
                            default=col_data.get("default"),
                            is_unique=col_data.get("is_unique", False)
                        )
                        columns.append(col)

                    foreign_keys = []
                    for fk_data in table_data.get("foreign_keys", []):
                        fk = ForeignKey(
                            column=fk_data["column"],
                            ref_table=fk_data["ref_table"],
                            ref_column=fk_data["ref_column"],
                            on_delete=fk_data.get("on_delete", "RESTRICT"),
                            on_update=fk_data.get("on_update", "RESTRICT")
                        )
                        foreign_keys.append(fk)

                    table = Table(
                        name=table_name,
                        columns=columns,
                        primary_key=table_data.get("primary_key"),
                        foreign_keys=foreign_keys
                    )
                    self.tables[table_name] = table

    def save_metadata(self):
        """Save database metadata to disk."""
        metadata = {
            "tables": {}
        }
        for table_name, table in self.tables.items():
            table_data = {
                "columns": [
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "is_primary": col.is_primary,
                        "is_nullable": col.is_nullable,
                        "default": col.default,
                        "is_unique": col.is_unique
                    }
                    for col in table.columns
                ],
                "primary_key": table.primary_key,
                "foreign_keys": [
                    {
                        "column": fk.column,
                        "ref_table": fk.ref_table,
                        "ref_column": fk.ref_column,
                        "on_delete": fk.on_delete,
                        "on_update": fk.on_update
                    }
                    for fk in table.foreign_keys
                ]
            }
            metadata["tables"][table_name] = table_data

        os.makedirs(self.path, exist_ok=True)
        with open(os.path.join(self.path, "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=4)

def create_database(db_name, owner=None):
    """Create a new database"""
    try:
        # Check if database already exists
        if os.path.exists(os.path.join(BASE_DIR, db_name)):
            print(f"Database '{db_name}' already exists")
            return False

        # Create database directory
        db_path = os.path.join(BASE_DIR, db_name)
        os.makedirs(db_path)
        print(f"Created database directory: {db_path}")

        # Create metadata file
        metadata = {
            'name': db_name,
            'owner': owner,
            'created_at': datetime.now().isoformat(),
            'tables': {}
        }
        
        metadata_path = os.path.join(db_path, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"Created metadata file: {metadata_path}")

        # Verify ownership was recorded
        if owner:
            with open(metadata_path, 'r') as f:
                stored_metadata = json.load(f)
                if stored_metadata.get('owner') != owner:
                    print(f"Error: Failed to record ownership for database '{db_name}'")
                    # Clean up if ownership recording failed
                    import shutil
                    shutil.rmtree(db_path)
                    return False
                print(f"Verified ownership for database '{db_name}'")

        return True
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        # Clean up if creation failed
        if os.path.exists(db_path):
            import shutil
            shutil.rmtree(db_path)
        return False

def drop_database(db_name):
    """Delete a database."""
    try:
        # First check if database exists in the filesystem
        db_path = os.path.join(BASE_DIR, db_name)
        if not os.path.exists(db_path):
            print(f"Error: Database '{db_name}' does not exist.")
            return False

        # Clean up ownership and sharing records first
        from user_manager import get_db_manager
        _, _, _, select_from_table, _, _, delete_from_table = get_db_manager()
        
        # Delete from database_owners table
        delete_from_table("user_database", "database_owners", lambda row: row[0] == db_name)
        
        # Delete from database_shares table
        delete_from_table("user_database", "database_shares", lambda row: row[0] == db_name)
        
        # Then remove the physical database directory
        import shutil
        shutil.rmtree(db_path)
        print(f"Database '{db_name}' dropped successfully.")
        return True
    except Exception as e:
        print(f"Error dropping database: {str(e)}")
        return False

def list_databases():
    """List all available databases by checking the data directory."""
    if not os.path.exists(BASE_DIR):
        print("No databases exist yet.")
        return []
    
    databases = [
        name for name in os.listdir(BASE_DIR) 
        if os.path.isdir(os.path.join(BASE_DIR, name))
        and os.path.exists(os.path.join(BASE_DIR, name, "metadata.json"))
    ]
    
    if not databases:
        print("No databases exist yet.")
        return []
    
    print("\nAvailable Databases:")
    for db in databases:
        print(f"- {db}")
    
    return databases

def create_table(db_name, table_name, columns, primary_key=None, foreign_keys=None, unique_constraints=None):
    """Create a new table with specified columns, primary key, and foreign keys."""
    db_path = os.path.join(BASE_DIR, db_name)
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_name}' does not exist.")
        return False

    table_path = os.path.join(db_path, "tables", table_name)
    if os.path.exists(table_path):
        print(f"Error: Table '{table_name}' already exists.")
        return False

    # Create table directory and files
    os.makedirs(table_path)
    
    # Create data file
    with open(os.path.join(table_path, "data.bin"), "wb") as f:
        pass

    # Create table object
    table = Table(table_name, columns, primary_key, foreign_keys)
    table.unique_constraints = unique_constraints or []
    
    # Add table to database
    db = Database(db_name)
    db.tables[table_name] = table
    db.save_metadata()

    # Create fresh indexes for all columns
    for column in columns:
        index_file = os.path.join(table_path, f"{column.name}_index.btree")
        # Remove existing index file if it exists
        if os.path.exists(index_file):
            os.remove(index_file)
        # Create new index
        btree = BTreeIndex(index_file)
        btree.close()

    print(f"Table '{table_name}' created successfully.")
    return True

def insert_into_table(db_name, table_name, values):
    """Insert a row into a table with constraint checking."""
    db = Database(db_name)
    if table_name not in db.tables:
        print(f"Error: Table '{table_name}' does not exist.")
        return False

    table = db.tables[table_name]
    
    # Validate column count
    if len(values) != len(table.columns):
        print(f"Error: Expected {len(table.columns)} values, got {len(values)}.")
        return False

    # Validate data types and constraints
    for i, (col, value) in enumerate(zip(table.columns, values)):
        # Check NULL constraint
        if value is None and not col.is_nullable:
            print(f"Error: Column '{col.name}' cannot be NULL.")
            return False

        # Check data type
        try:
            if col.data_type == "INTEGER":
                value = int(value)
            elif col.data_type == "FLOAT":
                value = float(value)
            elif col.data_type == "BOOLEAN":
                value = bool(value)
            elif col.data_type == "DATE":
                value = datetime.strptime(value, "%Y-%m-%d").date()
            values[i] = value
        except ValueError:
            print(f"Error: Invalid value type for column '{col.name}'.")
            return False

    # Check primary key constraint
    if table.primary_key:
        pk_col = next(col for col in table.columns if col.name == table.primary_key)
        pk_idx = table.columns.index(pk_col)
        pk_value = values[pk_idx]
        
        # Check if primary key already exists
        index_file = os.path.join(BASE_DIR, db_name, "tables", table_name, f"{pk_col.name}_index.btree")
        btree = BTreeIndex(index_file)
        if btree.search(pk_value) is not None:
            print(f"Error: Primary key value '{pk_value}' already exists.")
            return False

    # Check foreign key constraints
    for fk in table.foreign_keys:
        fk_col = next(col for col in table.columns if col.name == fk.column)
        fk_idx = table.columns.index(fk_col)
        fk_value = values[fk_idx]
        
        if fk_value is not None:  # Allow NULL for nullable foreign keys
            # Check if referenced table exists
            if fk.ref_table not in db.tables:
                print(f"Error: Referenced table '{fk.ref_table}' does not exist.")
                return False
            
            # Check if referenced column exists
            ref_table = db.tables[fk.ref_table]
            if fk.ref_column not in [c.name for c in ref_table.columns]:
                print(f"Error: Referenced column '{fk.ref_column}' does not exist in table '{fk.ref_table}'.")
                return False
            
            # Check if referenced value exists
            ref_index_file = os.path.join(BASE_DIR, db_name, "tables", fk.ref_table, f"{fk.ref_column}_index.btree")
            if not os.path.exists(ref_index_file):
                print(f"Error: Index for referenced column '{fk.ref_column}' does not exist.")
                return False
            
            ref_btree = BTreeIndex(ref_index_file)
            if ref_btree.search(fk_value) is None:
                print(f"Error: Foreign key value '{fk_value}' not found in referenced table '{fk.ref_table}'.")
                return False

    # Insert the row
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    data_file = os.path.join(table_path, "data.bin")
    
    # Serialize the row
    row_data = []
    for col, value in zip(table.columns, values):
        if value is None:
            row_data.append(b"NULL")
        elif col.data_type == "INTEGER":
            row_data.append(struct.pack("i", value))
        elif col.data_type == "FLOAT":
            row_data.append(struct.pack("f", value))
        elif col.data_type == "BOOLEAN":
            row_data.append(struct.pack("?", value))
        elif col.data_type == "DATE":
            row_data.append(value.isoformat().encode())
        else:  # STRING
            row_data.append(value.encode().ljust(20, b'\x00'))

    # Write to data file and update indexes
    with open(data_file, "ab") as f:
        # Write the row data
        f.write(b''.join(row_data))
        # Get the position where we wrote the data
        row_position = f.tell() - len(b''.join(row_data))
        
        # Update indexes
        for col, value in zip(table.columns, values):
            if value is not None:
                index_file = os.path.join(table_path, f"{col.name}_index.btree")
                btree = BTreeIndex(index_file)
                btree.insert(value, row_position)
                btree.close()

    print(f"Row inserted successfully into '{table_name}'.")
    return True

def select_from_table(db_name, table_name, columns=None, where=None, order_by=None, limit=None, offset=0):
    """Select rows from a table with optional filtering, ordering, and pagination."""
    try:
        print(f"Starting SELECT from {table_name}")
        
        db = Database(db_name)
        if table_name not in db.tables:
            print(f"Error: Table '{table_name}' does not exist.")
            return []

        table = db.tables[table_name]
        table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
        data_file = os.path.join(table_path, "data.bin")

        if not os.path.exists(data_file):
            print(f"Error: Data file for table '{table_name}' does not exist.")
            return []

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
            # Get file size
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(0)
            
            while f.tell() < file_size:
                try:
                    row = []
                    for col in table.columns:
                        try:
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
                        except (struct.error, ValueError, EOFError) as e:
                            print(f"Error reading column {col.name}: {str(e)}")
                            raise

                    # Apply WHERE clause if specified
                    if where is None or where(row):
                        # Select only requested columns
                        selected_row = []
                        for col_name in columns:
                            col_idx = next(i for i, c in enumerate(table.columns) if c.name == col_name)
                            selected_row.append(row[col_idx])
                        results.append(selected_row)

                except (struct.error, ValueError, EOFError) as e:
                    print(f"Error reading row: {str(e)}")
                    break

        print(f"Found {len(results)} rows")

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

    except Exception as e:
        print(f"Error in select_from_table: {str(e)}")
        return []

def join_tables(db_name, left_table, right_table, left_col, right_col, columns=None, where=None, order_by=None, limit=None, offset=0, join_type="INNER"):
    """Perform a join between two tables with optional filtering, ordering, and pagination."""
    try:
        db = Database(db_name)
        if left_table not in db.tables or right_table not in db.tables:
            print("Error: One or both tables do not exist.")
            return []

        # Get table objects
        left = db.tables[left_table]
        right = db.tables[right_table]

        # Validate join columns
        if left_col not in [c.name for c in left.columns] or right_col not in [c.name for c in right.columns]:
            print("Error: Invalid join column(s).")
            return []

        # Determine which columns to select
        if columns is None:
            columns = [f"{left_table}.{c.name}" for c in left.columns] + \
                     [f"{right_table}.{c.name}" for c in right.columns]

        # Perform the join
        results = []
        left_data = select_from_table(db_name, left_table)
        right_data = select_from_table(db_name, right_table)

        left_col_idx = next(i for i, c in enumerate(left.columns) if c.name == left_col)
        right_col_idx = next(i for i, c in enumerate(right.columns) if c.name == right_col)

        # Create lookup dictionary for right table
        right_lookup = {}
        for right_row in right_data:
            key = right_row[right_col_idx]
            if key not in right_lookup:
                right_lookup[key] = []
            right_lookup[key].append(right_row)

        # Perform join based on type
        if join_type == "INNER":
            for left_row in left_data:
                key = left_row[left_col_idx]
                if key in right_lookup:
                    for right_row in right_lookup[key]:
                        joined_row = []
                        for col in columns:
                            if col.startswith(f"{left_table}."):
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(left.columns) if c.name == col_name)
                                joined_row.append(left_row[col_idx])
                            else:
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(right.columns) if c.name == col_name)
                                joined_row.append(right_row[col_idx])
                        
                        if where is None or where(joined_row):
                            results.append(joined_row)

        elif join_type == "LEFT":
            for left_row in left_data:
                key = left_row[left_col_idx]
                if key in right_lookup:
                    for right_row in right_lookup[key]:
                        joined_row = []
                        for col in columns:
                            if col.startswith(f"{left_table}."):
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(left.columns) if c.name == col_name)
                                joined_row.append(left_row[col_idx])
                            else:
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(right.columns) if c.name == col_name)
                                joined_row.append(right_row[col_idx])
                        
                        if where is None or where(joined_row):
                            results.append(joined_row)
                else:
                    # No matching right row, fill with NULLs
                    joined_row = []
                    for col in columns:
                        if col.startswith(f"{left_table}."):
                            col_name = col.split(".")[1]
                            col_idx = next(i for i, c in enumerate(left.columns) if c.name == col_name)
                            joined_row.append(left_row[col_idx])
                        else:
                            joined_row.append(None)
                    
                    if where is None or where(joined_row):
                        results.append(joined_row)

        elif join_type == "RIGHT":
            # Create lookup dictionary for left table
            left_lookup = {}
            for left_row in left_data:
                key = left_row[left_col_idx]
                if key not in left_lookup:
                    left_lookup[key] = []
                left_lookup[key].append(left_row)

            for right_row in right_data:
                key = right_row[right_col_idx]
                if key in left_lookup:
                    for left_row in left_lookup[key]:
                        joined_row = []
                        for col in columns:
                            if col.startswith(f"{left_table}."):
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(left.columns) if c.name == col_name)
                                joined_row.append(left_row[col_idx])
                            else:
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(right.columns) if c.name == col_name)
                                joined_row.append(right_row[col_idx])
                        
                        if where is None or where(joined_row):
                            results.append(joined_row)
                else:
                    # No matching left row, fill with NULLs
                    joined_row = []
                    for col in columns:
                        if col.startswith(f"{left_table}."):
                            joined_row.append(None)
                        else:
                            col_name = col.split(".")[1]
                            col_idx = next(i for i, c in enumerate(right.columns) if c.name == col_name)
                            joined_row.append(right_row[col_idx])
                    
                    if where is None or where(joined_row):
                        results.append(joined_row)

        elif join_type == "FULL":
            # Perform LEFT JOIN
            for left_row in left_data:
                key = left_row[left_col_idx]
                if key in right_lookup:
                    for right_row in right_lookup[key]:
                        joined_row = []
                        for col in columns:
                            if col.startswith(f"{left_table}."):
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(left.columns) if c.name == col_name)
                                joined_row.append(left_row[col_idx])
                            else:
                                col_name = col.split(".")[1]
                                col_idx = next(i for i, c in enumerate(right.columns) if c.name == col_name)
                                joined_row.append(right_row[col_idx])
                        
                        if where is None or where(joined_row):
                            results.append(joined_row)
                else:
                    # No matching right row, fill with NULLs
                    joined_row = []
                    for col in columns:
                        if col.startswith(f"{left_table}."):
                            col_name = col.split(".")[1]
                            col_idx = next(i for i, c in enumerate(left.columns) if c.name == col_name)
                            joined_row.append(left_row[col_idx])
                        else:
                            joined_row.append(None)
                    
                    if where is None or where(joined_row):
                        results.append(joined_row)

            # Add unmatched right rows
            for right_row in right_data:
                key = right_row[right_col_idx]
                if key not in {row[left_col_idx] for row in left_data}:
                    joined_row = []
                    for col in columns:
                        if col.startswith(f"{left_table}."):
                            joined_row.append(None)
                        else:
                            col_name = col.split(".")[1]
                            col_idx = next(i for i, c in enumerate(right.columns) if c.name == col_name)
                            joined_row.append(right_row[col_idx])
                    
                    if where is None or where(joined_row):
                        results.append(joined_row)

        else:
            print(f"Error: Unsupported join type '{join_type}'")
            return []

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

    except Exception as e:
        print(f"Error in join_tables: {str(e)}")
        return []

def delete_from_table(db_name, table_name, where=None, returning_columns=None):
    """Delete rows from a table with optional filtering and returning deleted rows."""
    db = Database(db_name)
    if table_name not in db.tables:
        print(f"Error: Table '{table_name}' does not exist.")
        return False

    table = db.tables[table_name]
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    data_file = os.path.join(table_path, "data.bin")

    # Check for foreign key references
    for other_table in db.tables.values():
        for fk in other_table.foreign_keys:
            if fk.ref_table == table_name:
                # Check if any rows reference this table
                ref_data = select_from_table(db_name, other_table.name)
                if ref_data:
                    print(f"Error: Cannot delete from '{table_name}' - referenced by '{other_table.name}'.")
                    return False

    # Delete rows
    temp_file = os.path.join(table_path, "temp.bin")
    deleted_rows = []
    
    try:
        with open(data_file, "rb") as f_in, open(temp_file, "wb") as f_out:
            while True:
                try:
                    row = []
                    for col in table.columns:
                        if col.data_type == "INTEGER":
                            value = struct.unpack("i", f_in.read(4))[0]
                        elif col.data_type == "FLOAT":
                            value = struct.unpack("f", f_in.read(4))[0]
                        elif col.data_type == "BOOLEAN":
                            value = struct.unpack("?", f_in.read(1))[0]
                        elif col.data_type == "DATE":
                            date_str = f_in.read(10).decode()
                            value = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str.strip() else None
                        else:  # STRING
                            value = f_in.read(20).decode().rstrip('\x00')
                            value = None if not value.strip() else value
                        row.append(value)

                    # Check if we've reached the end of file
                    if not row or all(v is None for v in row):
                        break

                    # Keep row if it doesn't match WHERE clause
                    if where is None or not where(row):
                        # Write row to temp file
                        for col, value in zip(table.columns, row):
                            if value is None:
                                f_out.write(b"NULL")
                            elif col.data_type == "INTEGER":
                                f_out.write(struct.pack("i", value))
                            elif col.data_type == "FLOAT":
                                f_out.write(struct.pack("f", value))
                            elif col.data_type == "BOOLEAN":
                                f_out.write(struct.pack("?", value))
                            elif col.data_type == "DATE":
                                f_out.write(value.isoformat().encode())
                            else:  # STRING
                                f_out.write(str(value).encode().ljust(20, b'\x00'))
                    elif returning_columns:
                        # Store deleted row if RETURNING is specified
                        selected_row = []
                        for col_name in returning_columns:
                            col_idx = next(i for i, c in enumerate(table.columns) if c.name == col_name)
                            selected_row.append(row[col_idx])
                        deleted_rows.append(selected_row)

                except (struct.error, ValueError, EOFError):
                    break

        # Replace original file with temp file
        os.replace(temp_file, data_file)

        # Update indexes
        for col in table.columns:
            index_file = os.path.join(table_path, f"{col.name}_index.btree")
            btree = BTreeIndex(index_file)
            btree.close()  # Recreate empty index

        print(f"Rows deleted successfully from '{table_name}'.")
        return deleted_rows if returning_columns else True

    except Exception as e:
        print(f"Error deleting rows: {str(e)}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False

def update_table(db_name, table_name, set_values, where=None, returning_columns=None):
    """Update rows in a table with optional filtering and returning updated rows."""
    db = Database(db_name)
    if table_name not in db.tables:
        print(f"Error: Table '{table_name}' does not exist.")
        return False

    table = db.tables[table_name]
    table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
    data_file = os.path.join(table_path, "data.bin")

    # Validate column names
    for col_name in set_values:
        if col_name not in [c.name for c in table.columns]:
            print(f"Error: Column '{col_name}' does not exist.")
            return False

    # Update rows
    temp_file = os.path.join(table_path, "temp.bin")
    updated_rows = []
    old_values = []  # Store old values for transaction rollback
    
    try:
        with open(data_file, "rb") as f_in, open(temp_file, "wb") as f_out:
            while True:
                try:
                    row = []
                    row_start = f_in.tell()
                    for col in table.columns:
                        if col.data_type == "INTEGER":
                            value = struct.unpack("i", f_in.read(4))[0]
                        elif col.data_type == "FLOAT":
                            value = struct.unpack("f", f_in.read(4))[0]
                        elif col.data_type == "BOOLEAN":
                            value = struct.unpack("?", f_in.read(1))[0]
                        elif col.data_type == "DATE":
                            date_str = f_in.read(10).decode()
                            value = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str.strip() else None
                        else:  # STRING
                            value = f_in.read(20).decode().rstrip('\x00')
                            value = None if not value.strip() else value
                        row.append(value)

                    if not row:  # End of file
                        break

                    # Update row if it matches WHERE clause
                    if where is None or where(row):
                        # Store old values for rollback
                        old_values.append(row.copy())
                        
                        # Update the row
                        for col_name, new_value in set_values.items():
                            col_idx = next(i for i, c in enumerate(table.columns) if c.name == col_name)
                            # Convert new value to appropriate type
                            if table.columns[col_idx].data_type == "INTEGER":
                                new_value = int(new_value)
                            elif table.columns[col_idx].data_type == "FLOAT":
                                new_value = float(new_value)
                            elif table.columns[col_idx].data_type == "BOOLEAN":
                                new_value = bool(new_value)
                            elif table.columns[col_idx].data_type == "DATE":
                                new_value = datetime.strptime(new_value, "%Y-%m-%d").date()
                            row[col_idx] = new_value
                        
                        # Store updated row if RETURNING is specified
                        if returning_columns:
                            selected_row = []
                            for col_name in returning_columns:
                                col_idx = next(i for i, c in enumerate(table.columns) if c.name == col_name)
                                selected_row.append(row[col_idx])
                            updated_rows.append(selected_row)

                    # Write row to temp file
                    for col, value in zip(table.columns, row):
                        if value is None:
                            f_out.write(b"NULL")
                        elif col.data_type == "INTEGER":
                            f_out.write(struct.pack("i", value))
                        elif col.data_type == "FLOAT":
                            f_out.write(struct.pack("f", value))
                        elif col.data_type == "BOOLEAN":
                            f_out.write(struct.pack("?", value))
                        elif col.data_type == "DATE":
                            f_out.write(value.isoformat().encode())
                        else:  # STRING
                            f_out.write(str(value).encode().ljust(20, b'\x00'))

                except (struct.error, ValueError, EOFError):
                    break

        # Replace original file with temp file
        os.replace(temp_file, data_file)

        # Update indexes
        for col in table.columns:
            index_file = os.path.join(table_path, f"{col.name}_index.btree")
            btree = BTreeIndex(index_file)
            btree.close()  # Recreate empty index

        print(f"Rows updated successfully in '{table_name}'.")
        return old_values if returning_columns else True

    except Exception as e:
        print(f"Error updating rows: {str(e)}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False