import os
import json
import time
from datetime import datetime
from threading import Lock
import struct
from database_manager import Database, BTreeIndex

class Transaction:
    def __init__(self, transaction_id, start_time):
        self.transaction_id = transaction_id
        self.start_time = start_time
        self.status = "ACTIVE"  # ACTIVE, COMMITTED, ABORTED
        self.locks = set()  # Set of locked resources
        self.changes = {}  # Dictionary to store changes for rollback
        self.temp_files = set()  # Set of temporary files created

class TransactionManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self.transactions = {}  # transaction_id -> Transaction
        self.lock_table = {}  # resource -> (lock_type, transaction_id)
        self.next_transaction_id = 1
        self.lock = Lock()  # For thread safety
        self.log_file = os.path.join("databases", db_name, "transaction.log")
        self.checkpoint_file = os.path.join("databases", db_name, "checkpoint.json")
        self._initialize_log()

    def _initialize_log(self):
        """Initialize the transaction log file if it doesn't exist."""
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w") as f:
                json.dump([], f)

    def begin_transaction(self):
        """Start a new transaction."""
        with self.lock:
            transaction_id = self.next_transaction_id
            self.next_transaction_id += 1
            transaction = Transaction(transaction_id, datetime.now())
            self.transactions[transaction_id] = transaction
            self._log_transaction("BEGIN", transaction_id)
            return transaction_id

    def commit_transaction(self, transaction_id):
        """Commit a transaction."""
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} does not exist")
            
            transaction = self.transactions[transaction_id]
            if transaction.status != "ACTIVE":
                raise ValueError(f"Transaction {transaction_id} is not active")

            # Write all changes to disk
            for table_name, changes in transaction.changes.items():
                self._apply_changes(table_name, changes)

            # Release all locks
            for resource in transaction.locks:
                if resource in self.lock_table:
                    del self.lock_table[resource]

            # Update transaction status
            transaction.status = "COMMITTED"
            self._log_transaction("COMMIT", transaction_id)
            
            # Clean up temporary files
            for temp_file in transaction.temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            # Remove transaction
            del self.transactions[transaction_id]

    def abort_transaction(self, transaction_id):
        """Abort a transaction and rollback changes."""
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} does not exist")
            
            transaction = self.transactions[transaction_id]
            if transaction.status != "ACTIVE":
                raise ValueError(f"Transaction {transaction_id} is not active")

            # Rollback changes
            for table_name, changes in transaction.changes.items():
                self._rollback_changes(table_name, changes)

            # Release all locks
            for resource in transaction.locks:
                if resource in self.lock_table:
                    del self.lock_table[resource]

            # Update transaction status
            transaction.status = "ABORTED"
            self._log_transaction("ABORT", transaction_id)
            
            # Clean up temporary files
            for temp_file in transaction.temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            # Remove transaction
            del self.transactions[transaction_id]

    def acquire_lock(self, transaction_id, resource, lock_type="SHARED"):
        """Acquire a lock on a resource."""
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} does not exist")
            
            transaction = self.transactions[transaction_id]
            if transaction.status != "ACTIVE":
                raise ValueError(f"Transaction {transaction_id} is not active")

            # Check if resource is already locked
            if resource in self.lock_table:
                current_lock_type, current_tid = self.lock_table[resource]
                
                # If trying to upgrade to EXCLUSIVE lock
                if lock_type == "EXCLUSIVE" and current_lock_type == "SHARED":
                    if current_tid != transaction_id:
                        return False  # Cannot upgrade lock
                
                # If trying to get SHARED lock
                elif lock_type == "SHARED" and current_lock_type == "EXCLUSIVE":
                    if current_tid != transaction_id:
                        return False  # Cannot get shared lock

            # Acquire the lock
            self.lock_table[resource] = (lock_type, transaction_id)
            transaction.locks.add(resource)
            return True

    def release_lock(self, transaction_id, resource):
        """Release a lock on a resource."""
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} does not exist")
            
            transaction = self.transactions[transaction_id]
            if resource in transaction.locks:
                transaction.locks.remove(resource)
                if resource in self.lock_table:
                    del self.lock_table[resource]

    def _log_transaction(self, action, transaction_id):
        """Log a transaction action."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "transaction_id": transaction_id
        }
        
        with open(self.log_file, "r+") as f:
            log = json.load(f)
            log.append(log_entry)
            f.seek(0)
            json.dump(log, f, indent=4)
            f.truncate()

    def _apply_changes(self, table_name, changes):
        """Apply changes to a table."""
        table_path = os.path.join("databases", self.db_name, "tables", table_name)
        data_file = os.path.join(table_path, "data.bin")
        
        if changes["type"] == "INSERT":
            # For INSERT, we don't need to apply changes as they're already in the file
            pass
        elif changes["type"] == "UPDATE":
            # For UPDATE, we need to restore the old values
            with open(data_file, "r+b") as f:
                for old_row in changes["old_values"]:
                    # Write each value in the old row
                    for col, value in zip(self._get_table_columns(table_name), old_row):
                        if value is None:
                            f.write(b"NULL")
                        elif col.data_type == "INTEGER":
                            f.write(struct.pack("i", value))
                        elif col.data_type == "FLOAT":
                            f.write(struct.pack("f", value))
                        elif col.data_type == "BOOLEAN":
                            f.write(struct.pack("?", value))
                        elif col.data_type == "DATE":
                            f.write(value.isoformat().encode())
                        else:  # STRING
                            f.write(str(value).encode().ljust(20, b'\x00'))
        elif changes["type"] == "DELETE":
            # For DELETE, we need to restore the deleted rows
            with open(data_file, "ab") as f:
                for row_data in changes["rows"]:
                    f.write(row_data)

    def _get_table_columns(self, table_name):
        """Get the columns for a table."""
        db = Database(self.db_name)
        if table_name not in db.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        return db.tables[table_name].columns

    def _rollback_changes(self, table_name, changes):
        """Rollback changes to a table."""
        table_path = os.path.join("databases", self.db_name, "tables", table_name)
        data_file = os.path.join(table_path, "data.bin")
        
        if changes["type"] == "INSERT":
            # For INSERT, we need to remove the inserted row
            # Read the entire file
            with open(data_file, "rb") as f:
                data = f.read()
            
            # Find the position of the inserted row
            row_data = []
            for value in changes["values"]:
                if value is None:
                    row_data.append(b"NULL")
                elif isinstance(value, (int, str)):  # Handle both int and str
                    if isinstance(value, int):
                        row_data.append(struct.pack("i", value))
                    else:  # String
                        row_data.append(str(value).encode().ljust(20, b'\x00'))
                elif isinstance(value, float):
                    row_data.append(struct.pack("f", value))
                elif isinstance(value, bool):
                    row_data.append(struct.pack("?", value))
                elif isinstance(value, datetime.date):
                    row_data.append(value.isoformat().encode())
                else:  # Default to string
                    row_data.append(str(value).encode().ljust(20, b'\x00'))
            
            inserted_row = b''.join(row_data)
            position = data.find(inserted_row)
            
            if position != -1:
                # Write back the file without the inserted row
                with open(data_file, "wb") as f:
                    f.write(data[:position] + data[position + len(inserted_row):])
            
            # Update indexes
            for value in changes["values"]:
                if value is not None:
                    index_file = os.path.join(table_path, f"{value}_index.btree")
                    if os.path.exists(index_file):
                        btree = BTreeIndex(index_file)
                        btree.delete(value)
                        btree.close()
        
        elif changes["type"] == "UPDATE":
            # For UPDATE, we need to restore the old values
            with open(data_file, "r+b") as f:
                for old_row in changes["old_values"]:
                    # Write each value in the old row
                    for col, value in zip(self._get_table_columns(table_name), old_row):
                        if value is None:
                            f.write(b"NULL")
                        elif col.data_type == "INTEGER":
                            f.write(struct.pack("i", value))
                        elif col.data_type == "FLOAT":
                            f.write(struct.pack("f", value))
                        elif col.data_type == "BOOLEAN":
                            f.write(struct.pack("?", value))
                        elif col.data_type == "DATE":
                            f.write(value.isoformat().encode())
                        else:  # STRING
                            f.write(str(value).encode().ljust(20, b'\x00'))
        
        elif changes["type"] == "DELETE":
            # For DELETE, we need to restore the deleted rows
            with open(data_file, "ab") as f:
                for row in changes["rows"]:
                    # Convert each value in the row to bytes
                    for col, value in zip(self._get_table_columns(table_name), row):
                        if value is None:
                            f.write(b"NULL")
                        elif col.data_type == "INTEGER":
                            f.write(struct.pack("i", value))
                        elif col.data_type == "FLOAT":
                            f.write(struct.pack("f", value))
                        elif col.data_type == "BOOLEAN":
                            f.write(struct.pack("?", value))
                        elif col.data_type == "DATE":
                            f.write(value.isoformat().encode())
                        else:  # STRING
                            f.write(str(value).encode().ljust(20, b'\x00'))

    def create_checkpoint(self):
        """Create a checkpoint of the current database state."""
        checkpoint = {
            "timestamp": datetime.now().isoformat(),
            "next_transaction_id": self.next_transaction_id,
            "active_transactions": [
                {
                    "transaction_id": tid,
                    "start_time": t.start_time.isoformat(),
                    "status": t.status
                }
                for tid, t in self.transactions.items()
            ]
        }
        
        with open(self.checkpoint_file, "w") as f:
            json.dump(checkpoint, f, indent=4)

    def recover(self):
        """Recover the database state after a crash."""
        if not os.path.exists(self.checkpoint_file):
            return
        
        with open(self.checkpoint_file, "r") as f:
            checkpoint = json.load(f)
        
        # Recover active transactions
        for t_data in checkpoint["active_transactions"]:
            if t_data["status"] == "ACTIVE":
                # Rollback any uncommitted changes
                transaction_id = t_data["transaction_id"]
                self.abort_transaction(transaction_id) 