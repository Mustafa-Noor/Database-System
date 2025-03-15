import os
import csv
import re
from database_manager import *

class TransactionManager:
    def __init__(self):
        self.active_transaction = None  # Track the current transaction
        self.transaction_value = None  # Track the working value
        self.lock_table = {}  # Track locks: { (table, condition_value): {"type": "read/write", "txn_id": txn_id} }
        self.transaction_log = {}  # Track changes: { txn_id: [(table_name, row_index, old_value)] }


    def is_transaction_active(self):
        return self.active_transaction is not None


    def start_transaction(self, db_name, table_name, condition_column, condition_value, value_column, txn_id):
        """Start a transaction with Serializable Isolation Level."""
        table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)

        if not os.path.exists(table_path):
            print(f"< Error: Table '{table_name}' does not exist in database '{db_name}'. >")
            return

        index_files = sorted([f for f in os.listdir(table_path) if f.startswith("index_")])

        for index_file in index_files:
            file_path = os.path.join(table_path, index_file)
            with open(file_path, "r") as f:
                reader = csv.reader(f)
                rows = list(reader)

            if len(rows) < 2:
                continue  

            headers = rows[0]

            if condition_column not in headers or value_column not in headers:
                print(f"< Error: Column '{condition_column}' or '{value_column}' does not exist. >")
                return

            value_index = headers.index(value_column)
            condition_index = headers.index(condition_column)

            for row in rows[1:]:
                if row[condition_index] == condition_value:
                    
                    # 🔹 Check if there's a write lock (W-Lock) on this row
                    if (table_name, condition_value) in self.lock_table:
                        if self.lock_table[(table_name, condition_value)]["type"] == "write":
                            print("< Error: Row is write-locked by another transaction. >")
                            return

                    # 🔹 Apply Read Lock (S-Lock)
                    self.lock_table[(table_name, condition_value)] = {"type": "read", "txn_id": txn_id}

                    self.transaction_value = int(row[value_index])
                    self.active_transaction = (db_name, table_name, value_column, condition_column, condition_value, txn_id)
                    print("< Read Transaction started successfully! >")
                    return

            print("< Error: No matching record found for the given condition. >")




    def write_transaction(self, operation, txn_id):
        """Perform write operation with serializable isolation."""
        if not self.active_transaction:
            print("< Error: No active transaction. >")
            return

        db_name, table_name, value_column, condition_column, condition_value, active_txn_id = self.active_transaction

        # 🔹 Check if this transaction already has an exclusive lock
        if (table_name, condition_value) in self.lock_table:
            lock_info = self.lock_table[(table_name, condition_value)]

            # If another transaction holds a lock, deny access
            if lock_info["txn_id"] != txn_id:
                print("< Error: Row is locked by another transaction. >")
                return

            # If it only has a read lock, upgrade it to a write lock
            if lock_info["type"] == "read":
                print("< Upgrading Read Lock to Write Lock >")  # ✅ Debugging message
                self.lock_table[(table_name, condition_value)] = {"type": "write", "txn_id": txn_id}

        else:
            # Acquire exclusive write lock (X-Lock)
            self.lock_table[(table_name, condition_value)] = {"type": "write", "txn_id": txn_id}

        if not re.match(r"^[+-]\d+$", operation):
            print("< Error: Invalid operation format. Use '+X' or '-X'. >")
            return

        amount = int(operation)

        # 🔹 Save old value for rollback
        if txn_id not in self.transaction_log:
            self.transaction_log[txn_id] = []
        self.transaction_log[txn_id].append((table_name, condition_value, self.transaction_value))

        # Perform the update
        self.transaction_value += amount
        print(f"< Updated Transaction Value: {self.transaction_value} >")



    def show_transaction_value(self, txn_id):
        """Displays the current transaction value with proper read isolation."""
        if not self.active_transaction:
            print("< Error: No active transaction. >")
            return

        db_name, table_name, value_column, condition_column, condition_value, active_txn_id = self.active_transaction

        # 🔹 Ensure the transaction has a read lock
        if (table_name, condition_value) in self.lock_table:
            lock_info = self.lock_table[(table_name, condition_value)]
            
            # 🔹 If another transaction holds a write lock, block the read
            if lock_info["type"] == "write" and lock_info["txn_id"] != txn_id:
                print("< Error: Cannot read. Row is locked by another transaction. >")
                return
        else:
            # 🔹 If no lock exists, acquire a read lock
            self.lock_table[(table_name, condition_value)] = {"type": "read", "txn_id": txn_id}

        print(f"< Transaction Value: {self.transaction_value} >")


    def commit_transaction(self, txn_id):
        """Commits the transaction while ensuring proper serialization."""
        if not self.active_transaction:
            print("< Error: No active transaction to commit. >")
            return

        db_name, table_name, value_column, condition_column, condition_value, active_txn_id = self.active_transaction

        # 🔹 Check if the transaction has the correct write lock
        lock_info = self.lock_table.get((table_name, condition_value))
        if lock_info and lock_info["type"] == "write" and lock_info["txn_id"] != txn_id:
            print("< Error: Cannot commit. Another transaction holds the lock. >")
            return

        table_path = os.path.join(BASE_DIR, db_name, "tables", table_name)
        index_files = sorted([f for f in os.listdir(table_path) if f.startswith("index_")])

        for index_file in index_files:
            file_path = os.path.join(table_path, index_file)
            with open(file_path, "r") as f:
                reader = csv.reader(f)
                rows = list(reader)

            headers = rows[0]
            condition_index = headers.index(condition_column)
            value_index = headers.index(value_column)
            updated = False

            for row in rows[1:]:
                if row[condition_index] == condition_value:
                    row[value_index] = str(self.transaction_value)
                    updated = True
                    break

            if updated:
                with open(file_path, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                # 🔹 Release the lock properly
                self.lock_table.pop((table_name, condition_value), None)  # ✅ Ensure lock is removed
                self.active_transaction = None
                self.transaction_value = None

                print(f"< Transaction Committed: ID {condition_value} in '{table_name}' >")
                return

        print("< Error: Failed to commit transaction. >")


    def rollback_transaction(self, txn_id):
        """Aborts the transaction and restores old values."""
        if not self.active_transaction:
            print("< Error: No active transaction to rollback. >")
            return

        db_name, table_name, value_column, condition_column, condition_value, active_txn_id = self.active_transaction

        if active_txn_id != txn_id:
            print("< Error: Cannot rollback another transaction. >")
            return

        # 🔹 Restore Old Values
        if txn_id in self.transaction_log:
            for table, condition_value, old_value in self.transaction_log[txn_id]:
                self.transaction_value = old_value  # Restore old value

        # 🔹 Release locks
        self.lock_table.pop((table_name, condition_value), None)  # ✅ Ensure lock is removed
        self.transaction_log.pop(txn_id, None)
        self.active_transaction = None
        self.transaction_value = None

        print(f"< Transaction Aborted: ID {condition_value} in '{table_name}' >")


