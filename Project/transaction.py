import uuid
from database_manager import get_value_from_db, update_table

class Transaction:
    def __init__(self, db, table, condition_column, condition_value, value_column):
        """Initialize a transaction with database details."""
        self.txn_id = uuid.uuid4().hex[:8]  # Generate a unique short transaction ID
        self.db = db
        self.table = table
        self.condition_column = condition_column
        self.condition_value = condition_value
        self.value_column = value_column
        self.active = True  # Track transaction state

        # Fetch the original value from the database
        self.original_value = self._fetch_value()
        if self.original_value is None:
            raise ValueError(f"Error: No matching record found in '{table}' where {condition_column}={condition_value}.")

        self.current_value = self.original_value  # Track the modified value
        print(f"Transaction {self.txn_id} started. Initial value: {self.original_value}")

    def _fetch_value(self):
        """Fetch the current value from the database."""
        try:
            value = get_value_from_db(self.db, self.table, self.value_column, self.condition_column, self.condition_value)
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def modify_value(self, amount):
        """Modify the transaction value."""
        if not self.active:
            print(f"Error: Transaction {self.txn_id} is not active.")
            return False

        try:
            amount = int(amount)  # Ensure input is numeric
            self.current_value += amount
            print(f"Transaction {self.txn_id} updated: New value = {self.current_value}")
            return True
        except ValueError:
            print(f"Error: Invalid modification amount '{amount}'. Expected an integer.")
            return False

    def commit(self):
        """Commit changes to the database."""
        if not self.active:
            print(f"Error: Transaction {self.txn_id} is not active.")
            return False

        update_table(self.db, self.table, self.value_column, self.current_value, self.condition_column, self.condition_value)
        self.active = False
        print(f"Transaction {self.txn_id} committed successfully! Final value: {self.current_value}")
        return True

    def rollback(self):
        """Rollback to the original value."""
        if not self.active:
            print(f"Error: Transaction {self.txn_id} is not active.")
            return False

        update_table(self.db, self.table, self.value_column, self.original_value, self.condition_column, self.condition_value)
        self.active = False
        print(f"Transaction {self.txn_id} rolled back successfully! Value reset to {self.original_value}.")
        return True

    def is_active(self):
        """Check if the transaction is active."""
        return self.active
