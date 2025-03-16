import threading
import time
from transaction import Transaction

class TransactionManager:
    def __init__(self):
        self.transactions = {}  # Active transactions { txn_id -> Transaction object }
        self.locks = {}  # Locked rows { (db, table, condition_column, condition_value) -> (txn_id, lock_type) }
        self.lock_queue = {}  # Waiting transactions { (db, table, condition_column, condition_value) -> [txn_id] }
        self.lock = threading.Lock()  # Ensure thread safety

    def _acquire_lock(self, txn_id, key, lock_type):
        """Attempt to acquire a lock for the transaction."""
        with self.lock:
            while True:
                if key not in self.locks:
                    self.locks[key] = (txn_id, lock_type)
                    return True

                existing_txn, existing_lock = self.locks[key]

                if lock_type == "X":  # Exclusive Lock (Write)
                    if existing_txn != txn_id:
                        self.lock_queue.setdefault(key, []).append(txn_id)
                        self.lock.release()
                        time.sleep(0.5)
                        self.lock.acquire()
                        continue

                elif lock_type == "S":  # Shared Lock (Read)
                    if existing_lock == "X" and existing_txn != txn_id:
                        self.lock_queue.setdefault(key, []).append(txn_id)
                        self.lock.release()
                        time.sleep(0.5)
                        self.lock.acquire()
                        continue

                return False  # Lock cannot be acquired

    def _release_lock(self, txn_id, key):
        """Release the lock when a transaction commits or rolls back."""
        with self.lock:
            if key in self.locks and self.locks[key][0] == txn_id:
                del self.locks[key]
                if key in self.lock_queue and self.lock_queue[key]:
                    next_txn = self.lock_queue[key].pop(0)
                    self.locks[key] = (next_txn, "X")

    def start_transaction(self, db, table, condition_column, condition_value, value_column, mode="X"):
        """Start a new transaction with a lock mode (S = Read, X = Write)."""
        key = (db.lower(), table.lower(), condition_column.lower(), str(condition_value))
        
        try:
            txn = Transaction(db, table, condition_column, condition_value, value_column)
        except ValueError as e:
            print(e)
            return None

        if not self._acquire_lock(txn.txn_id, key, mode):
            print(f"Error: Transaction {txn.txn_id} could not acquire lock.")
            return None

        self.transactions[txn.txn_id] = txn
        print(f"Transaction {txn.txn_id} started successfully with {mode}-lock.")
        return txn.txn_id

    def is_transaction_active(self, txn_id):
        """Check if a transaction is active."""
        return txn_id in self.transactions and self.transactions[txn_id].is_active()

    def modify_transaction(self, txn_id, amount):
        """Modify the transaction value."""
        if not self.is_transaction_active(txn_id):
            print("Error: No active transaction.")
            return False

        txn = self.transactions[txn_id]
        key = (txn.db.lower(), txn.table.lower(), txn.condition_column.lower(), str(txn.condition_value))

        if key not in self.locks or self.locks[key][0] != txn_id:
            print(f"Error: Transaction {txn_id} does not hold an exclusive lock for modification.")
            return False

        return txn.modify_value(amount)

    def show_transaction_value(self, txn_id):
        """Display the current value, ensuring it’s not a dirty read."""
        if not self.is_transaction_active(txn_id):
            print(f"Error: No active transaction {txn_id}.")
            return

        txn = self.transactions[txn_id]
        key = (txn.db.lower(), txn.table.lower(), txn.condition_column.lower(), str(txn.condition_value))

        if key in self.locks and self.locks[key][1] == "X" and self.locks[key][0] != txn_id:
            print("Error: Cannot read uncommitted data (dirty read prevention).")
            return

        print(f"Current transaction value: {txn.current_value}")

    def commit_transaction(self, txn_id):
        """Commit the transaction and release locks."""
        if not self.is_transaction_active(txn_id):
            print(f"Error: No active transaction {txn_id} to commit.")
            return

        txn = self.transactions.pop(txn_id)
        key = (txn.db.lower(), txn.table.lower(), txn.condition_column.lower(), str(txn.condition_value))

        if txn.commit():
            self._release_lock(txn_id, key)
            print(f"Transaction {txn_id} committed and lock released.")

    def rollback_transaction(self, txn_id):
        """Rollback the transaction and release locks."""
        if not self.is_transaction_active(txn_id):
            print(f"Error: No active transaction {txn_id} to rollback.")
            return

        txn = self.transactions.pop(txn_id)
        key = (txn.db.lower(), txn.table.lower(), txn.condition_column.lower(), str(txn.condition_value))

        if txn.rollback():
            self._release_lock(txn_id, key)
            print(f"Transaction {txn_id} rolled back and lock released.")
