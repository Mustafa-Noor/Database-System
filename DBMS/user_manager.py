import csv
import os
from utils import wait_for_keypress
from file_manager import find_file_for_username
from transaction_manager import TransactionManager  # Import TransactionManager

# Dictionary to store TransactionManager instances per user
user_transactions = {}

def get_transaction_manager(username):
    """Returns a unique TransactionManager instance for each user."""
    if username not in user_transactions:
        user_transactions[username] = TransactionManager()
    return user_transactions[username]

def register(username, password):
    """Register a new user."""
    filename = find_file_for_username(username)
    if not filename:
        print("Error: No file found for this username.")
        return

    users = []

    # Read existing users
    if os.path.exists(filename):
        with open(filename, "r") as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip header
            users = list(reader)

    # Check if username already exists
    for row in users:
        if row[0] == username:
            print("Username already exists! Choose a different one.")
            return

    # Insert new user in sorted order
    users.append([username, password])
    users.sort()  # Sort by username

    # Write back the updated user list
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Username", "Password"])  # Write header
        writer.writerows(users)
    
    print(f"User '{username}' registered successfully!")

def sign_in(username, password):
    """Sign in a user."""
    filename = find_file_for_username(username)
    if not filename or not os.path.exists(filename):
        print("Error: No file found for this username.")
        return False

    with open(filename, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            if row[0] == username:
                if row[1] == password:
                    print(f"Welcome back, {username}!")
                    return True
                else:
                    print("Incorrect password!")
                    return False

    print("Username not found!")
    return False
