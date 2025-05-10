import os
from database_manager import Database, create_database, insert_into_table, select_from_table

BASE_DIR = "databases"

def register(username, password):
    """Register a new user by adding a row to the database."""
    # Check if user_database exists, if not create it
    if not os.path.exists(os.path.join(BASE_DIR, "user_database")):
        create_database("user_database")
        # Create users table if it doesn't exist
        from database_manager import create_table, Column
        columns = [
            Column("username", "STRING", is_primary=True, is_nullable=False),
            Column("password", "STRING", is_nullable=False)
        ]
        create_table("user_database", "users", columns)

    # Check if username already exists
    results = select_from_table("user_database", "users", ["username"], 
                              lambda row: row[0] == username)
    if results:
        print("Username already exists! Choose a different one.")
        return False

    # Insert the new user
    success = insert_into_table("user_database", "users", [username, password])
    if success:
        print(f"User '{username}' registered successfully!")
        return True
    return False

def sign_in(username, password):
    """Sign in a user by verifying credentials in the database."""
    # Check if user_database exists
    if not os.path.exists(os.path.join(BASE_DIR, "user_database")):
        print("No users registered yet!")
        return False

    # Search for the user
    results = select_from_table("user_database", "users", ["username", "password"],
                              lambda row: row[0] == username)
    
    if not results:
        print("Username not found!")
        return False

    # Verify password
    stored_username, stored_password = results[0]
    if stored_username == username and stored_password == password:
        print(f"Welcome back, {username}!")
        return True

    print("Incorrect password!")
    return False