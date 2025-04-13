import os
from database_manager import create_table, insert_into_table, search_in_table

# Initialize the users table in the database
DATABASE_NAME = "user_database"
TABLE_NAME = "users"
COLUMNS = ["username", "password"]

# Ensure the database and table exist
if not os.path.exists(os.path.join("databases", DATABASE_NAME)):
    create_table(DATABASE_NAME, TABLE_NAME, COLUMNS)

def register(username, password):
    """Register a new user by adding a row to the database."""
    # Check if the username already exists
    if search_in_table(DATABASE_NAME, TABLE_NAME, "username", username):
        print("Username already exists! Choose a different one.")
        return

    # Insert the new user into the database
    insert_into_table(DATABASE_NAME, TABLE_NAME, [username, password])
    print(f"User '{username}' registered successfully!")

def sign_in(username, password):
    """Sign in a user by verifying credentials in the database."""
    # Search for the username in the database
    result = search_in_table(DATABASE_NAME, TABLE_NAME, "username", username)
    if not result:
        print("Username not found!")
        return False

    # Verify the password
    for row in result:
        if row[0] == username and row[1] == password:
            print(f"Welcome back, {username}!")
            return True

    print("Incorrect password!")
    return False