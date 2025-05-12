import os
import hashlib
import json
from datetime import datetime
import time
from database_manager import Column

# Change BASE_DIR to be inside the project folder
BASE_DIR = os.path.join(os.path.dirname(__file__), "databases")

def get_db_manager():
    """Lazy load database manager to avoid circular imports."""
    from database_manager import Database, create_database, insert_into_table, select_from_table, create_table, Column, delete_from_table
    return Database, create_database, insert_into_table, select_from_table, create_table, Column, delete_from_table

def initialize_user_database():
    """Initialize the user database with required tables."""
    Database, create_database, insert_into_table, select_from_table, create_table, Column, _ = get_db_manager()
    
    if not os.path.exists(os.path.join(BASE_DIR, "user_database")):
        create_database("user_database")
        # Create users table
        columns = [
            Column("username", "STRING", is_primary=True, is_nullable=False),
            Column("password", "STRING", is_nullable=False),
            Column("email", "STRING", is_nullable=False),
            Column("created_at", "DATE", is_nullable=False)
        ]
        create_table("user_database", "users", columns)
        # Create database_owners table
        columns = [
            Column("database_name", "STRING", is_primary=True, is_nullable=False),
            Column("owner", "STRING", is_nullable=False)
        ]
        create_table("user_database", "database_owners", columns)
        # Create database_shares table for managing database access
        columns = [
            Column("database_name", "STRING", is_nullable=False),
            Column("owner", "STRING", is_nullable=False),
            Column("shared_with", "STRING", is_nullable=False),
            Column("shared_at", "DATE", is_nullable=False)
        ]
        create_table("user_database", "database_shares", columns)
        # Create sessions table
        columns = [
            Column("username", "STRING", is_primary=True, is_nullable=False),
            Column("session_token", "STRING", is_nullable=False),
            Column("created_at", "DATE", is_nullable=False)
        ]
        create_table("user_database", "sessions", columns)

def register(username, password, email):
    """Register a new user by adding a row to the database."""
    initialize_user_database()
    _, _, insert_into_table, select_from_table, _, _, _ = get_db_manager()
    
    # Check if username already exists
    results = select_from_table("user_database", "users", ["username"], 
                              lambda row: row[0] == username)
    if results:
        print("Username already exists! Choose a different one.")
        return False
    
    success = insert_into_table("user_database", "users", [
        username, 
        password,  # In production, this should be hashed
        email,
        datetime.now().strftime("%Y-%m-%d")
    ])
    if success:
        print(f"User '{username}' registered successfully!")
        return True
    return False

def record_database_owner(database_name, owner):
    """Record the owner of a database in the database_owners table."""
    # Make sure user_database exists
    initialize_user_database()
    _, _, insert_into_table, select_from_table, _, _, _ = get_db_manager()
    
    print(f"Recording ownership for database '{database_name}' by user '{owner}'")
    
    # Check if already exists
    results = select_from_table("user_database", "database_owners", ["database_name"], 
                              lambda row: row[0] == database_name)
    if results:
        print(f"Database '{database_name}' already has an owner recorded")
        return False
    
    # Insert ownership record
    success = insert_into_table("user_database", "database_owners", [database_name, owner])
    if success:
        print(f"Successfully recorded '{owner}' as owner of '{database_name}'")
        return True
    else:
        print(f"Failed to record '{owner}' as owner of '{database_name}'")
        return False

def sign_in(username, password):
    """Sign in a user by verifying credentials in the database."""
    if not os.path.exists(os.path.join(BASE_DIR, "user_database")):
        print("No users registered yet!")
        return False

    _, _, _, select_from_table, _, _, _ = get_db_manager()
    
    # Search for the user
    results = select_from_table("user_database", "users", ["username", "password", "email"],
                              lambda row: row[0] == username)
    
    if not results:
        print("Username not found!")
        return False

    # Verify password
    stored_username, stored_password, email = results[0]
    if stored_username == username and stored_password == password:
        # Return user data
        user_data = {
            "username": username,
            "email": email,
            "databases": get_user_databases(username)
        }
        
        print(f"Welcome back, {username}!")
        return user_data

    print("Incorrect password!")
    return False

def sign_out():
    """Sign out the current user and invalidate their session."""
    _, _, _, select_from_table, _, _, delete_from_table = get_db_manager()
    
    # Delete the session from the sessions table
    if os.path.exists(os.path.join(BASE_DIR, "user_database", "tables", "sessions")):
        delete_from_table("user_database", "sessions", lambda row: True)
    
    return True

def verify_session(session_token):
    """Verify if a session token is valid."""
    if not os.path.exists(os.path.join(BASE_DIR, "user_database", "tables", "sessions")):
        return False
        
    _, _, _, select_from_table, _, _, _ = get_db_manager()
    
    # Check if session exists and is not expired (24 hour expiry)
    results = select_from_table("user_database", "sessions", ["username", "created_at"],
                              lambda row: row[1] == session_token)
    
    if not results:
        return False
        
    username, created_at = results[0]
    try:
        created_time = datetime.strptime(created_at, "%Y-%m-%d")
        if (datetime.now() - created_time).total_seconds() > 86400:  # 24 hours
            # Session expired, delete it
            delete_from_table("user_database", "sessions", lambda row: row[0] == username)
            return False
    except ValueError:
        # If date parsing fails, consider session invalid
        return False
        
    return True

def get_user_databases(username):
    """Get all databases accessible to the user."""
    _, _, _, select_from_table, _, _, _ = get_db_manager()
    databases = []
    print(f"Looking up databases for user: {username}")
    
    # Get databases owned by the user
    print("Checking databases owned by the user")
    owned = select_from_table("user_database", "database_owners", ["database_name"], lambda row: row[1] == username)
    print(f"Found {len(owned)} owned databases: {[db[0] for db in owned] if owned else 'None'}")
    
    for db in owned:
        databases.append({
            "name": db[0],
            "type": "owned"
        })
    
    # Get databases shared with the user
    print("Checking databases shared with the user")
    results = select_from_table("user_database", "database_shares", ["database_name"], lambda row: row[2] == username)
    print(f"Found {len(results)} shared databases: {[db[0] for db in results] if results else 'None'}")
    
    for db_name in results:
        # Avoid duplicates
        if not any(d["name"] == db_name[0] for d in databases):
            databases.append({
                "name": db_name[0],
                "type": "shared"
            })
    
    print(f"Total accessible databases: {len(databases)}")
    return databases

def share_database(owner, database_name, shared_with):
    """Share a database with another user."""
    _, _, insert_into_table, select_from_table, _, _, _ = get_db_manager()
    
    # Verify database ownership
    results = select_from_table("user_database", "database_owners", ["database_name", "owner"], lambda row: row[0] == database_name and row[1] == owner)
    if not results:
        print("You can only share databases you own!")
        return False
    
    # Check if database exists
    if not os.path.exists(os.path.join(BASE_DIR, database_name)):
        print("Database does not exist!")
        return False
    
    # Check if user exists
    results = select_from_table("user_database", "users", ["username"], lambda row: row[0] == shared_with)
    if not results:
        print("User does not exist!")
        return False
    
    # Check if share already exists
    results = select_from_table("user_database", "database_shares", ["database_name", "shared_with"], lambda row: row[0] == database_name and row[2] == shared_with)
    if results:
        print("Database already shared with this user!")
        return False
    
    # Add share record
    success = insert_into_table("user_database", "database_shares", [
        database_name,
        owner,
        shared_with,
        datetime.now().strftime("%Y-%m-%d")
    ])
    
    if success:
        print(f"Database '{database_name}' shared with '{shared_with}' successfully!")
        return True
    return False

def revoke_database_access(owner, database_name, shared_with):
    """Revoke database access from a user."""
    _, _, _, select_from_table, _, _, delete_from_table = get_db_manager()
    
    # Verify database ownership
    results = select_from_table("user_database", "database_owners", ["database_name", "owner"], lambda row: row[0] == database_name and row[1] == owner)
    if not results:
        print("You can only revoke access to databases you own!")
        return False
    
    # Delete share record
    success = delete_from_table("user_database", "database_shares", lambda row: row[0] == database_name and row[1] == owner and row[2] == shared_with)
    
    if success:
        print(f"Access to '{database_name}' revoked from '{shared_with}' successfully!")
        return True
    return False

def user_has_access_to_db(username, db_name):
    """Return True if the user owns or has been shared the database."""
    _, _, _, select_from_table, _, _, _ = get_db_manager()
    
    # Check if user owns the database
    owned = select_from_table("user_database", "database_owners", ["database_name"], lambda row: row[0] == db_name and row[1] == username)
    if owned:
        return True
    
    # Check if database is shared with user
    shared = select_from_table("user_database", "database_shares", ["database_name"], lambda row: row[0] == db_name and row[2] == username)
    return bool(shared)

def is_database_owner(username, db_name):
    """Return True if the user owns the database."""
    _, _, _, select_from_table, _, _, _ = get_db_manager()
    results = select_from_table("user_database", "database_owners", ["database_name"], lambda row: row[0] == db_name and row[1] == username)
    return bool(results)