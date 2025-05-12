import os
import time
from parser import parse_command
from user_manager import register, sign_in, sign_out, share_database, revoke_database_access, get_user_databases
from utils import clear_screen
from database_manager import *

# Global variable to track the active user and their session
active_user = None
current_txn_id = None  # Track the active transaction ID

# def setup_environment():
#     """Sets up the initial environment for the user database."""
#     db_name = "user_database"
#     table_name = "users"
#     columns = ["username", "password"]

#     # Check if the database directory exists
#     db_path = os.path.join("databases", db_name)
#     if not os.path.exists(db_path):
#         os.makedirs(db_path)
#         print(f"Database '{db_name}' created.")

#     # Create the users table
#     create_table(db_name, table_name, columns)
#     print(f"Table '{table_name}' created in database '{db_name}'.")

def print_header():
    """Prints a welcome header."""
    print("\n\n" + "=" * 50)
    print("  WELCOME TO LA VIDA DATABASE  ".center(50))
    print("=" * 50)

def start_menu():
    """Handles user registration and login."""
    global active_user

    while True:
        print_header()
        print("\n1️. Register\n2️. Sign In\n3️. Exit")
        choice = input("🔹 Enter choice> ").strip()

        if choice == "1":
            username = input("Enter username: ").strip()
            password = input("Enter password: ").strip()
            email = input("Enter email: ").strip()
            register(username, password, email)

        elif choice == "2":
            username = input("Enter username: ").strip()
            password = input("Enter password: ").strip()
            
            user_data = sign_in(username, password)
            if user_data:
                active_user = user_data
                print(f"\nWelcome back, {username}!")
                print("\nYour databases:")
                for db in user_data["databases"]:
                    print(f"- {db['name']} ({db['type']})")
                main()  # Start the main loop for commands

        elif choice == "3":
            print("\n< Goodbye! Thanks for using La Vida Database. >")
            break

        else:
            print("\n< Invalid choice, please try again! >")

def main():
    """Handles database operations after user signs in."""
    global active_user, current_txn_id

    while True:
        command = input("> ").strip()

        if command.upper() == "EXIT":
            if active_user:
                sign_out()
                active_user = None
                current_txn_id = None
            print("\nExiting the system. Goodbye!")
            break

        elif command.upper() == "SHARE":
            if not active_user:
                print("Please sign in first!")
                continue
                
            database_name = input("Enter database name to share: ").strip()
            shared_with = input("Enter username to share with: ").strip()
            share_database(active_user["username"], database_name, shared_with)
            # Refresh user's database list
            active_user["databases"] = get_user_databases(active_user["username"])

        elif command.upper() == "REVOKE":
            if not active_user:
                print("Please sign in first!")
                continue
                
            database_name = input("Enter database name: ").strip()
            shared_with = input("Enter username to revoke access from: ").strip()
            
            revoke_database_access(active_user["username"], database_name, shared_with)
            # Refresh user's database list
            active_user["databases"] = get_user_databases(active_user["username"])

        elif command.upper() == "SHOW DATABASES":
            if not active_user:
                print("Please sign in first!")
                continue
                
            # Refresh user's database list
            active_user["databases"] = get_user_databases(active_user["username"])
            print("\nYour databases:")
            for db in active_user["databases"]:
                print(f"- {db['name']} ({db['type']})")

        else:
            result = parse_command(command, active_user)
            # If the command was a database creation or deletion, refresh the list
            if isinstance(result, bool) and result:
                if command.upper().startswith("CREATE DATABASE") or command.upper().startswith("DROP DATABASE"):
                    active_user["databases"] = get_user_databases(active_user["username"])

if __name__ == "__main__":
    start_menu()
