import os
import time
from parser import parse_command
from user_manager import register, sign_in, get_transaction_manager
from utils import clear_screen

# Global variable to track the active user and their transaction
active_user = None
current_txn_id = None  # Track the active transaction ID

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
            register(username, password)

        elif choice == "2":
            username = input("Enter username: ").strip()
            password = input("Enter password: ").strip()
            
            if sign_in(username, password):
                active_user = username  # Set active user
                transaction_manager = get_transaction_manager(username)  # Get user-specific transaction manager
                main(transaction_manager)  # Pass the transaction manager

        elif choice == "3":
            print("\n< Goodbye! Thanks for using La Vida Database. >")
            break

        else:
            print("\n< Invalid choice, please try again! >")

def main(transaction_manager):
    """Handles database and transaction operations."""
    global current_txn_id
    clear_screen()

    while True:
        prompt = "🔹 " if current_txn_id else "> "
        command = input(prompt).strip()

        if command.upper() == "EXIT":
            print("\nExiting the system. Goodbye!")
            break
        
        # Pass the transaction manager and current transaction ID
        current_txn_id = parse_command(command, transaction_manager, current_txn_id)

if __name__ == "__main__":
    start_menu()
