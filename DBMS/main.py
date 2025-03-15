import os
import time
from parser import parse_command
from user_manager import register, sign_in, get_transaction_manager
from utils import clear_screen
from transaction_manager import TransactionManager



def print_header():
    print("\n\n")
    print("=" * 50)
    print("  WELCOME TO LA VIDA DATABASE  ".center(50))
    print("=" * 50)
    

# Global variable to track the active user
active_user = None

def startMenu():
    """Main entry point to handle user registration and login."""
    global active_user
    while True:
        print_header()
        print("\n1️. Register\n2️. Sign In\n3️. Exit")
        choice = input("🔹 Enter choice> ")

        if choice == "1":
            username = input("Enter username: ")
            password = input("Enter password: ")
            register(username, password)

        elif choice == "2":
            username = input("Enter username: ")
            password = input("Enter password: ")
            if sign_in(username, password):
                active_user = username  # Set active user
                transaction_manager = get_transaction_manager(username)
                main(transaction_manager)

        elif choice == "3":
            print("\n< Goodbye! Thanks for using La Vida Database. >")
            break

        else:
            print("\n< Invalid choice, please try again! >")

def main(transaction_manager):
    """Handle normal database commands."""
    clear_screen()
    print(f"< Welcome, {active_user}! You can execute database commands. Type EXIT to log out. >")

    while True:
        command = input("> ").strip()
        if command.upper() == "EXIT":
            print(f"< Logging out {active_user}. Goodbye! >")
            break
        
        # If a transaction starts, switch to transaction mode
        if command.upper().startswith("START TRANSACTION FROM"):
            start_transaction_system(transaction_manager, command)
        else:
            parse_command(command, transaction_manager)

def start_transaction_system(transaction_manager, command):
    """Handles transaction-specific operations."""
    print("< Transaction started! Enter commands. Type COMMIT or ROLLBACK to exit transaction mode. >")

    # Start the transaction
    parse_command(command, transaction_manager)

    while True:
        command = input("🔹 ").strip()
        if command.upper() in ["COMMIT TRANSACTION", "ROLLBACK TRANSACTION"]:
            parse_command(command, transaction_manager)
            print("< Transaction ended. Returning to normal mode. >")
            break
        parse_command(command, transaction_manager)

if __name__ == "__main__":
    startMenu()