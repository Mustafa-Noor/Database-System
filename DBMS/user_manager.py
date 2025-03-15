import csv
import os
from utils import wait_for_keypress
from file_manager import find_file_for_username

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
    else:
        return True

    with open(filename, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            if row[0] == username:
                if row[1] == password:
                    print(f"Welcome back, {username}!")
                    main()
                else:
                    print("Incorrect password!")
        
                return

    print("Username not found!")
