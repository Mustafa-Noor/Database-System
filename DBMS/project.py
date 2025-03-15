import os

DATA_FILE = "records.txt"
INDEX_FILE = "index.txt"

# Load index from file
def load_index():
    index = {}
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, "r") as f:
            for line in f:
                username, line_num = line.strip().split(",")
                index[username] = int(line_num)
    return index

# Save index to file
def save_index(index):
    with open(INDEX_FILE, "w") as f:
        for username, line_num in index.items():
            f.write(f"{username},{line_num}\n")

# Register a new user
def register(username, password):
    index = load_index()

    if username in index:
        print("Username already exists! Choose a different one.")
        return

    with open(DATA_FILE, "a") as f:
        line_num = sum(1 for _ in open(DATA_FILE)) + 1
        f.write(f"{username},{password}\n")

    index[username] = line_num
    save_index(index)
    print("Registration successful!")

# Sign in an existing user
def sign_in(username, password):
    index = load_index()

    if username not in index:
        print("User not found!")
        return False

    line_num = index[username]
    with open(DATA_FILE, "r") as f:
        for i, line in enumerate(f, start=1):
            if i == line_num:
                stored_username, stored_password = line.strip().split(",")
                if stored_password == password:
                    print("Login successful!")
                    return True
                else:
                    print("Incorrect password!")
                    return False

# Main function to run the system
def main():
    while True:
        print("\n1. Register\n2. Sign In\n3. Exit")
        choice = input("Enter choice: ")

        if choice == "1":
            username = input("Enter username: ")
            password = input("Enter password: ")
            register(username, password)

        elif choice == "2":
            username = input("Enter username: ")
            password = input("Enter password: ")
            sign_in(username, password)

        elif choice == "3":
            print("Goodbye!")
            break

        else:
            print("Invalid choice, try again.")

if __name__ == "__main__":
    main()
