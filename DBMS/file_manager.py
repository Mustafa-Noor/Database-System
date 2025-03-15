import csv
import os

# Define paths
UserTableFolder = "data/UserTable"
UserIndexFile = os.path.join(UserTableFolder, "index.csv")

# Ensure the data/UserTable folder exists
os.makedirs(UserTableFolder, exist_ok=True)

def initialize_files():
    """Ensure index.csv and user files exist."""
    if not os.path.exists(UserIndexFile):
        with open(UserIndexFile, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Range", "File"])
            writer.writerows([
                ["A-D", "a_d.csv"],
                ["E-H", "e_h.csv"],
                ["I-L", "i_l.csv"],
                ["M-P", "m_p.csv"],
                ["Q-T", "q_t.csv"],
                ["U-Z", "u_z.csv"],
            ])

    # Create user files if they don't exist
    with open(UserIndexFile, "r") as index_file:
        reader = csv.reader(index_file)
        next(reader)  # Skip header
        for row in reader:
            filepath = os.path.join(UserTableFolder, row[1])
            if not os.path.exists(filepath):
                with open(filepath, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Username", "Password"])  # Header

def find_file_for_username(username):
    """Find the correct file for a given username based on index.csv."""
    with open(UserIndexFile, "r") as index_file:
        reader = csv.reader(index_file)
        next(reader)  # Skip header
        for row in reader:
            range_letters, filename = row
            start, end = range_letters.split("-")
            if start <= username[0].upper() <= end:
                return os.path.join(UserTableFolder, filename)
    return None

# Run initialization when imported
initialize_files()
