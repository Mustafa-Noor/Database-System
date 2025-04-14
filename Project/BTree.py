from BTrees.OOBTree import OOBTree
import pickle
import os

class BTreeIndex:
    def __init__(self, index_file):
        """Initialize the B-Tree index."""
        self.index_file = index_file
        self.tree = None

        # Load or create the B-Tree index
        if os.path.exists(index_file):
            with open(index_file, "rb") as f:
                self.tree = pickle.load(f)  # Load the B-Tree from the file
        else:
            self.tree = OOBTree()  # Create a new B-Tree

    def insert(self, key, row_id):
        """Insert a key-row_id pair into the B-Tree."""
        self.tree[key] = row_id

    def delete(self, key):
        """Delete a key from the B-Tree."""
        if key in self.tree:
            del self.tree[key]

    def search(self, key):
        """Search for a key in the B-Tree and return the row_id."""
        return self.tree.get(key, -1)

    def close(self):
        """Save and close the B-Tree index."""
        with open(self.index_file, "wb") as f:
            pickle.dump(self.tree, f)  # Save the B-Tree to the file