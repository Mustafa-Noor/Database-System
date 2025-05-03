from BTrees.OOBTree import OOBTree
import pickle
import os

class BTreeIndex:
    def __init__(self, index_file):
        """Initialize the B-Tree index."""
        self.index_file = index_file
        self.tree = None
        self.load_index()

    def load_index(self):
        """Load or create the B-Tree index."""
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, "rb") as f:
                    self.tree = pickle.load(f)
            else:
                self.tree = OOBTree()
        except (pickle.UnpicklingError, EOFError, Exception) as e:
            print(f"Error loading index {self.index_file}: {str(e)}")
            self.tree = OOBTree()

    def insert(self, key, row_id):
        """Insert a key-row_id pair into the B-Tree."""
        try:
            self.tree[key] = row_id
        except Exception as e:
            print(f"Error inserting key {key}: {str(e)}")

    def delete(self, key):
        """Delete a key from the B-Tree."""
        try:
            if key in self.tree:
                del self.tree[key]
        except Exception as e:
            print(f"Error deleting key {key}: {str(e)}")

    def search(self, key):
        """Search for a key in the B-Tree and return the row_id."""
        try:
            return self.tree.get(key, None)
        except Exception as e:
            print(f"Error searching for key {key}: {str(e)}")
            return None

    def range_search(self, start_key=None, end_key=None):
        """Search for keys within a range and return their row_ids."""
        try:
            if start_key is None and end_key is None:
                return list(self.tree.values())
            
            if start_key is None:
                return [v for k, v in self.tree.items(min=end_key, max=end_key)]
            if end_key is None:
                return [v for k, v in self.tree.items(min=start_key, max=start_key)]
            
            return [v for k, v in self.tree.items(min=start_key, max=end_key)]
        except Exception as e:
            print(f"Error in range search: {str(e)}")
            return []

    def close(self):
        """Save and close the B-Tree index."""
        try:
            with open(self.index_file, "wb") as f:
                pickle.dump(self.tree, f)
        except Exception as e:
            print(f"Error saving index {self.index_file}: {str(e)}")