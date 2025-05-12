from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from user_manager import register, sign_in, get_user_databases, share_database, revoke_database_access, sign_out
from database_manager import create_database, drop_database, list_databases
from parser import parse_command

app = Flask(__name__)
# Configure CORS to allow requests from the frontend
CORS(app, resources={
    r"/api/*": {
        "origins": ["http://localhost:5173"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

@app.route('/api/auth/register', methods=['POST'])
def register_user():
    try:
        data = request.get_json()
        if register(data['username'], data['password'], data['email']):
            return jsonify({
                'message': 'Registration successful',
                'status': 'success'
            }), 201
        return jsonify({
            'error': 'Registration failed',
            'status': 'error'
        }), 400
    except Exception as e:
        print(f"Registration error: {str(e)}")
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    result = sign_in(data['username'], data['password'])
    if result:
        return jsonify({'user': result}), 200
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/databases', methods=['GET'])
def get_databases():
    username = request.headers.get('Authorization')
    if not username:
        return jsonify({'error': 'Unauthorized'}), 401
    databases = get_user_databases(username)
    return jsonify({'databases': databases}), 200

@app.route('/api/databases', methods=['POST'])
def create_new_database():
    username = request.headers.get('Authorization')
    if not username:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    print(f"Creating database '{data['name']}' for user '{username}'")
    
    try:
        # Create the database
        if create_database(data['name'], owner=username):
            # Record ownership
            from user_manager import record_database_owner
            if record_database_owner(data['name'], username):
                print(f"Database '{data['name']}' created and ownership recorded for user '{username}'")
                return jsonify({'message': 'Database created successfully'}), 201
            else:
                print(f"Warning: Database created but ownership recording failed for '{data['name']}'")
                return jsonify({'error': 'Database created but ownership recording failed'}), 500
        return jsonify({'error': 'Failed to create database'}), 400
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_name>/share', methods=['POST'])
def share_db(database_name):
    username = request.headers.get('Authorization')
    if not username:
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json()
    if share_database(username, database_name, data['username']):
        return jsonify({'message': 'Database shared successfully'}), 200
    return jsonify({'error': 'Failed to share database'}), 400

@app.route('/api/databases/<database_name>/revoke', methods=['POST'])
def revoke_db_access(database_name):
    username = request.headers.get('Authorization')
    if not username:
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json()
    if revoke_database_access(username, database_name, data['username']):
        return jsonify({'message': 'Access revoked successfully'}), 200
    return jsonify({'error': 'Failed to revoke access'}), 400

@app.route('/api/databases/<database_name>/fix_ownership', methods=['POST'])
def fix_database_ownership(database_name):
    username = request.headers.get('Authorization')
    if not username:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        from user_manager import record_database_owner
        if record_database_owner(database_name, username):
            return jsonify({'message': 'Database ownership fixed successfully'}), 200
        return jsonify({'error': 'Failed to fix database ownership'}), 400
    except Exception as e:
        print(f"Error fixing database ownership: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/execute_query', methods=['POST'])
def execute_query():
    try:
        # Get username from Authorization header
        username = request.headers.get('Authorization')
        if not username:
            return jsonify({'error': 'Unauthorized'}), 401

        # Get query and database from request
        data = request.get_json()
        if not data or 'query' not in data or 'database' not in data:
            return jsonify({'error': 'Missing query or database'}), 400

        query = data['query']
        database = data['database']
        print(f"Executing query: {query} on database: {database}")

        # Create user object for parser
        user = {'username': username}

        # Switch to selected database
        result = parse_command(f"USE DATABASE {database}", user)
        if isinstance(result, str) and result.startswith("Error"):
            return jsonify({'error': result}), 400

        # Execute the query
        result = parse_command(query, user, db_name=database)
        print(f"Query result: {result}")

        # Handle different types of results
        if isinstance(result, dict):
            return jsonify(result)
        elif isinstance(result, str):
            if result.startswith("Error"):
                return jsonify({'error': result}), 400
            return jsonify({'message': result})
        else:
            return jsonify({'error': 'Query execution failed'}), 400

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    try:
        username = request.headers.get('Authorization')
        if not username:
            return jsonify({'error': 'Unauthorized'}), 401
            
        if sign_out():
            return jsonify({'message': 'Logged out successfully'}), 200
        return jsonify({'error': 'Logout failed'}), 400
    except Exception as e:
        print(f"Logout error: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000) 