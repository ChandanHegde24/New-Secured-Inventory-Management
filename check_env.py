# check_env.py
import os
from dotenv import load_dotenv

print("--- Starting environment check ---")

try:
    app_env_file = os.environ.get('APP_ENV_FILE', '.env')
    db_env_file = os.environ.get('DB_ENV_FILE', '.env.db')

    found_app_env = load_dotenv(app_env_file)
    found_db_env = load_dotenv(db_env_file, override=True)

    if found_app_env:
        print(f"SUCCESS: Found app env file: {app_env_file}")
    else:
        print(f"WARNING: App env file not found: {app_env_file}")

    if found_db_env:
        print(f"SUCCESS: Found DB env file: {db_env_file}")
    else:
        print(f"WARNING: DB env file not found: {db_env_file}")
        print("Create this file to keep database settings separate from .env")
    
    db_backend = (os.environ.get('DB_BACKEND') or 'mysql').strip().lower()
    print(f"DB_BACKEND = {db_backend}")

    if db_backend in ('mongodb', 'mongo'):
        mongo_uri = os.environ.get('MONGO_URI')
        mongo_db_name = os.environ.get('MONGO_DB_NAME') or os.environ.get('DB_NAME')

        if mongo_uri is None:
            masked_mongo_uri = None
        elif mongo_uri == "":
            masked_mongo_uri = "(empty)"
        else:
            masked_mongo_uri = mongo_uri[:20] + "..."

        print(f"MONGO_URI = {masked_mongo_uri}")
        print(f"MONGO_DB_NAME = {mongo_db_name}")

        if not mongo_uri:
            print("\nWARNING: MONGO_URI is missing from your .env file.")
        else:
            print("\nSUCCESS: MongoDB settings are present.")
    else:
        # MySQL mode (default)
        db_host = os.environ.get('DB_HOST')
        db_user = os.environ.get('DB_USER')
        db_pass = os.environ.get('DB_PASS')
        db_name = os.environ.get('DB_NAME')

        if db_pass is None:
            masked_db_pass = None
        elif db_pass == "":
            masked_db_pass = "(empty)"
        else:
            masked_db_pass = "*" * 8

        print(f"DB_HOST = {db_host}")
        print(f"DB_USER = {db_user}")
        print(f"DB_PASS = {masked_db_pass}")
        print(f"DB_NAME = {db_name}")

        if not all([db_host, db_user, db_name]):
            print("\nWARNING: One or more MySQL variables are missing from your .env file.")
        else:
            print("\nSUCCESS: All required MySQL variables are present.")

except ImportError:
    print("\nCRITICAL ERROR: The 'python-dotenv' library is not installed.")
    print("Please run this command: pip install python-dotenv")
except Exception as e:
    print(f"\nAn unexpected error happened: {e}")

print("--- Check complete ---")