# check_env.py
import os

from dotenv import load_dotenv

print('--- Starting environment check ---')

try:
    app_env_file = os.environ.get('APP_ENV_FILE', '.env')
    db_env_file = os.environ.get('DB_ENV_FILE', '.env.db')

    found_app_env = load_dotenv(app_env_file)
    found_db_env = load_dotenv(db_env_file, override=True)

    if found_app_env:
        print(f'SUCCESS: Found app env file: {app_env_file}')
    else:
        print(f'WARNING: App env file not found: {app_env_file}')

    if found_db_env:
        print(f'SUCCESS: Found DB env file: {db_env_file}')
    else:
        print(f'WARNING: DB env file not found: {db_env_file}')
        print('Create this file to keep database settings separate from .env')

    mongo_uri = os.environ.get('MONGO_URI')
    mongo_db_name = os.environ.get('MONGO_DB_NAME') or 'inventory_db'

    if mongo_uri is None:
        masked_mongo_uri = None
    elif mongo_uri == '':
        masked_mongo_uri = '(empty)'
    else:
        masked_mongo_uri = mongo_uri[:20] + '...'

    print('DATABASE = mongodb')
    print(f'MONGO_URI = {masked_mongo_uri}')
    print(f'MONGO_DB_NAME = {mongo_db_name}')

    if not mongo_uri:
        print('\nWARNING: MONGO_URI is missing from your environment files.')
    else:
        print('\nSUCCESS: MongoDB settings are present.')

except ImportError:
    print("\nCRITICAL ERROR: The 'python-dotenv' library is not installed.")
    print('Please run this command: pip install python-dotenv')
except Exception as err:
    print(f'\nAn unexpected error happened: {err}')

print('--- Check complete ---')