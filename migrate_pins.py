import importlib
import os

import bcrypt
from dotenv import load_dotenv

# Load app env first, then DB-only overrides.
APP_ENV_FILE = os.environ.get('APP_ENV_FILE', '.env')
DB_ENV_FILE = os.environ.get('DB_ENV_FILE', '.env.db')
DB_ENV_KEYS = ('MONGO_URI', 'MONGO_DB_NAME')

runtime_db_env = {key: os.environ[key] for key in DB_ENV_KEYS if key in os.environ}
load_dotenv(APP_ENV_FILE)
load_dotenv(DB_ENV_FILE, override=True)
for key, value in runtime_db_env.items():
    os.environ[key] = value

BCRYPT_PREFIXES = ('$2a$', '$2b$', '$2y$')


def is_bcrypt_hash(value):
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return len(stripped) >= 60 and any(stripped.startswith(prefix) for prefix in BCRYPT_PREFIXES)


try:
    pymongo_module = importlib.import_module('pymongo')
    pymongo_errors = importlib.import_module('pymongo.errors')
    MongoClient = getattr(pymongo_module, 'MongoClient')
    PyMongoError = getattr(pymongo_errors, 'PyMongoError')
except Exception:
    MongoClient = None

    class PyMongoError(Exception):
        pass


def migrate_mongo_pins():
    mongo_uri = os.environ.get('MONGO_URI')
    mongo_db_name = os.environ.get('MONGO_DB_NAME') or 'inventory_db'

    if not mongo_uri:
        print('Error: MONGO_URI must be set in your environment files.')
        return

    if MongoClient is None:
        print('Error: pymongo is not installed.')
        return

    client = None
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        db = client[mongo_db_name]
        print(f"Connected to MongoDB database '{mongo_db_name}'...")

        users = list(db.users.find({}, {'_id': 0, 'username': 1, 'pin': 1}))
        updated_count = 0

        for user_doc in users:
            username = user_doc.get('username')
            pin = user_doc.get('pin')

            if is_bcrypt_hash(pin):
                print(f"Skipping user '{username}': PIN already hashed.")
                continue

            print(f"Hashing PIN for user '{username}'...")
            hashed_pin = bcrypt.hashpw(str(pin).encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            db.users.update_one(
                {'username': username},
                {'$set': {'pin': hashed_pin}}
            )
            updated_count += 1

        if updated_count == 0 and len(users) > 0:
            print('\nAll user PINs were already hashed. No changes made.')
        else:
            print(f"\nMigration complete! {updated_count} user PINs were securely hashed.")

    except PyMongoError as err:
        print(f'\nMongoDB error: {err}')
    except Exception as err:
        print(f'\nAn unexpected error occurred: {err}')
    finally:
        if client:
            client.close()
            print('MongoDB connection closed.')


if __name__ == '__main__':
    migrate_mongo_pins()
