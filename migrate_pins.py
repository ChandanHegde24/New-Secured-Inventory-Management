import importlib
import bcrypt
from dotenv import load_dotenv
import os

# Load app env first, then DB-only overrides.
APP_ENV_FILE = os.environ.get('APP_ENV_FILE', '.env')
DB_ENV_FILE = os.environ.get('DB_ENV_FILE', '.env.db')
load_dotenv(APP_ENV_FILE)
load_dotenv(DB_ENV_FILE, override=True)

BCRYPT_PREFIXES = ('$2a$', '$2b$', '$2y$')


def is_bcrypt_hash(value):
    """Returns True when the provided value looks like a bcrypt hash."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return len(stripped) >= 60 and any(stripped.startswith(prefix) for prefix in BCRYPT_PREFIXES)

try:
    import mysql.connector
except Exception:
    mysql = None

if mysql is not None:
    MySQLError = mysql.connector.Error
else:
    class MySQLError(Exception):
        pass

try:
    pymongo_module = importlib.import_module('pymongo')
    pymongo_errors = importlib.import_module('pymongo.errors')
    MongoClient = getattr(pymongo_module, 'MongoClient')
    PyMongoError = getattr(pymongo_errors, 'PyMongoError')
except Exception:
    MongoClient = None

    class PyMongoError(Exception):
        pass


def migrate_mysql_pins():
    db_host = os.environ.get('DB_HOST')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_name = os.environ.get('DB_NAME')

    if not all([db_host, db_user, db_name]):
        print("Error: DB_HOST, DB_USER, and DB_NAME must be set in your .env file.")
        return

    if mysql is None:
        print("Error: mysql-connector-python is not installed.")
        return

    db = None
    cursor = None

    try:
        db = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name
        )
        cursor = db.cursor()
        print("Connected to MySQL database...")

        # Ensure the pin column can store bcrypt hashes.
        try:
            cursor.execute("ALTER TABLE users MODIFY pin VARCHAR(60) NOT NULL;")
            print("Altered 'users.pin' column to VARCHAR(60).")
        except MySQLError as err:
            print(f"Could not alter table (maybe already altered?): {err}")

        cursor.execute("SELECT username, pin FROM users;")
        users = cursor.fetchall()

        updated_count = 0
        for username, pin in users:
            if is_bcrypt_hash(pin):
                print(f"Skipping user '{username}': PIN already hashed.")
                continue

            print(f"Hashing PIN for user '{username}'...")
            hashed_pin = bcrypt.hashpw(str(pin).encode('utf-8'), bcrypt.gensalt())
            cursor.execute(
                "UPDATE users SET pin = %s WHERE username = %s",
                (hashed_pin.decode('utf-8'), username)
            )
            updated_count += 1

        if updated_count == 0 and len(users) > 0:
            print("\nAll user PINs were already hashed. No changes made.")
        else:
            db.commit()
            print(f"\nMigration complete! {updated_count} user PINs were securely hashed.")

    except MySQLError as err:
        print(f"\nDatabase error: {err}")
        if db:
            db.rollback()
    except Exception as err:
        print(f"\nAn unexpected error occurred: {err}")
        if db:
            db.rollback()
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()
            print("MySQL database connection closed.")


def migrate_mongo_pins():
    mongo_uri = os.environ.get('MONGO_URI')
    mongo_db_name = os.environ.get('MONGO_DB_NAME') or os.environ.get('DB_NAME') or 'inventory_db'

    if not mongo_uri:
        print("Error: MONGO_URI must be set in your .env file for MongoDB migration.")
        return

    if MongoClient is None:
        print("Error: pymongo is not installed.")
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
            print("\nAll user PINs were already hashed. No changes made.")
        else:
            print(f"\nMigration complete! {updated_count} user PINs were securely hashed.")

    except PyMongoError as err:
        print(f"\nMongoDB error: {err}")
    except Exception as err:
        print(f"\nAn unexpected error occurred: {err}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")


if __name__ == '__main__':
    db_backend = (os.environ.get('DB_BACKEND') or 'mysql').strip().lower()

    if db_backend in ('mongodb', 'mongo'):
        migrate_mongo_pins()
    else:
        migrate_mysql_pins()